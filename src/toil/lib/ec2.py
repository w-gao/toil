import logging
import re
import time

from typing import Optional, List, Dict, Union, Iterable
from operator import attrgetter
from past.builtins import map
from toil.lib.exceptions import panic
from toil.lib.retry import old_retry
from boto.ec2.instance import Instance as Boto2Instance
from boto.ec2.spotinstancerequest import SpotInstanceRequest
from boto.exception import EC2ResponseError
from boto3 import Session
from boto3.resources.base import ServiceResource
from botocore.client import BaseClient
from botocore.credentials import JSONFileCache
from botocore.exceptions import ClientError
from botocore.session import get_session



a_short_time = 5
a_long_time = 60 * 60
log = logging.getLogger(__name__)


class UserError(RuntimeError):
    def __init__(self, message=None, cause=None):
        if (message is None) == (cause is None):
            raise RuntimeError("Must pass either message or cause.")
        super(
            UserError, self).__init__(
            message if cause is None else cause.message)


def not_found(e):
    return e.error_code.endswith('.NotFound')


def retry_ec2(t=a_short_time, retry_for=10 * a_short_time, retry_while=not_found):
    return old_retry(delays=(t, t, t * 2, t * 4),
                     timeout=retry_for,
                     predicate=retry_while)


class UnexpectedResourceState(Exception):
    def __init__(self, resource, to_state, state):
        super(UnexpectedResourceState, self).__init__(
            "Expected state of %s to be '%s' but got '%s'" %
            (resource, to_state, state))

def establish_boto3_session(region_name: Optional[str] = None) -> Session:
    """
    This is the One True Place where Boto3 sessions should be established, and
    prepares them with the nexessary credential caching.

    :param region_name: If given, the session will be associated with the given AWS region.
    """

    # Make sure to use credential caching when talking to Amazon via boto3
    # See https://github.com/boto/botocore/pull/1338/
    # And https://github.com/boto/botocore/commit/2dae76f52ae63db3304b5933730ea5efaaaf2bfc

    botocore_session = get_session()
    botocore_session.get_component('credential_provider').get_provider('assume-role').cache = JSONFileCache()

    return Session(botocore_session=botocore_session, region_name=region_name)

# This regex matches AWS availability zones.
availability_zone_re = re.compile(r'^([a-z]{2}-[a-z]+-[1-9][0-9]*)([a-z])$')

def zone_to_region(zone: str):
    """Get a region (e.g. us-west-2) from a zone (e.g. us-west-1c)."""
    m = availability_zone_re.match(zone)
    if not m:
        raise ValueError("Can't extract region from availability zone '%s'"
                         % availability_zone)
    return m.group(1)


def wait_transition(resource, from_states, to_state,
                    state_getter=attrgetter('state')):
    """
    Wait until the specified EC2 resource (instance, image, volume, ...) transitions from any
    of the given 'from' states to the specified 'to' state. If the instance is found in a state
    other that the to state or any of the from states, an exception will be thrown.

    :param resource: the resource to monitor
    :param from_states:
        a set of states that the resource is expected to be in before the  transition occurs
    :param to_state: the state of the resource when this method returns
    """
    state = state_getter(resource)
    while state in from_states:
        time.sleep(a_short_time)
        for attempt in retry_ec2():
            with attempt:
                resource.update(validate=True)
        state = state_getter(resource)
    if state != to_state:
        raise UnexpectedResourceState(resource, to_state, state)


def wait_instances_running(ec2, instances: Iterable[Boto2Instance]) -> Iterable[Boto2Instance]:
    """
    Wait until no instance in the given iterable is 'pending'. Yield every instance that
    entered the running state as soon as it does.

    :param boto.ec2.connection.EC2Connection ec2: the EC2 connection to use for making requests
    :param Iterable[Boto2Instance] instances: the instances to wait on
    :rtype: Iterable[Boto2Instance]
    """
    running_ids = set()
    other_ids = set()
    while True:
        pending_ids = set()
        for i in instances:
            if i.state == 'pending':
                pending_ids.add(i.id)
            elif i.state == 'running':
                assert i.id not in running_ids
                running_ids.add(i.id)
                yield i
            else:
                assert i.id not in other_ids
                other_ids.add(i.id)
                yield i
        log.info('%i instance(s) pending, %i running, %i other.',
                 *map(len, (pending_ids, running_ids, other_ids)))
        if not pending_ids:
            break
        seconds = max(a_short_time, min(len(pending_ids), 10 * a_short_time))
        log.info('Sleeping for %is', seconds)
        time.sleep(seconds)
        for attempt in retry_ec2():
            with attempt:
                instances = ec2.get_only_instances(list(pending_ids))


def wait_spot_requests_active(ec2, requests: Iterable[SpotInstanceRequest], timeout: float = None, tentative: bool = False) -> Iterable[List[SpotInstanceRequest]]:
    """
    Wait until no spot request in the given iterator is in the 'open' state or, optionally,
    a timeout occurs. Yield spot requests as soon as they leave the 'open' state.

    :param requests: The requests to wait on.

    :param timeout: Maximum time in seconds to spend waiting or None to wait forever. If a
    timeout occurs, the remaining open requests will be cancelled.

    :param tentative: if True, give up on a spot request at the earliest indication of it
    not being fulfilled immediately

    """

    if timeout is not None:
        timeout = time.time() + timeout
    active_ids = set()
    other_ids = set()
    open_ids = None

    def cancel():
        log.warning('Cancelling remaining %i spot requests.', len(open_ids))
        ec2.cancel_spot_instance_requests(list(open_ids))

    def spot_request_not_found(e):
        error_code = 'InvalidSpotInstanceRequestID.NotFound'
        return isinstance(e, EC2ResponseError) and e.error_code == error_code

    try:
        while True:
            open_ids, eval_ids, fulfill_ids = set(), set(), set()
            batch = []
            for r in requests:
                if r.state == 'open':
                    open_ids.add(r.id)
                    if r.status.code == 'pending-evaluation':
                        eval_ids.add(r.id)
                    elif r.status.code == 'pending-fulfillment':
                        fulfill_ids.add(r.id)
                    else:
                        log.info(
                            'Request %s entered status %s indicating that it will not be '
                            'fulfilled anytime soon.', r.id, r.status.code)
                elif r.state == 'active':
                    assert r.id not in active_ids
                    active_ids.add(r.id)
                    batch.append(r)
                else:
                    assert r.id not in other_ids
                    other_ids.add(r.id)
                    batch.append(r)
            if batch:
                yield batch
            log.info('%i spot requests(s) are open (%i of which are pending evaluation and %i '
                     'are pending fulfillment), %i are active and %i are in another state.',
                     *map(len, (open_ids, eval_ids, fulfill_ids, active_ids, other_ids)))
            if not open_ids or tentative and not eval_ids and not fulfill_ids:
                break
            sleep_time = 2 * a_short_time
            if timeout is not None and time.time() + sleep_time >= timeout:
                log.warning('Timed out waiting for spot requests.')
                break
            log.info('Sleeping for %is', sleep_time)
            time.sleep(sleep_time)
            for attempt in retry_ec2(retry_while=spot_request_not_found):
                with attempt:
                    requests = ec2.get_all_spot_instance_requests(
                        list(open_ids))
    except BaseException:
        if open_ids:
            with panic(log):
                cancel()
        raise
    else:
        if open_ids:
            cancel()


def create_spot_instances(ec2, price, image_id, spec, num_instances=1, timeout=None, tentative=False, tags=None) -> Iterable[List[Boto2Instance]]:
    """
    Create instances on the spot market.
    """
    def spotRequestNotFound(e):
        return getattr(e, 'error_code', None) == "InvalidSpotInstanceRequestID.NotFound"

    for attempt in retry_ec2(retry_for=a_long_time,
                             retry_while=inconsistencies_detected):
        with attempt:
            requests = ec2.request_spot_instances(
                price, image_id, count=num_instances, **spec)

    if tags is not None:
        for requestID in (request.id for request in requests):
            for attempt in retry_ec2(retry_while=spotRequestNotFound):
                with attempt:
                    ec2.create_tags([requestID], tags)

    num_active, num_other = 0, 0
    # noinspection PyUnboundLocalVariable,PyTypeChecker
    # request_spot_instances's type annotation is wrong
    for batch in wait_spot_requests_active(ec2,
                                           requests,
                                           timeout=timeout,
                                           tentative=tentative):
        instance_ids = []
        for request in batch:
            if request.state == 'active':
                instance_ids.append(request.instance_id)
                num_active += 1
            else:
                log.info(
                    'Request %s in unexpected state %s.',
                    request.id,
                    request.state)
                num_other += 1
        if instance_ids:
            # This next line is the reason we batch. It's so we can get multiple instances in
            # a single request.
            yield ec2.get_only_instances(instance_ids)
    if not num_active:
        message = 'None of the spot requests entered the active state'
        if tentative:
            log.warning(message + '.')
        else:
            raise RuntimeError(message)
    if num_other:
        log.warning('%i request(s) entered a state other than active.', num_other)


def inconsistencies_detected(e):
    if isinstance(e, ClientError):
        # Boto3 error
        if e.response['Error']['Code'] == 'InvalidGroup.NotFound':
            return True
        # This is where boto3 keeps messages
        m = err.response['Error']['Message']
    else:
        # Maybe a boto2 error?
        if getattr(e, 'code', None) == 'InvalidGroup.NotFound':
            return True
        # This is where boto2 keeps messages
        m = getattr(e, 'error_message', '').lower()
    return 'invalid iam instance profile' in m or 'no associated iam roles' in m


def create_ondemand_instances(ec2, image_id, spec, num_instances=1) -> List[Boto2Instance]:
    """
    Requests the RunInstances EC2 API call but accounts for the race between recently created
    instance profiles, IAM roles and an instance creation that refers to them.

    :rtype: List[Boto2Instance]
    """
    instance_type = spec['instance_type']
    log.info('Creating %s instance(s) ... ', instance_type)
    for attempt in retry_ec2(retry_for=a_long_time,
                             retry_while=inconsistencies_detected):
        with attempt:
            return ec2.run_instances(image_id,
                                     min_count=num_instances,
                                     max_count=num_instances,
                                     **spec).instances


def prune(bushy: dict) -> dict:
    """
    Prune entries in the given dict with false-y values.
    Boto3 may not like None and instead wants no key.
    """
    pruned = dict()
    for key in bushy:
        if bushy[key]:
            pruned[key] = bushy[key]
    return pruned
    
    

# TODO: Implement retry_decorator here
# [5, 5, 10, 20, 20, 20, 20] I don't think we need to retry for an hour... ???
# InvalidGroup.NotFound
# OR
# 'invalid iam instance profile' in m.lower() or 'no associated iam roles' in m.lower()
def create_instances(ec2_resource: ServiceResource,
                     image_id: str,
                     key_name: str,
                     instance_type: str,
                     num_instances: int = 1,
                     security_group_ids: Optional[List] = None,
                     user_data: Optional[Union[str, bytes]] = None,
                     block_device_map: Optional[List[Dict]] = None,
                     instance_profile_arn: Optional[str] = None,
                     placement_az: Optional[str] = None,
                     subnet_id: str = None,
                     tags: Optional[Dict[str, str]] = None) -> List[dict]:
    """
    Replaces create_ondemand_instances.  Uses boto3 and returns a list of Boto3 instance dicts.

    See "create_instances" (returns a list of ec2.Instance objects):
      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.ServiceResource.create_instances
    Not to be confused with "run_instances" (same input args; returns a dictionary):
      https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.run_instances
      
    Tags, if given, are applied to the instances, and all volumes.
    """
    log.info('Creating %s instance(s) ... ', instance_type)
    
    if isinstance(user_data, str):
        user_data = user_data.encode('utf-8')
    
    for attempt in retry_ec2(retry_for=a_long_time, retry_while=inconsistencies_detected):
        with attempt:
            
            request = {'ImageId': image_id,
                       'MinCount': num_instances,
                       'MaxCount': num_instances,
                       'KeyName': key_name,
                       'SecurityGroupIds': security_group_ids,
                       'InstanceType': instance_type,
                       'UserData': user_data,
                       'BlockDeviceMappings': block_device_map,
                       'SubnetId': subnet_id}
                       
            if instance_profile_arn:
                request['IamInstanceProfile'] = {'Arn': instance_profile_arn}
                
            if placement_az:
                request['Placement'] = {'AvailabilityZone': placement_az}
                       
            if tags:
                # Tag everything when we make it.
                # TODO: just generate tags data once?
                request['TagSpecifications'] = [{'ResourceType': 'instance', 'Tags': [{'Key': k, 'Value': v} for k, v in tags.items()]},
                                                {'ResourceType': 'volume', 'Tags': [{'Key': k, 'Value': v} for k, v in tags.items()]}]

            return ec2_resource.create_instances(**prune(request))
            
def create_launch_template(ec2_client: BaseClient,
                           template_name: str,
                           image_id: str,
                           key_name: str,
                           instance_type: str,
                           security_group_ids: Optional[List] = None,
                           user_data: Optional[Union[str, bytes]] = None,
                           block_device_map: Optional[List[Dict]] = None,
                           instance_profile_arn: Optional[str] = None,
                           placement_az: Optional[str] = None,
                           subnet_id: Optional[str] = None,
                           tags: Optional[Dict[str, str]] = None) -> str:
    """
    Creates a launch template with the given name for launching instances with the given parameters.
    
    We only ever use launch template version 1 of any launch template.

    Internally calls https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html?highlight=create_launch_template#EC2.Client.create_launch_template
    
    Returns the ID of the launch template.
    
    Tags, if given, are applied to the template itself, all instances, and all volumes.
    """
    log.info('Creating launch template for %s instances ... ', instance_type)
    
    if isinstance(user_data, bytes):
        # Boto3 insists on a str here.
        user_data = user_data.decode('utf-8')
    
    for attempt in retry_ec2(retry_for=a_long_time, retry_while=inconsistencies_detected):
        with attempt:
            template = {'ImageId': image_id,
                        'KeyName': key_name,
                        'SecurityGroupIds': security_group_ids,
                        'InstanceType': instance_type,
                        'UserData': user_data,
                        'BlockDeviceMappings': block_device_map,
                        'SubnetId': subnet_id}
                       
            if instance_profile_arn:
                template['IamInstanceProfile'] = {'Arn': instance_profile_arn}
                
            if placement_az:
                template['Placement'] = {'AvailabilityZone': placement_az}
                       
            if tags:
                # Tag everything when we make it.
                # TODO: just generate tags data once?
                template['TagSpecifications'] = [{'ResourceType': 'instance', 'Tags': [{'Key': k, 'Value': v} for k, v in tags.items()]},
                                                 {'ResourceType': 'volume', 'Tags': [{'Key': k, 'Value': v} for k, v in tags.items()]}]

            request = {'LaunchTemplateData': prune(template),
                       'LaunchTemplateName': template_name}
                       
            if tags:
                request['TagSpecifications'] = [{'ResourceType': 'launch-template', 'Tags': [{'Key': k, 'Value': v} for k, v in tags.items()]}]
                       
            return ec2_client.create_launch_template(**request)
            
def create_auto_scaling_group(autoscaling_client: BaseClient,
                              asg_name: str,
                              launch_template_id: str,
                              vpc_subnets: List[str],
                              min_size: int = 0,
                              max_size: int = 0,
                              instance_types: Optional[List[str]] = None,
                              spot_bid: Optional[float] = None,
                              spot_cheapest: bool = False,
                              tags: Optional[Dict[str, str]] = None) -> None:

    """
    Create a new Auto Scaling Group with the given name (which is also its
    unique identifier).
    
    :param vpc_subnets: One or more subnet IDs to place instances in the group
           into. Determine the availability zone(s) instances will launch into.
    :param instance_types: Use a pool over the given instance types, instead of
           the type given in the launch template. For on-demand groups, this is
           a prioritized list. For spot groups, we let AWS balance according to
           spot_strategy. Must be 20 types or shorter.
    :param spot_bid: If set, the ASG will be a spot market ASG. Bid is in
           dollars per instance hour. All instance types in the group are bid on
           equivalently.
    :param spot_cheapest: If true, use the cheapest spot instances available out
           of instance_types, instead of the spot instances that minimize
           eviction probability.
    :param tags: Tags to apply to the ASG only. Tags for the instances should
           be added to the launch template instead.
    
    
    The default version of the launch template is used.
    """
    
    if instance_types is None:
        instance_types = []
    
    if instance_types is not None and len(instance_types) > 20:
        raise RuntimeError(f"Too many instance types ({len(instance_types)}) in group; AWS supports only 20.")
        
    if len(vpc_subnets) == 0:
        raise RuntimeError("No VPC subnets specified to launch into; not clear where to put instances")
    
    for attempt in retry_ec2(retry_for=a_long_time, retry_while=inconsistencies_detected):
        with attempt:
            # We always write the ASG with a MixedInstancesPolicy even when we have only one type.
            mip = {'LaunchTemplate': {'LaunchTemplateSpecification': {'LaunchTemplateId': launch_template_id, 'Version': '$Default'},
                                      'Overrides': [{'InstanceType': t} for t in instance_types]}}
            
            if spot_bid is not None:
                # Ask for spot instances by saying everything above base capacity of 0 should be spot.
                mip['InstancesDistribution'] = {'OnDemandPercentageAboveBaseCapacity': 0,
                                                'SpotAllocationStrategy': 'capacity-optimized' if not spot_cheapest else 'lowest-price',
                                                'SpotMaxPrice': str(spot_bid)}
            
            asg = {'AutoScalingGroupName': asg_name,
                   'MixedInstancesPolicy': prune(mip),
                   'MinSize': min_size,
                   'MaxSize': max_size,
                   'VPCZoneIdentifier': ','.join(vpc_subnets)}
                       
            if tags:
                # Tag the ASG itself.
                asg['Tags'] = [{'Key': k, 'Value': v, } for k, v in tags.items()]

            # Don't prune the ASG because MinSize and MaxSize are required and may be 0.
            autoscaling_client.create_auto_scaling_group(**asg)
