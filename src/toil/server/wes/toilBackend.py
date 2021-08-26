import json
import logging
import os
import shutil
import signal
import subprocess
import uuid
from multiprocessing import Process
from typing import Optional, List, Union, Dict, Any

from toil.server.wes.abstractBackend import WESBackend, DefaultOptions
from toil.server.wes.utils import handle_errors, WorkflowNotFoundError, get_iso_time
from toil.version import baseVersion


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ToilWorkflow:
    def __init__(self, run_id: str):
        """
        Represents a toil workflow.

        :param run_id: A uuid string.  Used to name the folder that contains
                       all of the files containing this particular workflow
                       instance's information.
        """
        self.run_id = run_id

        self.work_dir = os.path.join(os.getcwd(), "workflows", self.run_id)
        self.out_dir = os.path.join(self.work_dir, "outdir")
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)

        # execution
        self.stdout_file = os.path.join(self.work_dir, "stdout")
        self.stderr_file = os.path.join(self.work_dir, "stderr")

        self.start_time = os.path.join(self.work_dir, "starttime")
        self.end_time = os.path.join(self.work_dir, "endtime")

        self.pid_file = os.path.join(self.work_dir, "pid")
        self.exit_code_file = os.path.join(self.work_dir, "exit_code")
        self.stat_complete_file = os.path.join(self.work_dir, "status_completed")
        self.stat_error_file = os.path.join(self.work_dir, "status_error")

        # workflow information
        self.cmd_file = os.path.join(self.work_dir, "cmd")
        self.job_store_file = os.path.join(self.work_dir, "jobstore")

        self.request_json = os.path.join(self.work_dir, "request.json")
        self.input_json = os.path.join(self.work_dir, "wes_input.json")

        self.job_store_default = "file:" + os.path.join(self.work_dir, "toiljobstore")
        self.job_store: Optional[str] = None

    def sort_toil_options(self, wf_type: str, options: List[str]) -> List[str]:
        """
        """
        # TODO: this is dependent on the workflow type
        #  e.g.: Toil workflows don't take the --jobStore argument. They also don't have --outDir.

        # determine job store and set a new default if the user did not set one
        cloud = False
        for e in options:
            if e.startswith("--jobStore="):
                self.job_store = e[11:]
                if self.job_store.startswith(("aws", "google", "azure")):
                    cloud = True
                if wf_type == "py":
                    options.remove(e)
            if e.startswith(("--outdir=", "-o=")):
                options.remove(e)

        if not cloud:
            if wf_type in ('cwl', 'wdl'):
                options.append("--outdir=" + self.out_dir)
            else:
                # TODO: find a way to communicate the outDir to the Toil workflow
                pass
        if not self.job_store:
            if wf_type in ('cwl', 'wdl'):
                options.append("--jobStore=" + self.job_store_default)
            else:
                # append the positional jobStore argument at the end for Python workflows
                options.append(self.job_store_default)

            self.job_store = self.job_store_default

        # store the job store location
        with open(self.job_store_file, "w") as f:
            f.write(self.job_store)

        return options

    def write_workflow(self,
                       request: Dict[str, Any],
                       opts: DefaultOptions,
                       temp_dir: str,
                       wf_type: str = "cwl") -> List[str]:
        """
        Writes a cwl, wdl, or python file as appropriate from the request
        dictionary.
        """

        workflow_url = request["workflow_url"]

        # link the cwl and json into the cwd
        # TODO: users shouldn't be able to access the server's filesystem, right?
        if workflow_url.startswith("file://"):
            try:
                os.link(workflow_url[7:], os.path.join(temp_dir, "wes_workflow." + wf_type))
            except OSError:
                os.symlink(workflow_url[7:], os.path.join(temp_dir, "wes_workflow." + wf_type))
            workflow_url = os.path.join(temp_dir, "wes_workflow." + wf_type)

        try:
            os.link(self.input_json, os.path.join(temp_dir, "wes_input.json"))
        except OSError:
            os.symlink(self.input_json, os.path.join(temp_dir, "wes_input.json"))
        self.input_json = os.path.join(temp_dir, "wes_input.json")

        extra_options = self.sort_toil_options(wf_type, opts.get_options("extra"))

        if wf_type == "cwl":
            command_args = (
                ["toil-cwl-runner"] + extra_options + [workflow_url, self.input_json]
            )
        elif wf_type == "wdl":
            command_args = (
                ["toil-wdl-runner"] + extra_options + [workflow_url, self.input_json]
            )
        elif wf_type == "py":
            command_args = ["python"] + [workflow_url] + extra_options
        else:
            raise RuntimeError(
                'workflow_type is not "cwl", "wdl", or "py": ' + str(wf_type)
            )

        return command_args

    def call_cmd(self, cmd: Union[List[str], str], cwd: str) -> int:
        """
        Calls a command with Popen. Writes stdout, stderr, and the command to
        separate files.

        This is a blocking call.

        :param cmd: A string or array of strings.
        :param cwd: A path to the working directory.

        :return: The exit code of the command.
        """
        with open(self.cmd_file, "w") as f:
            f.write(" ".join(cmd))

        with open(self.stdout_file, "w") as stdout, open(self.stderr_file, "w") as stderr:
            logger.info("Calling: " + " ".join(cmd))
            process = subprocess.Popen(cmd,
                                       stdout=stdout,
                                       stderr=stderr,
                                       close_fds=True,
                                       cwd=cwd)

        # write pid to file
        with open(self.pid_file, "w") as f:
            f.write(str(process.pid))

        # block until an exit code is received
        return process.wait()

    def cancel(self) -> bool:
        """
        Cancel a workflow run by sending a SIGTERM to the process.
        """
        pid = self.fetch(self.pid_file)

        if not pid:
            # process was not created
            return False

        try:
            # signal an interrupt to kill the process gently
            os.kill(int(pid), signal.SIGINT)
        except ProcessLookupError:
            return False

        return True

    def fetch(self, filename: str, default_value: str = "") -> str:
        """
        Returns the contents of the given file. If the file does not exist, the
        default value is returned.
        """
        if os.path.exists(filename):
            with open(filename, "r") as f:
                return f.read()
        return default_value

    def get_log(self) -> Dict[str, Any]:
        """
        Get detailed information about a current workflow run.
        """
        state = self.get_state()

        with open(self.request_json, "r") as f:
            request = json.load(f)

        with open(self.job_store_file, "r") as f:
            self.job_store = f.read()

        stderr = self.fetch(self.stderr_file)
        exit_code = self.fetch(self.exit_code_file)
        start_time = self.fetch(self.start_time)
        end_time = self.fetch(self.end_time)
        cmd = [self.fetch(self.cmd_file)]

        output_obj = {}
        if state == "COMPLETE":
            # only tested locally
            if self.job_store.startswith("file:"):
                for file in os.listdir(self.out_dir):
                    if file.startswith("out_tmpdir"):
                        shutil.rmtree(os.path.join(self.out_dir, file))
                for file in os.listdir(self.out_dir):
                    output_obj[file] = {
                        "location": os.path.join(self.out_dir, file),
                        "size": os.stat(os.path.join(self.out_dir, file)).st_size,
                        "class": "File",
                    }

        return {
            "run_id": self.run_id,
            "request": request,
            "state": state,
            "run_log": {
                "cmd": cmd,
                "start_time": start_time,
                "end_time": end_time,
                # TODO: stdout and stderr should be a URL that points to the output file, not the actual contents.
                "stdout": "",
                "stderr": stderr,
                "exit_code": int(exit_code) if exit_code != "" else "",
            },
            "task_logs": [],
            "outputs": output_obj,
        }

    def run(self, request: Dict[str, Any], temp_dir: str, opts: DefaultOptions) -> Dict[str, str]:
        """
        Constructs a command to run a cwl/json from requests and opts, runs it,
        and deposits the outputs in outdir.

        Runner:
            opts.get_option("runner", default="cwl-runner")

        CWL (url):
            request["workflow_url"] == a url to a cwl file
        or
            request["workflow_attachment"] == input cwl text (written to a file and a url constructed for that file)

        JSON File:
            request["workflow_params"] == input json text (to be written to a file)

        :param dict request: A dictionary containing the cwl/json information.
        :param str temp_dir: Folder where input files have been staged and the
                            cwd to run at.
        :param DefaultOptions opts: contains the user's arguments; specifically
                                    the runner and runner options.

        :return: {"run_id": self.run_id, "state": state}
        """
        wf_type = request["workflow_type"].lower().strip()
        version = request["workflow_type_version"]

        if version != "v1.0" and wf_type == "cwl":
            raise RuntimeError(
                'workflow_type "cwl" requires '
                '"workflow_type_version" to be "v1.0": ' + str(version)
            )
        if version != "2.7" and wf_type == "py":
            raise RuntimeError(
                'workflow_type "py" requires '
                '"workflow_type_version" to be "2.7": ' + str(version)
            )

        logger.info("Beginning Toil Workflow ID: " + str(self.run_id))

        with open(self.start_time, "w") as f:
            f.write(get_iso_time())
        with open(self.request_json, "w") as f:
            json.dump(request, f)
        with open(self.input_json, "w") as temp:
            json.dump(request["workflow_params"], temp)

        command_args = self.write_workflow(request, opts, temp_dir, wf_type=wf_type)
        exit_code = self.call_cmd(command_args, temp_dir)

        with open(self.end_time, "w") as f:
            f.write(get_iso_time())
        with open(self.exit_code_file, "w") as f:
            f.write(str(exit_code))

        if exit_code == 0:
            open(self.stat_complete_file, "a").close()
        else:
            # non-zero exit code indicates failure
            open(self.stat_error_file, "a").close()

        return self.get_status()

    def get_state(self) -> str:
        """
        Get the state of the current workflow run. Can be one of the following:
            QUEUED,
            INITIALIZING,
            RUNNING,
            COMPLETE,
            or
            EXECUTOR_ERROR.
        """
        # the job store never existed
        if not os.path.exists(self.job_store_file):
            logger.info("Workflow " + self.run_id + ": QUEUED")
            return "QUEUED"

        # errored earlier
        if os.path.exists(self.stat_error_file):
            logger.info("Workflow " + self.run_id + ": EXECUTOR_ERROR")
            return "EXECUTOR_ERROR"

        # completed earlier
        if os.path.exists(self.stat_complete_file):
            logger.info("Workflow " + self.run_id + ": COMPLETE")
            return "COMPLETE"

        # the workflow is staged but has not run yet
        if not os.path.exists(self.stderr_file):
            logger.info("Workflow " + self.run_id + ": INITIALIZING")
            return "INITIALIZING"

        # TODO: Query with "toil status"?

        logger.info("Workflow " + self.run_id + ": RUNNING")
        return "RUNNING"

    def get_status(self) -> Dict[str, str]:
        """
        Get the status of the workflow run as a dict.
        """
        return {"run_id": self.run_id, "state": self.get_state()}


class ToilBackend(WESBackend):
    """
    WES backend implemented for Toil to run CWL, WDL, or Toil workflows.
    """

    def __init__(self, opts: List[str]) -> None:
        super(ToilBackend, self).__init__(opts)
        self.work_dir: str = os.path.join(os.getcwd(), "workflows")
        self.processes: Dict[str, "Process"] = {}

    def assert_exists(self, run_id: str) -> None:
        """
        Raises an error if the given workflow run does not exist.
        """
        if not os.path.isdir(os.path.join(self.work_dir, run_id)):
            raise WorkflowNotFoundError

    @handle_errors
    def get_service_info(self) -> Dict[str, Any]:
        """
        Get information about Workflow Execution Service.
        """
        return {
            "workflow_type_versions": {
                "PY": {"workflow_type_version": ["3.6", "3.7", "3.8", "3.9"]},
                "CWL": {"workflow_type_version": ["v1.0", "v1.1", "v1.2"]},
                "WDL": {"workflow_type_version": ["draft-2", "1.0"]},
            },
            "supported_wes_versions": ["1.0.0"],
            "supported_filesystem_protocols": ["file", "http", "https"],
            "workflow_engine_versions": {"toil": baseVersion},
            "system_state_counts": {},
            "tags": {},
        }

    @handle_errors
    def list_runs(self, page_size: Optional[int] = None, page_token: Optional[str] = None) -> Dict[str, Any]:
        """
        List the workflow runs.
        """
        # FIXME: results don't page

        if not os.path.exists(self.work_dir):
            return {"workflows": [], "next_page_token": ""}

        wf = []
        for entry in os.listdir(self.work_dir):
            if os.path.isdir(os.path.join(self.work_dir, entry)):
                wf.append(ToilWorkflow(entry))

        workflows = [{"run_id": w.run_id, "state": w.get_state()} for w in wf]
        return {"workflows": workflows, "next_page_token": ""}

    @handle_errors
    def run_workflow(self) -> Dict[str, str]:
        """
        Run a workflow.
        """
        temp_dir, body = self.collect_attachments()

        run_id = uuid.uuid4().hex
        job = ToilWorkflow(run_id)
        p = Process(target=job.run, args=(body, temp_dir, self.opts))
        p.start()
        self.processes[run_id] = p
        return {"run_id": run_id}

    @handle_errors
    def get_run_log(self, run_id: str) -> Dict[str, Any]:
        """
        Get detailed info about a workflow run.
        """
        self.assert_exists(run_id)
        return ToilWorkflow(run_id).get_log()

    @handle_errors
    def cancel_run(self, run_id: str) -> Dict[str, str]:
        """
        Cancel a running workflow.
        """
        self.assert_exists(run_id)

        # terminate the actual process that runs our command
        status = ToilWorkflow(run_id).cancel()

        if run_id in self.processes:
            # should this block with `p.is_alive()`?
            self.processes[run_id].terminate()

        if status:
            return {"run_id": run_id}

        raise RuntimeError("Failed to cancel run.  Workflow is likely canceled.")

    @handle_errors
    def get_run_status(self, run_id: str) -> Dict[str, str]:
        """
        Get quick status info about a workflow run, returning a simple result
        with the overall state of the workflow run.
        """
        self.assert_exists(run_id)
        return ToilWorkflow(run_id).get_status()
