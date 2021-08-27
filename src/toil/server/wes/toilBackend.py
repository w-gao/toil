import json
import logging
import os
import shutil
import signal
import subprocess
import uuid
from multiprocessing import Process
from typing import Optional, List, Dict, Any, Generator, Tuple, Union, Type

from toil.server.api.abstractBackend import WESBackend
from toil.server.wes.models import PythonWorkflow, CWLWorkflow, WDLWorkflow, WESWorkflow
from toil.server.utils import handle_errors, WorkflowNotFoundError, get_iso_time, DefaultOptions
from toil.version import baseVersion

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class ToilWorkflowExecutor:
    """
    Responsible for creating and executing submitted workflows.

    Local implementation -
    Interacts with the workflows/ directory to store and retrieve information
    associated with all the workflow runs through the filesystem.
    """

    def __init__(self) -> None:
        self.work_dir = os.path.join(os.getcwd(), "workflows")
        self.workflow_types: Dict[str, Type[WESWorkflow]] = {}

    def register_workflow_type(self, name: str, workflow: Type[WESWorkflow]) -> None:
        """
        Register a workflow type that can be run with this executor.
        """
        self.workflow_types[name.lower()] = workflow

    def _join_work_dir(self, run_id: str, *args: str) -> str:
        """
        Returns the full path to the given file located under the the run_id
        workflow directory.
        """
        return os.path.join(self.work_dir, run_id, *args)

    def fetch(self, run_id: str, filename: str, default: Optional[str] = None) -> Optional[str]:
        """
        Returns the contents of the given file. If the file does not exist, the
        default value is returned.
        """
        if os.path.exists(self._join_work_dir(run_id, filename)):
            with open(self._join_work_dir(run_id, filename), "r") as f:
                return f.read()
        return default

    def write(self, run_id: str, filename: str, content: str) -> None:
        """
        Write a file to the directory of the given run.
        """
        with open(self._join_work_dir(run_id, filename), "w") as f:
            f.write(content)

    def exists(self, run_id: str) -> bool:
        """
        Returns True if the workflow run exists.
        """
        return os.path.isdir(self._join_work_dir(run_id))

    def get_state(self, run_id: str) -> str:
        """
        Returns the state of the given run.
        """
        return self.fetch(run_id, "state") or "UNKNOWN"

    def set_state(self, run_id: str, state: str) -> None:
        """
        Set the state for a run.
        """
        logger.info(f"Workflow {run_id}: {state}")
        self.write(run_id, "state", state)

    def get_runs(self) -> Generator[Tuple[str, str], None, None]:
        """
        A generator of a list of run ids and their state.
        """
        if not os.path.exists(self.work_dir):
            return

        for run_id in os.listdir(self.work_dir):
            if os.path.isdir(self._join_work_dir(run_id)):
                yield run_id, self.get_state(run_id)

    def set_up_run(self, run_id: str) -> str:
        """
        Calls when a new workflow run has been requested. This creates a
        temporary directory for this workflow to run.

        :returns: Returns the directory where attachments are staged and the
                  workflow should be executed.
        """
        out_dir = self._join_work_dir(run_id, "out_dir")
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        self.set_state(run_id, "QUEUED")
        return self._join_work_dir(run_id, "run_dir")

    def create_workflow(self, run_id: str, request: Dict[str, Any], options: DefaultOptions) -> WESWorkflow:
        """
        Creates a WESWorkflow object from the user request.

        :param run_id: The run id.
        :param request: The request dictionary containing user input parameters
                        for the workflow execution.
        :param options: A list of default options that should be attached when
                        starting the workflow.
        """
        wf_type = request["workflow_type"].lower().strip()
        version = request["workflow_type_version"]

        wf = self.workflow_types.get(wf_type)
        if not wf:
            raise RuntimeError(f"workflow_type '{wf_type}' is not supported.")
        if version not in wf.supported_versions():
            raise RuntimeError("workflow_type '{}' requires 'workflow_type_version' to be one of '{}'.  Got '{}'"
                               "instead.".format(wf_type, str(wf.supported_versions()), version))

        logger.info(f"Beginning Toil Workflow ID: {run_id}")
        self.set_state(run_id, "INITIALIZING")

        self.write(run_id, "starttime", get_iso_time())
        self.write(run_id, "request.json", json.dumps(request))
        self.write(run_id, "wes_input.json", json.dumps(request["workflow_params"]))

        parameters = request.get("workflow_engine_parameters", None)
        if parameters:
            # TODO: user supplied options
            pass
        opts = options.get_options("extra")

        # create an instance of the workflow
        return wf(run_id=run_id,
                  work_dir=self._join_work_dir(run_id),
                  workflow_url=request["workflow_url"],
                  input_json=self._join_work_dir(run_id, "wes_input.json"),
                  options=opts)

    def run_workflow(self, workflow: WESWorkflow) -> None:
        """ Runs the workflow in a separate multiprocessing Process."""
        run_id = workflow.run_id
        run_dir = workflow.run_dir

        workflow.prepare_run()
        # store the jobStore location so we can access the output files later
        self.write(run_id, "job_store", workflow.job_store)

        self.set_state(run_id, "RUNNING")

        exit_code = self.call_cmd(run_id=run_id,
                                  cmd=workflow.construct_command(),
                                  cwd=run_dir)

        self.write(run_id, "endtime", get_iso_time())
        self.write(run_id, "exit_code", str(exit_code))

        if exit_code == 0:
            self.set_state(run_id, "COMPLETE")
        else:
            # non-zero exit code indicates failure
            self.set_state(run_id, "EXECUTOR_ERROR")

    def call_cmd(self, run_id: str, cmd: Union[List[str], str], cwd: str) -> int:
        """
        Calls a command with Popen. Writes stdout, stderr, and the command to
        separate files. This is a blocking call.

        :returns: The exit code of the command.
        """
        self.write(run_id, "cmd", " ".join(cmd))

        stdout_f = self._join_work_dir(run_id, "stdout")
        stderr_f = self._join_work_dir(run_id, "stderr")

        with open(stdout_f, "w") as stdout, open(stderr_f, "w") as stderr:
            logger.info("Calling: " + " ".join(cmd))
            process = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, close_fds=True, cwd=cwd)

        self.write(run_id, "pid", str(process.pid))
        return process.wait()

    def cancel_run(self, run_id: str) -> bool:
        """ Kill the workflow process."""
        if self.get_state(run_id) not in ("QUEUED", "INITIALIZING", "RUNNING"):
            return False
        self.set_state(run_id, "CANCELING")

        pid = self.fetch(run_id, "pid")

        if not pid:  # process was not created
            return False

        try:
            # signal an interrupt to kill the process gently
            os.kill(int(pid), signal.SIGINT)
        except ProcessLookupError:
            return False

        self.set_state(run_id, "CANCELED")
        return True

    def get_run_log(self, run_id: str) -> Dict[str, Any]:
        """ Get detailed info about a workflow run."""
        state = self.get_state(run_id)

        # TODO: move this to ToilBackend
        request = json.loads(self.fetch(run_id, "request.json") or "{}")
        job_store = self.fetch(run_id, "job_store") or ""

        stdout = self.fetch(run_id, "stdout")
        stderr = self.fetch(run_id, "stderr")
        exit_code = self.fetch(run_id, "exit_code")
        start_time = self.fetch(run_id, "starttime")
        end_time = self.fetch(run_id, "endtime")
        cmd = (self.fetch(run_id, "cmd") or "").split("\n")

        output_obj = {}
        if state == "COMPLETE":
            out_dir = self._join_work_dir(run_id, "out_dir")

            # only tested locally
            if job_store.startswith("file:"):
                for file in os.listdir(out_dir):
                    if file.startswith("out_tmpdir"):
                        shutil.rmtree(os.path.join(out_dir, file))
                for file in os.listdir(out_dir):
                    output_obj[file] = {
                        "location": os.path.join(out_dir, file),
                        "size": os.stat(os.path.join(out_dir, file)).st_size,
                        "class": "File",
                    }

        return {
            "run_id": run_id,
            "request": request,
            "state": state,
            "run_log": {
                "cmd": cmd,
                "start_time": start_time,
                "end_time": end_time,
                # TODO: stdout and stderr should be a URL that points to the output file, not the actual contents.
                "stdout": stdout,
                "stderr": stderr,
                "exit_code": int(exit_code) if exit_code is not None else None,
            },
            "task_logs": [],
            "outputs": output_obj,
        }


class ToilBackend(WESBackend):
    """
    WES backend implemented for Toil to run CWL, WDL, or Toil workflows.
    """

    def __init__(self, opts: List[str]) -> None:
        super(ToilBackend, self).__init__(opts)
        self.processes: Dict[str, "Process"] = {}
        self.executor = ToilWorkflowExecutor()

        self.executor.register_workflow_type("py", PythonWorkflow)
        self.executor.register_workflow_type("cwl", CWLWorkflow)
        self.executor.register_workflow_type("wdl", WDLWorkflow)

    @handle_errors
    def get_service_info(self) -> Dict[str, Any]:
        """ Get information about Workflow Execution Service."""

        return {
            "workflow_type_versions": {
                k: {
                    "workflow_type_version": v.supported_versions()
                } for k, v in self.executor.workflow_types.items()
            },
            "supported_wes_versions": ["1.0.0"],
            "supported_filesystem_protocols": ["file", "http", "https"],
            "workflow_engine_versions": {"toil": baseVersion},
            "system_state_counts": {},
            "tags": {},
        }

    @handle_errors
    def list_runs(self, page_size: Optional[int] = None, page_token: Optional[str] = None) -> Dict[str, Any]:
        """ List the workflow runs."""
        # TODO: implement pagination
        workflows = [{"run_id": run_id, "state": state}
                     for run_id, state in self.executor.get_runs()]

        return {"workflows": workflows, "next_page_token": ""}

    @handle_errors
    def run_workflow(self) -> Dict[str, str]:
        """ Run a workflow."""
        run_id = uuid.uuid4().hex

        # TODO: refine logic
        # create the execution directory for the run
        run_dir = self.executor.set_up_run(run_id)

        # collect user uploaded files and configurations from the body of the request
        body = self.collect_attachments(run_id, temp_dir=run_dir)

        try:
            workflow = self.executor.create_workflow(run_id, request=body, options=self.opts)
        except:
            # let users know that their request is invalid
            self.executor.set_state(run_id, "EXECUTOR_ERROR")
            raise

        p = Process(target=self.executor.run_workflow, args=(workflow,))
        p.start()
        self.processes[run_id] = p

        return {"run_id": run_id}

    @handle_errors
    def get_run_log(self, run_id: str) -> Dict[str, Any]:
        """ Get detailed info about a workflow run."""
        if not self.executor.exists(run_id):
            raise WorkflowNotFoundError

        return self.executor.get_run_log(run_id)

    @handle_errors
    def cancel_run(self, run_id: str) -> Dict[str, str]:
        """ Cancel a running workflow."""
        if not self.executor.exists(run_id):
            raise WorkflowNotFoundError

        # terminate the actual process that runs our command
        status = self.executor.cancel_run(run_id)

        if run_id in self.processes:
            # should this block with `p.is_alive()`?
            self.processes[run_id].terminate()

        if not status:
            raise RuntimeError("Failed to cancel run.  Workflow is likely canceled.")

        return {
            "run_id": run_id
        }

    @handle_errors
    def get_run_status(self, run_id: str) -> Dict[str, str]:
        """
        Get quick status info about a workflow run, returning a simple result
        with the overall state of the workflow run.
        """
        if not self.executor.exists(run_id):
            raise WorkflowNotFoundError

        return {
            "run_id": run_id,
            "state": self.executor.get_state(run_id)
        }
