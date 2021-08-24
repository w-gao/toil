import os
import uuid
from multiprocessing import Process
from typing import Optional

from toil.server.wes import WESBackend, ToilWorkflow
from toil.server.wes.runners import PythonRunner, CWLRunner, WDLRunner
from toil.server.wes.utils import handle_errors, WorkflowNotFoundError
from toil.version import baseVersion


class ToilBackend(WESBackend):
    """
    WES backend implemented for Toil to run CWL, WDL, or Toil workflows.
    """

    def __init__(self, opts):
        super(ToilBackend, self).__init__(opts)
        self.work_dir = os.path.join(os.getcwd(), "workflows")
        self.runners = {
            "py": PythonRunner,
            "cwl": CWLRunner,
            "wdl": WDLRunner
        }
        self.processes = {}

    def assert_exists(self, run_id: str) -> None:
        """
        Raises an error if the given workflow run does not exist.
        """
        if not os.path.isdir(os.path.join(self.work_dir, run_id)):
            raise WorkflowNotFoundError

    @handle_errors
    def get_service_info(self) -> dict:
        """
        Get information about Workflow Execution Service.
        """
        return {
            "workflow_type_versions": {
                k: {
                    "workflow_type_version": v.supported_versions()
                } for k, v in self.runners.items()
            },
            "supported_wes_versions": ["1.0.0"],
            "supported_filesystem_protocols": ["file", "http", "https"],
            "workflow_engine_versions": {"toil": baseVersion},
            "system_state_counts": {},
            "tags": {},
        }

    @handle_errors
    def list_runs(self, page_size: Optional[int] = None, page_token: Optional[str] = None) -> dict:
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
    def run_workflow(self) -> dict:
        """
        Run a workflow.
        """
        tempdir, body = self.collect_attachments()

        run_id = uuid.uuid4().hex
        job = ToilWorkflow(run_id)
        p = Process(target=job.run, args=(body, tempdir, self.opts))
        p.start()
        self.processes[run_id] = p
        return {"run_id": run_id}

    @handle_errors
    def get_run_log(self, run_id: str) -> dict:
        """
        Get detailed info about a workflow run.
        """
        self.assert_exists(run_id)
        return ToilWorkflow(run_id).get_log()

    @handle_errors
    def cancel_run(self, run_id: str) -> dict:
        """
        Cancel a running workflow.
        """
        self.assert_exists(run_id)

        # terminate the actual process that runs our command
        ToilWorkflow(run_id).cancel()

        if run_id in self.processes:
            # should this block with `p.is_alive()`?
            self.processes[run_id].terminate()

        return {"run_id": run_id}

    @handle_errors
    def get_run_status(self, run_id: str) -> dict:
        """
        Get quick status info about a workflow run, returning a simple result
        with the overall state of the workflow run.
        """
        self.assert_exists(run_id)
        return ToilWorkflow(run_id).get_status()
