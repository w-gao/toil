import os
import uuid
from multiprocessing import Process
from typing import Optional

from toil.server.wes import WESBackend, ToilWorkflow
from toil.server.wes.abstractBackend import handle_errors
from toil.server.wes.runners import WES_WORKFLOW_RUNNERS
from toil.version import baseVersion


class ToilBackend(WESBackend):
    """
    """
    processes = {}

    @handle_errors
    def get_service_info(self) -> dict:
        workflow_type_versions = {
            k: {"workflow_type_version": v.supported_versions()} for k, v in WES_WORKFLOW_RUNNERS.items()
        }

        return {
            "workflow_type_versions": workflow_type_versions,
            "supported_wes_versions": ["1.0.0"],
            "supported_filesystem_protocols": ["file", "http", "https"],
            "workflow_engine_versions": {"toil": baseVersion},
            "system_state_counts": {},
            "tags": {},
        }

    @handle_errors
    def list_runs(self, page_size: Optional[int] = None, page_token: Optional[str] = None) -> dict:
        # FIXME: results don't page
        if not os.path.exists(os.path.join(os.getcwd(), "workflows")):
            return {"workflows": [], "next_page_token": ""}

        wf = []
        for entry in os.listdir(os.path.join(os.getcwd(), "workflows")):
            if os.path.isdir(os.path.join(os.getcwd(), "workflows", entry)):
                wf.append(ToilWorkflow(entry))

        workflows = [{"run_id": w.run_id, "state": w.getstate()[0]} for w in wf]
        return {"workflows": workflows, "next_page_token": ""}

    @handle_errors
    def run_workflow(self) -> dict:
        tempdir, body = self.collect_attachments()

        run_id = uuid.uuid4().hex
        job = ToilWorkflow(run_id)
        p = Process(target=job.run, args=(body, tempdir, self))
        p.start()
        self.processes[run_id] = p
        return {"run_id": run_id}

    @handle_errors
    def get_run_log(self, run_id: str) -> dict:
        # TODO: check if the workflow exists first
        job = ToilWorkflow(run_id)
        return job.get_log()

    @handle_errors
    def cancel_run(self, run_id: str) -> dict:
        # should this block with `p.is_alive()`?
        if run_id in self.processes:
            self.processes[run_id].terminate()
        return {"run_id": run_id}

    @handle_errors
    def get_run_status(self, run_id: str) -> dict:
        job = ToilWorkflow(run_id)
        return job.getstatus()
