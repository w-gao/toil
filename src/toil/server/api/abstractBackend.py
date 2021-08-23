import tempfile
import json
import os
import logging
from abc import abstractmethod, ABC
from typing import Optional, List

import connexion
from werkzeug.utils import secure_filename


def visit(d, op):
    """Recursively call op(d) for all list subelements and dictionary 'values' that d may have."""
    op(d)
    if isinstance(d, list):
        for i in d:
            visit(i, op)
    elif isinstance(d, dict):
        for i in d.values():
            visit(i, op)


class WESBackend(ABC):
    """Stores and retrieves options.  Intended to be inherited."""

    def __init__(self, opts: List[str]):
        """Parse and store options as a list of tuples."""
        self.pairs = []
        for o in opts if opts else []:
            k, v = o.split("=", 1)
            self.pairs.append((k, v))

    def resolve_operation_id(self, operation_id: str):
        """
        A function that maps an operationId defined in the OpenAPI or swagger
        yaml file to a function.

        :param operation_id: The operationId.
        :returns: A function that should be called when the given endpoint is
                  reached.
        """
        return getattr(self, operation_id.split(".")[-1])

    @abstractmethod
    def get_service_info(self) -> dict:
        """
        Get information about Workflow Execution Service.

        GET /service-info
        """
        raise NotImplementedError

    @abstractmethod
    def list_runs(self, page_size: Optional[int] = None, page_token: Optional[str] = None) -> dict:
        """
        List the workflow runs.

        GET /runs
        """
        raise NotImplementedError

    @abstractmethod
    def run_workflow(self) -> dict:
        """
        Run a workflow. This endpoint creates a new workflow run and returns
        a `RunId` to monitor its progress.

        POST /runs
        """
        raise NotImplementedError

    @abstractmethod
    def get_run_log(self, run_id: str) -> dict:
        """
        Get detailed info about a workflow run.

        GET /runs/{run_id}
        """
        raise NotImplementedError

    @abstractmethod
    def cancel_run(self, run_id: str) -> dict:
        """
        Cancel a running workflow.

        POST /runs/{run_id}/cancel
        """
        raise NotImplementedError

    @abstractmethod
    def get_run_status(self, run_id: str) -> dict:
        """
        Get quick status info about a workflow run, returning a simple result
        with the overall state of the workflow run.

        GET /runs/{run_id}/status
        """
        raise NotImplementedError

    # --- helper function ---

    def getopt(self, p: str, default: Optional[str] = None) -> str:
        """Returns the first option value stored that matches p or default."""
        for k, v in self.pairs:
            if k == p:
                return v
        return default

    def getoptlist(self, p: str) -> List[str]:
        """Returns all option values stored that match p as a list."""
        optlist = []
        for k, v in self.pairs:
            if k == p:
                optlist.append(v)
        return optlist

    def log_for_run(self, run_id: str, message: str) -> None:
        logging.info("Workflow %s: %s", run_id, message)

    def collect_attachments(self, run_id: Optional[str] = None) -> tuple:
        """
        """
        tempdir = tempfile.mkdtemp()
        body = {}
        has_attachments = False
        for k, ls in connexion.request.files.lists():
            try:
                for v in ls:
                    if k == "workflow_attachment":
                        sp = v.filename.split("/")
                        fn = []
                        for p in sp:
                            if p not in ("", ".", ".."):
                                fn.append(secure_filename(p))
                        dest = os.path.join(tempdir, *fn)
                        if not os.path.isdir(os.path.dirname(dest)):
                            os.makedirs(os.path.dirname(dest))
                        self.log_for_run(
                            run_id,
                            f"Staging attachment '{v.filename}' to '{dest}'",
                        )
                        v.save(dest)
                        has_attachments = True
                        body[k] = (
                            "file://%s" % tempdir
                        )  # Reference to temp working dir.
                    elif k in ("workflow_params", "tags", "workflow_engine_parameters"):
                        content = v.read()
                        body[k] = json.loads(content.decode("utf-8"))
                    else:
                        body[k] = v.read().decode()
            except Exception as e:
                raise ValueError(f"Error reading parameter '{k}': {e}")
        for k, ls in connexion.request.form.lists():
            try:
                for v in ls:
                    if not v:
                        continue
                    if k in ("workflow_params", "tags", "workflow_engine_parameters"):
                        body[k] = json.loads(v)
                    else:
                        body[k] = v
            except Exception as e:
                raise ValueError(f"Error reading parameter '{k}': {e}")

        if "workflow_url" in body:
            if ":" not in body["workflow_url"]:
                if not has_attachments:
                    raise ValueError(
                        "Relative 'workflow_url' but missing 'workflow_attachment'"
                    )
                body["workflow_url"] = "file://%s" % os.path.join(
                    tempdir, secure_filename(body["workflow_url"])
                )
            self.log_for_run(
                run_id, "Using workflow_url '%s'" % body.get("workflow_url")
            )
        else:
            raise ValueError("Missing 'workflow_url' in submission")

        if "workflow_params" not in body:
            raise ValueError("Missing 'workflow_params' in submission")

        return tempdir, body
