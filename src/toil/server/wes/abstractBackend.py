import tempfile
import json
import os
import logging
from abc import abstractmethod, ABC
from typing import Optional, List, Tuple, Dict, Any

import connexion  # type: ignore
from werkzeug.utils import secure_filename


class DefaultOptions:
    """
    Stores and retrieves options.
    """
    def __init__(self, opts: List[str]):
        """Parse and store options as a list of tuples."""
        self.pairs = []
        for o in opts if opts else []:
            k, v = o.split("=", 1)
            self.pairs.append((k, v))

    def get_option(self, p: str, default: Optional[str] = None) -> Optional[str]:
        """
        Returns the first option value stored that matches p or default.
        """
        for k, v in self.pairs:
            if k == p:
                return v
        return default

    def get_options(self, p: str) -> List[str]:
        """
        Returns all option values stored that match p as a list.
        """
        opt_list = []
        for k, v in self.pairs:
            if k == p:
                opt_list.append(v)
        return opt_list


class WESBackend(ABC):
    """
    Represents a workflow execution service (WES) API backend. Intended to be
    inherited. Subclasses should implement all abstract methods to handle user
    requests when they hit different endpoints.
    """

    def __init__(self, opts: List[str]):
        """
        :param opts: A list of default options that should be considered when
                     starting all workflows.
        """
        self.opts = DefaultOptions(opts)

    def resolve_operation_id(self, operation_id: str) -> Any:
        """
        A function that maps an operationId defined in the OpenAPI or swagger
        yaml file to a function.

        :param operation_id: The operationId.
        :returns: A function that should be called when the given endpoint is
                  reached.
        """
        return getattr(self, operation_id.split(".")[-1])

    @abstractmethod
    def get_service_info(self) -> Dict[str, Any]:
        """
        Get information about Workflow Execution Service.

        GET /service-info
        """
        raise NotImplementedError

    @abstractmethod
    def list_runs(self, page_size: Optional[int] = None, page_token: Optional[str] = None) -> Dict[str, Any]:
        """
        List the workflow runs.

        GET /runs
        """
        raise NotImplementedError

    @abstractmethod
    def run_workflow(self) -> Dict[str, str]:
        """
        Run a workflow. This endpoint creates a new workflow run and returns
        a `RunId` to monitor its progress.

        POST /runs
        """
        raise NotImplementedError

    @abstractmethod
    def get_run_log(self, run_id: str) -> Dict[str, Any]:
        """
        Get detailed info about a workflow run.

        GET /runs/{run_id}
        """
        raise NotImplementedError

    @abstractmethod
    def cancel_run(self, run_id: str) -> Dict[str, str]:
        """
        Cancel a running workflow.

        POST /runs/{run_id}/cancel
        """
        raise NotImplementedError

    @abstractmethod
    def get_run_status(self, run_id: str) -> Dict[str, str]:
        """
        Get quick status info about a workflow run, returning a simple result
        with the overall state of the workflow run.

        GET /runs/{run_id}/status
        """
        raise NotImplementedError

    # --- helper functions ---

    @staticmethod
    def log_for_run(run_id: Optional[str], message: str) -> None:
        logging.info("Workflow %s: %s", run_id, message)

    def collect_attachments(self, run_id: Optional[str] = None) -> Tuple[str, Dict[str, Any]]:
        """
        Collect the attachments that are provided along with the request.

        :returns: The temporary directory where uploaded files are staged, and
                  a dictionary of input parameters provided by the user.
        """
        temp_dir = tempfile.mkdtemp()
        body = {}
        has_attachments = False
        for key, ls in connexion.request.files.lists():
            try:
                for value in ls:
                    # uploaded files that are required to execute the workflow
                    if key == "workflow_attachment":

                        # guard against maliciously constructed filenames
                        sp = value.filename.split("/")
                        fn = []
                        for p in sp:
                            if p not in ("", ".", ".."):
                                fn.append(secure_filename(p))
                        dest = os.path.join(temp_dir, *fn)
                        if not os.path.isdir(os.path.dirname(dest)):
                            os.makedirs(os.path.dirname(dest))

                        self.log_for_run(run_id, f"Staging attachment '{value.filename}' to '{dest}'")
                        value.save(dest)
                        has_attachments = True
                        body[key] = f"file://{temp_dir}"  # Reference to temp working dir.

                    elif key in ("workflow_params", "tags", "workflow_engine_parameters"):
                        content = value.read()
                        body[key] = json.loads(content.decode("utf-8"))
                    else:
                        body[key] = value.read().decode()
            except Exception as e:
                raise ValueError(f"Error reading parameter '{key}': {e}")

        # form data
        for key, ls in connexion.request.form.lists():
            try:
                for value in ls:
                    if not value:
                        continue
                    if key in ("workflow_params", "tags", "workflow_engine_parameters"):
                        body[key] = json.loads(value)
                    else:
                        body[key] = value
            except Exception as e:
                raise ValueError(f"Error reading parameter '{key}': {e}")

        if "workflow_url" in body:
            if ":" not in body["workflow_url"]:
                if not has_attachments:
                    raise ValueError("Relative 'workflow_url' but missing 'workflow_attachment'")

                body["workflow_url"] = "file://%s" % os.path.join(
                    temp_dir, secure_filename(body["workflow_url"])
                )
            self.log_for_run(run_id, "Using workflow_url '%s'" % body.get("workflow_url"))
        else:
            raise ValueError("Missing 'workflow_url' in submission")

        if "workflow_params" not in body:
            raise ValueError("Missing 'workflow_params' in submission")

        return temp_dir, body
