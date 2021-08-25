# Copyright (C) 2015-2021 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import functools
import logging
from datetime import datetime
from typing import Callable, Any

logger = logging.getLogger(__name__)


class WorkflowNotFoundError(Exception):
    """
    Raised when the user specified run ID is not found.
    """
    def __init__(self) -> None:
        super(WorkflowNotFoundError, self).__init__("The requested workflow run wasn't found.")


def handle_errors(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    This decorator catches errors from the wrapped function and returns a JSON
    formatted error message with the appropriate status code defined by the
    GA4GH WES spec.
    """

    def error(msg, code: int = 500):  # type: ignore
        logger.exception(f"Exception raised when calling '{func.__name__}()':")
        return {"msg": str(msg), "status_code": code}, code

    @functools.wraps(func)
    def wrapper(*args, **kwargs):  # type: ignore
        try:
            return func(*args, **kwargs)
        except (TypeError, ValueError) as e:
            return error(e, code=400)  # the request is malformed
        except (FileNotFoundError, WorkflowNotFoundError) as e:
            return error(e, code=404)
        except Exception as e:
            return error(e, code=500)

    return wrapper


def get_iso_time() -> str:
    """
    Returns the current time in ISO 8601 format.
    """
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
