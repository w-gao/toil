import argparse
import sys
import connexion
from connexion import Resolver
from flask_cors import CORS

from toil.version import version


def main(argv=None):
    parser = argparse.ArgumentParser(description="The Toil Workflow Execution Service Server")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument("--version", action="store_true", default=False)
    parser.add_argument("--opt", "-o", type=str, action="append",
                        help="Example: '--opt runner=cwltoil --opt extra=--logLevel=CRITICAL' "
                             "or '--opt extra=--workDir=/'.  Accepts multiple values.")
    args = parser.parse_args(argv)

    if args.version:
        print(version)
        exit(0)

    app = connexion.FlaskApp(__name__,
                             specification_dir='ga4gh_api_spec/',
                             options={
                                 # "swagger_ui": False
                             })

    # enable cross origin resource sharing
    CORS(app.app)

    # workflow execution service (WES) API
    backend = connexion.utils.get_function_from_name("toil.server.api.ToilBackend")(args.opt)

    app.add_api('workflow_execution_service.swagger.yaml',
                resolver=Resolver(function_resolver=backend.resolve_operation_id))

    # start the development server
    app.run(port=args.port, debug=args.debug)


if __name__ == "__main__":
    main(sys.argv[1:])
