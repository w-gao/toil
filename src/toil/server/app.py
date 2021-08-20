import argparse
import sys
import connexion

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

    # workflow execution service (WES) server
    app = connexion.FlaskApp(__name__, specification_dir='ga4gh_wes_api_spec/')
    backend = connexion.utils.get_function_from_name("toil.server.api.ToilBackend")(args.opt)

    def rs(x):
        return getattr(backend, x.split(".")[-1])

    app.add_api('workflow_execution_service.swagger.yaml', resolver=connexion.resolver.Resolver(rs))
    app.run(port=args.port, debug=args.debug)


if __name__ == "__main__":
    main(sys.argv[1:])
