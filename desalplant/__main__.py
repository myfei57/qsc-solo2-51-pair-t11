"""Command line entry point: ``python -m desalplant``."""

from .app import build_application
from .config import parse_args
from .console.server import serve


def main(argv: list[str] | None = None) -> int:
    """Build the service and serve the console until interrupted."""
    config = parse_args(argv)
    application = build_application(config)
    print(f"control console listening on http://{config.host}:{config.port}", flush=True)
    print(
        f"committed keys {application.store.count()} at tick {application.clock.tick} "
        f"generation {application.store.generation}",
        flush=True,
    )
    if application.store.recovered:
        report = application.store.recovery_report()
        print(
            f"recovery rolled back {application.store.rollback_count} time(s); "
            f"now on generation {application.store.generation} "
            f"checksum {application.store.checksum()}",
            flush=True,
        )
        if report is not None and report.record is not None:
            record = report.record
            print(
                f"  rollback: generation {record.from_generation} -> {record.to_generation} "
                f"({record.reason})",
                flush=True,
            )
    serve(config, application.console)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
