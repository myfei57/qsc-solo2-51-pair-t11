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
        f"committed keys {application.store.count()} at tick {application.clock.tick}",
        flush=True,
    )
    print(_version_line(application), flush=True)
    serve(config, application.console)
    return 0


def _version_line(application) -> str:
    """State, plainly, which persisted version this process is running on."""
    boot = application.store.boot_summary()
    checksum = boot.get("checksum") or "-"
    action = boot["action"]
    if action == "rollback":
        return (
            f"state version {boot['version']} (checksum {checksum}) via rollback: "
            f"rejected version {boot.get('rejected_version')} "
            f"({boot.get('reason')}, evidence {str(boot.get('evidence_checksum'))[:12]})"
        )
    if action == "reset":
        return (
            f"state reset to empty after unrecoverable document "
            f"({boot.get('reason')}, evidence {str(boot.get('evidence_checksum'))[:12]})"
        )
    if action == "legacy-import":
        return f"state imported from a legacy document (checksum {checksum})"
    if action == "fresh":
        return "state version 0 (no persisted document)"
    return f"state version {boot['version']} (checksum {checksum}) loaded from disk"


if __name__ == "__main__":
    raise SystemExit(main())
