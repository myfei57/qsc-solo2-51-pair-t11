"""Wiring of the console and its subsystems from a configuration."""

from dataclasses import dataclass

from .audit.recorder import Recorder
from .clock import LogicalClock
from .config import Config
from .console.plant import Plant
from .console.router import Console
from .store.snapshot import Snapshot
from .store.store import Store


@dataclass
class Application:
    """Everything the process needs to serve the console."""

    config: Config
    store: Store
    clock: LogicalClock
    recorder: Recorder
    plant: Plant
    console: Console

    def snapshot(self) -> Snapshot:
        """Frozen view of the persisted document."""
        return self.store.snapshot()


def build_application(config: Config) -> Application:
    """Build the service from a configuration."""
    store = Store(config.store_path())
    clock = LogicalClock()
    recorder = Recorder(store, clock)
    # Surface a startup rollback/reset in the operational event record as well;
    # the authoritative, rollback-proof evidence stays in the recovery journal.
    boot = store.boot_report
    if boot.event is not None and boot.action != "loaded":
        recorder.log(f"state-recovery-{boot.action}", _describe_boot(store))
    plant = Plant(store, clock, recorder)
    console = Console(plant)
    return Application(
        config=config,
        store=store,
        clock=clock,
        recorder=recorder,
        plant=plant,
        console=console,
    )


def _describe_boot(store: Store) -> str:
    summary = store.boot_summary()
    if summary["action"] == "rollback":
        return (
            f"rolled back corrupt version {summary.get('rejected_version')} "
            f"(checksum {summary.get('rejected_checksum')}); restored "
            f"version {summary['version']}"
        )
    return f"state reset after unrecoverable document: {summary.get('reason')}"
