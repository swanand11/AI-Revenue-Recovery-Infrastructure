from common.wal import WalWriter


class RecoveryAudit:
    def __init__(self, wal: WalWriter) -> None:
        self.wal = wal

    def write(self, record: dict) -> None:
        self.wal.write_event({"event_type": "recovery_audit", "record": record})
