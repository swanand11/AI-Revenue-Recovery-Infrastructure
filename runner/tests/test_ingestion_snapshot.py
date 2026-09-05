from runner import store


def test_snapshot_replacement_keeps_old_readers_on_complete_document(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "BASE_DIR", tmp_path)
    store.write_json("events", [{"event_id": "old"}])
    with (tmp_path / "events.json").open() as old_reader:
        store.write_json("events", [{"event_id": "new"}])
        assert '"old"' in old_reader.read()
    assert store.read_json("events", []) == [{"event_id": "new"}]
