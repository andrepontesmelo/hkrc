"""Friction flag storage + inert `hkrc flag` append (schema 7 -> 8)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from hkrc.assist import _normalize_value, _opaque
from hkrc.cli import main
from hkrc.state import ControllerState, SCHEMA_VERSION


def _make_instance(tmp_path: Path) -> Path:
    """Create a full instance via the real `hkrc init` path; return config path."""

    config_path = tmp_path / "instance.toml"
    assert (
        main(
            [
                "init",
                "--config",
                str(config_path),
                "--instance-name",
                "flag-test",
                "--native-boards-root",
                str(tmp_path / "native"),
                "--state-db",
                str(tmp_path / "controller.sqlite3"),
            ]
        )
        == 0
    )
    return config_path


def _flag_rows(db_path: Path) -> list[sqlite3.Row]:
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        return connection.execute(
            "SELECT * FROM friction_flags ORDER BY id"
        ).fetchall()


def _flag_count(db_path: Path) -> int:
    with sqlite3.connect(db_path) as connection:
        return int(
            connection.execute("SELECT COUNT(*) FROM friction_flags").fetchone()[0]
        )


def _flag_argv(config_path: Path, severity: str, kind: str, note: str) -> list[str]:
    return [
        "flag",
        "--config",
        str(config_path),
        "--severity",
        severity,
        "--kind",
        kind,
        "--note",
        note,
    ]


def test_schema_bumps_to_8_with_friction_flags_table(tmp_path: Path) -> None:
    assert SCHEMA_VERSION == 8
    config_path = _make_instance(tmp_path)
    assert main(["status", "--config", str(config_path)]) == 0

    db_path = tmp_path / "controller.sqlite3"
    with sqlite3.connect(db_path) as connection:
        objects = {
            (row[0], row[1])
            for row in connection.execute(
                "SELECT name, type FROM sqlite_master WHERE type IN ('table', 'index')"
            )
        }
    assert ("friction_flags", "table") in objects
    assert ("idx_friction_flags_created_at", "index") in objects


def test_flag_happy_path_writes_one_scrubbed_row(tmp_path: Path, monkeypatch) -> None:
    config_path = _make_instance(tmp_path)
    monkeypatch.setenv("HERMES_SESSION_ID", "session-abc")
    monkeypatch.setenv("HERMES_PROFILE", "developer")

    note = "  deploy  script  touched /home/andre/.config/secrets.env  twice  "
    assert main(_flag_argv(config_path, "medium", "tooling", note)) == 0

    rows = _flag_rows(tmp_path / "controller.sqlite3")
    assert len(rows) == 1
    row = rows[0]
    assert row["severity"] == "medium"
    assert row["kind"] == "tooling"
    assert row["note"] == _normalize_value(note)
    assert row["session_ref"] == _opaque("session:session-abc")
    assert row["profile_ref"] == _opaque("profile:developer")
    assert row["created_at"]
    assert row["id"] == 1


def test_flag_without_hermes_env_writes_null_refs(tmp_path: Path, monkeypatch) -> None:
    config_path = _make_instance(tmp_path)
    monkeypatch.delenv("HERMES_SESSION_ID", raising=False)
    monkeypatch.delenv("HERMES_PROFILE", raising=False)

    assert main(_flag_argv(config_path, "low", "other", "no env here")) == 0

    rows = _flag_rows(tmp_path / "controller.sqlite3")
    assert len(rows) == 1
    assert rows[0]["session_ref"] is None
    assert rows[0]["profile_ref"] is None


def test_flag_missing_instance_state_is_fail_closed_zero_writes(
    tmp_path: Path, capsys
) -> None:
    config_path = tmp_path / "missing.toml"
    db_path = tmp_path / "controller.sqlite3"
    config_path.write_text(
        "[controller]\n"
        'name = "flag-test"\n'
        f'native_boards_root = "{tmp_path / "native"}"\n'
        f'state_db = "{db_path}"\n',
        encoding="utf-8",
    )
    assert not db_path.exists()

    assert main(_flag_argv(config_path, "high", "orchestration", "should not land")) == 2
    assert "hkrc: error:" in capsys.readouterr().err
    assert not db_path.exists()


def test_flag_rejects_bad_input_before_any_write(tmp_path: Path, capsys) -> None:
    config_path = _make_instance(tmp_path)
    db_path = tmp_path / "controller.sqlite3"
    assert _flag_count(db_path) == 0

    # argparse-rejected inputs exit via SystemExit(2) before the handler runs.
    for severity, kind, note in (
        ("urgent", "tooling", "bad severity"),
        ("low", "invented", "bad kind"),
    ):
        with pytest.raises(SystemExit) as excinfo:
            main(_flag_argv(config_path, severity, kind, note))
        assert excinfo.value.code != 0
    # Handler-rejected inputs return non-zero pre-write.
    for note in ("   ", ""):
        assert main(_flag_argv(config_path, "low", "tooling", note)) == 2
    captured = capsys.readouterr()
    assert "hkrc: error:" in captured.err or "usage:" in captured.err
    assert _flag_count(db_path) == 0


def test_flag_append_is_inert_state_file_delta_is_only_the_flag_row(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = _make_instance(tmp_path)
    db_path = tmp_path / "controller.sqlite3"
    monkeypatch.setenv("HERMES_SESSION_ID", "session-abc")
    monkeypatch.setenv("HERMES_PROFILE", "developer")

    # Duplicate rows are allowed by design (no dedup at insert).
    for _ in range(2):
        assert main(_flag_argv(config_path, "low", "working-agreement", "first inert flag")) == 0

    rows = _flag_rows(db_path)
    assert [row["note"] for row in rows] == [
        "first inert flag",
        "first inert flag",
    ]
    assert [row["id"] for row in rows] == [1, 2]

    # Inert append proof: no other controller table gained rows, and the
    # harness-loop state file was never touched.
    with sqlite3.connect(db_path) as connection:
        friction_rows = {
            table: int(
                connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            )
            for table in (
                "friction_flags",
                "assist_candidates",
                "assist_candidate_events",
                "blocker_reservations",
                "interventions",
                "resolutions",
                "handled_events",
                "stream_handled_events",
            )
        }
    assert friction_rows["friction_flags"] == 2
    assert all(
        count == 0 for name, count in friction_rows.items() if name != "friction_flags"
    )
    assert not (tmp_path / "harness_loop_state.json").exists()
    assert list(tmp_path.glob("*.json")) == []


def test_friction_flag_row_survives_reopen(tmp_path: Path) -> None:
    config_path = _make_instance(tmp_path)
    db_path = tmp_path / "controller.sqlite3"
    assert main(_flag_argv(config_path, "high", "other", "persist me")) == 0

    with ControllerState.open_existing(db_path) as state:
        assert state.friction_flag_count() == 1
        assert state.schema_version == SCHEMA_VERSION


def test_open_existing_upgrades_pre8_database_additively(tmp_path: Path) -> None:
    config_path = _make_instance(tmp_path)
    db_path = tmp_path / "controller.sqlite3"

    # Simulate a pre-8 database: table dropped, version marker rolled back.
    with sqlite3.connect(db_path) as connection:
        connection.execute("DROP TABLE friction_flags")
        connection.execute(
            "UPDATE schema_meta SET value = '7' WHERE key = 'schema_version'"
        )
        connection.commit()

    # open_existing re-runs the additive schema without recreating the file.
    with ControllerState.open_existing(db_path) as state:
        assert state.schema_version == 8
        assert (
            state.record_friction_flag(
                severity="low",
                kind="tooling",
                note="post-migration",
                session_ref=None,
                profile_ref=None,
            )
            == 1
        )
    assert _flag_count(db_path) == 1
