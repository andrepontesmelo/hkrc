from __future__ import annotations

from pathlib import Path
import sqlite3

import pytest

from hkrc.cli import main
from hkrc.config import (
    ConfigError,
    ControllerConfig,
    StreamConfig,
    default_config,
    load_config,
    write_config,
)
from hkrc.state import ControllerState, StateError


def test_config_round_trip(tmp_path: Path) -> None:
    config = ControllerConfig(
        instance_name="work-a",
        native_boards_root=tmp_path / "native-boards",
        state_db=tmp_path / "controller.sqlite3",
    )
    path = tmp_path / "config.toml"
    write_config(path, config)

    assert load_config(path) == config
    assert "native_boards_root" in path.read_text()


def test_legacy_config_without_stream_section_remains_manual_compatible(tmp_path: Path) -> None:
    path = tmp_path / "legacy.toml"
    path.write_text(
        """format_version = 1

[instance]
name = "legacy"
native_boards_root = "/tmp/hermes/kanban/boards"

[controller]
state_db = "/tmp/hermes/state/hkrc/state.sqlite3"
workspace = "/tmp/hermes/workspace/hkrc"

[native]
cli = "hermes"

[telegram]
chat_id = ""
chat_id_env = "HKRC_TELEGRAM_CHAT_ID"
""",
        encoding="utf-8",
    )

    config = load_config(path)

    assert config.stream == StreamConfig()
    assert config.stream.mode == "manual_compatibility"


def test_stream_config_is_opt_in_and_round_trips_without_credentials(tmp_path: Path) -> None:
    config = ControllerConfig(
        instance_name="stream-a",
        native_boards_root=tmp_path / "native-boards",
        state_db=tmp_path / "controller.sqlite3",
        stream=StreamConfig(
            enabled=True,
            adapter="approved_websocket",
            endpoint="wss://dashboard.example.test/api/plugins/kanban/events",
            boards=("main", "ops"),
            credential_env="HKRC_STREAM_TICKET",
            current_state_reader="approved-dashboard-snapshot",
        ),
    )
    path = tmp_path / "config.toml"
    write_config(path, config)

    assert load_config(path) == config
    text = path.read_text(encoding="utf-8")
    assert "enabled = true" in text
    assert "approved_websocket" in text
    assert "HKRC_STREAM_TICKET" in text
    assert "ticket-value" not in text


def test_enabled_stream_mode_without_board_allowlist_is_valid_for_runtime_discovery(
    tmp_path: Path,
) -> None:
    config = ControllerConfig(
        instance_name="stream-a",
        native_boards_root=tmp_path / "native-boards",
        state_db=tmp_path / "controller.sqlite3",
        stream=StreamConfig(
            enabled=True,
            adapter="approved_websocket",
            endpoint="wss://dashboard.example.test/api/plugins/kanban/events",
            boards=(),
            credential_env="HKRC_STREAM_TICKET",
            current_state_reader="approved-dashboard-snapshot",
        ),
    )
    path = tmp_path / "config.toml"
    write_config(path, config)

    assert load_config(path) == config
    text = path.read_text(encoding="utf-8")
    assert "boards = []" in text
    assert "all non-archived boards" in text


def test_stream_config_rejects_enabled_mode_without_approved_wiring() -> None:
    with pytest.raises(ConfigError, match="enabled stream mode"):
        StreamConfig(enabled=True)

    with pytest.raises(ConfigError, match="disabled"):
        StreamConfig(adapter="approved_websocket")

    with pytest.raises(ConfigError, match="wss://"):
        StreamConfig(
            enabled=True,
            adapter="approved_websocket",
            endpoint="https://dashboard.example.test/events",
            boards=("main",),
            credential_env="HKRC_STREAM_TICKET",
            current_state_reader="approved-dashboard-snapshot",
        )


@pytest.mark.parametrize(
    ("endpoint", "accepted"),
    [
        ("ws://127.0.0.1/events", True),
        ("ws://localhost/events", True),
        ("ws://[::1]/events", True),
        ("wss://dashboard.example.test/events", True),
        ("wss://100.64.12.34/events", True),
        ("ws://dashboard.example.test/events", False),
        ("ws://100.64.12.34/events", False),
    ],
)
def test_stream_endpoint_allows_plain_websocket_only_on_loopback(
    endpoint: str, accepted: bool
) -> None:
    if accepted:
        config = StreamConfig(
            enabled=True,
            adapter="approved_websocket",
            endpoint=endpoint,
            boards=("main",),
            credential_env="HKRC_STREAM_TICKET",
            current_state_reader="approved-dashboard-snapshot",
        )
        assert config.endpoint == endpoint
    else:
        with pytest.raises(ConfigError, match="stream endpoint"):
            StreamConfig(endpoint=endpoint)


def test_cli_init_stream_gate_requires_complete_approved_configuration(tmp_path: Path) -> None:
    config_path = tmp_path / "config.toml"
    state_path = tmp_path / "state.sqlite3"
    native_path = tmp_path / "native"

    assert main(
        [
            "init",
            "--config",
            str(config_path),
            "--instance-name",
            "stream-instance",
            "--native-boards-root",
            str(native_path),
            "--state-db",
            str(state_path),
            "--stream-enabled",
            "--stream-adapter",
            "approved_websocket",
            "--stream-endpoint",
            "wss://dashboard.example.test/events",
            "--stream-board",
            "main",
            "--stream-credential-env",
            "HKRC_STREAM_TICKET",
            "--stream-current-state-reader",
            "approved-dashboard-snapshot",
        ]
    ) == 0

    config = load_config(config_path)
    assert config.stream == StreamConfig(
        enabled=True,
        adapter="approved_websocket",
        endpoint="wss://dashboard.example.test/events",
        boards=("main",),
        credential_env="HKRC_STREAM_TICKET",
        current_state_reader="approved-dashboard-snapshot",
    )


def test_config_rejects_bad_instance_name(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="instance_name"):
        ControllerConfig(
            instance_name="../other",
            native_boards_root=tmp_path / "native",
            state_db=tmp_path / "state.sqlite3",
        )


def test_state_is_controller_owned_and_idempotent(tmp_path: Path) -> None:
    state_path = tmp_path / "state" / "controller.sqlite3"
    with ControllerState.initialize(state_path, "default") as state:
        assert state.instance_name == "default"
        assert state.schema_version == 7

    with ControllerState.initialize(state_path, "default") as state:
        assert state.instance_name == "default"
        assert state.schema_version == 7

    with sqlite3.connect(state_path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    assert {"schema_meta", "controller_identity"} <= tables


def test_state_rejects_cross_instance_reuse(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    ControllerState.initialize(state_path, "one").close()

    with pytest.raises(StateError, match="belongs to instance"):
        ControllerState.initialize(state_path, "two")


def test_open_existing_does_not_create(tmp_path: Path) -> None:
    missing = tmp_path / "missing.sqlite3"
    with pytest.raises(StateError, match="not found"):
        ControllerState.open_existing(missing)
    assert not missing.exists()


def test_intervention_state_is_one_ever_and_records_phases(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    with ControllerState.initialize(state_path, "default") as state:
        assert state.reserve_blocker(
            "board-a", "task-a", blocker_kind="capability", latest_event_at=10
        )
        assert state.begin_intervention("board-a", "task-a")
        assert not state.begin_intervention("board-a", "task-a")
        state.record_intervention_phase(
            "board-a", "task-a", "commented", outcome="ok"
        )
        row = state.get_intervention("board-a", "task-a")

    assert row is not None
    assert row["phase"] == "commented"
    assert row["outcome"] == "ok"


def test_cli_init_and_status_are_explicitly_non_recovery(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "instance.toml"
    state_path = tmp_path / "controller.sqlite3"
    native_path = tmp_path / "native"

    assert (
        main(
            [
                "init",
                "--config",
                str(config_path),
                "--instance-name",
                "test-instance",
                "--native-boards-root",
                str(native_path),
                "--state-db",
                str(state_path),
            ]
        )
        == 0
    )
    init_output = capsys.readouterr().out
    assert "read-only boundary" in init_output
    assert not native_path.exists()

    assert main(["status", "--config", str(config_path)]) == 0
    status_output = capsys.readouterr().out
    assert "native_boards_root=" in status_output
    assert "(not scanned)" in status_output
    assert "schema_version=7" in status_output
    assert "stream_mode=manual_compatibility" in status_output
    assert "stream_enabled=false" in status_output


def test_cli_daemon_requires_explicit_stream_enablement(tmp_path: Path, capsys) -> None:
    config_path = tmp_path / "instance.toml"
    config = ControllerConfig(
        "test-instance",
        tmp_path / "native",
        tmp_path / "controller.sqlite3",
    )
    write_config(config_path, config)

    assert main(["daemon", "--config", str(config_path), "--max-cycles", "1"]) == 2
    assert "stream mode is disabled" in capsys.readouterr().err
    assert not (tmp_path / "native").exists()


def test_default_config_is_absolute() -> None:
    config = default_config()
    assert config.native_boards_root.is_absolute()
    assert config.state_db.is_absolute()


def test_discovery_config_round_trips_custom_threshold(tmp_path: Path) -> None:
    config = ControllerConfig(
        instance_name="work-a",
        native_boards_root=tmp_path / "native-boards",
        state_db=tmp_path / "controller.sqlite3",
        unclaimed_child_after_seconds=300,
        recency_window_seconds=86400,
    )
    path = tmp_path / "config.toml"
    write_config(path, config)

    assert load_config(path) == config
    text = path.read_text(encoding="utf-8")
    assert "[discovery]" in text
    assert "unclaimed_child_after_seconds = 300" in text
    assert "recency_window_seconds = 86400" in text


def test_legacy_config_without_discovery_section_defaults_threshold(tmp_path: Path) -> None:
    path = tmp_path / "legacy.toml"
    path.write_text(
        """format_version = 1

[instance]
name = "legacy"
native_boards_root = "/tmp/hermes/kanban/boards"

[controller]
state_db = "/tmp/hermes/state/hkrc/state.sqlite3"

[native]
cli = "hermes"

[telegram]
chat_id = ""
chat_id_env = "HKRC_TELEGRAM_CHAT_ID"
""",
        encoding="utf-8",
    )

    config = load_config(path)

    assert config.unclaimed_child_after_seconds == 1800
    assert config.recency_window_seconds == 3600
    assert config.stream == StreamConfig()


@pytest.mark.parametrize(
    ("value", "fragment"),
    [
        (0, "unclaimed_child_after_seconds = 0"),
        (-5, "unclaimed_child_after_seconds = -5"),
        ('"sixty"', 'unclaimed_child_after_seconds = "sixty"'),
    ],
)
def test_discovery_config_rejects_non_positive_threshold(
    tmp_path: Path, value: object, fragment: str
) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        f"""format_version = 1

[instance]
name = "work-a"
native_boards_root = "/tmp/hermes/kanban/boards"

[controller]
state_db = "/tmp/hermes/state/hkrc/state.sqlite3"

[discovery]
{fragment}
""",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="unclaimed_child_after_seconds"):
        load_config(path)


def test_controller_config_rejects_non_positive_threshold_directly(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="unclaimed_child_after_seconds"):
        ControllerConfig(
            instance_name="work-a",
            native_boards_root=tmp_path / "native-boards",
            state_db=tmp_path / "controller.sqlite3",
            unclaimed_child_after_seconds=0,
        )


def test_harness_loop_analysis_max_attempts_round_trips(tmp_path: Path) -> None:
    config = ControllerConfig(
        instance_name="work-a",
        native_boards_root=tmp_path / "native-boards",
        state_db=tmp_path / "controller.sqlite3",
    )
    path = tmp_path / "config.toml"
    write_config(path, config)

    assert "analysis_max_attempts = 2" in path.read_text()
    assert load_config(path) == config


@pytest.mark.parametrize(
    "fragment",
    ["analysis_max_attempts = 0", 'analysis_max_attempts = "two"'],
)
def test_harness_loop_config_rejects_bad_analysis_max_attempts(
    tmp_path: Path, fragment: str
) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        f"""format_version = 1

[instance]
name = "work-a"
native_boards_root = "/tmp/hermes/kanban/boards"

[controller]
state_db = "/tmp/hermes/state/hkrc/state.sqlite3"

[harness_loop]
{fragment}
""",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="analysis_max_attempts"):
        load_config(path)


@pytest.mark.parametrize(
    "fragment",
    ["recency_window_seconds = 0", "recency_window_seconds = -5", 'recency_window_seconds = "sixty"'],
)
def test_discovery_config_rejects_non_positive_recency_window(
    tmp_path: Path, fragment: str
) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        f"""format_version = 1

[instance]
name = "work-a"
native_boards_root = "/tmp/hermes/kanban/boards"

[controller]
state_db = "/tmp/hermes/state/hkrc/state.sqlite3"

[discovery]
{fragment}
""",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="recency_window_seconds"):
        load_config(path)


def test_controller_config_rejects_non_positive_recency_window_directly(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="recency_window_seconds"):
        ControllerConfig(
            instance_name="work-a",
            native_boards_root=tmp_path / "native-boards",
            state_db=tmp_path / "controller.sqlite3",
            recency_window_seconds=0,
        )


def test_cli_init_writes_discovery_section_with_default_threshold(tmp_path: Path) -> None:
    config_path = tmp_path / "instance.toml"
    state_path = tmp_path / "controller.sqlite3"
    native_path = tmp_path / "native"

    assert (
        main(
            [
                "init",
                "--config",
                str(config_path),
                "--instance-name",
                "test-instance",
                "--native-boards-root",
                str(native_path),
                "--state-db",
                str(state_path),
            ]
        )
        == 0
    )

    config = load_config(config_path)
    assert config.unclaimed_child_after_seconds == 1800
    assert "[discovery]" in config_path.read_text(encoding="utf-8")
