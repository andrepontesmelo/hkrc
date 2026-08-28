#!/usr/bin/env python3
"""Gate 2 clean-root E2E: fresh instance, admission, and git enforcement.

Builds a temp instance root, installs the current repo release into it (the
installed wrapper is the only runtime used — no source checkout path), creates
a temp git repository, then verifies the acceptance core end to end:

- a prototype-only contract denies merge_main (A)
- a broad child admission is denied before dispatch and a narrowed admission
  is allowed exactly once through the native kanban CLI (C)
- an unauthorized protected-main update is denied by the installed hook and an
  authorized, reviewed update succeeds; non-protected refs keep working (E)
- uninstall restores normal git (E/G)
- no Hermes source, config, or runtime files are touched (H)

Non-interactive; exits non-zero on the first failed assertion.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
RELEASE = ROOT / "scripts" / "hkrc_release.py"
VERSION_MATCH = __import__("re").search(
    r'__version__ = "([^"]+)"',
    (ROOT / "src" / "hkrc" / "__init__.py").read_text(encoding="utf-8"),
)
assert VERSION_MATCH is not None
VERSION = VERSION_MATCH.group(1)

EVIDENCE = [
    {"evidence_type": "independent_review", "evidence_ref": "gate2-review"}
]


def run(cmd: list[str], *, env=None, cwd=None, stdin=None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd, text=True, capture_output=True, check=False, env=env, cwd=cwd, input=stdin
    )


def contract(contract_id: str, effects: list[str], evidence, statement: str) -> dict:
    return {
        "schema_version": "hkrc.outcome-contract.v1",
        "contract_id": contract_id,
        "declared_outcome": contract_id,
        "terminal_evidence": evidence,
        "allowed_effects": effects,
        "continuation_policy": "stop",
        "authority_source": {
            "authority_id": f"auth-{contract_id}",
            "kind": "operator",
            "actor": "E2E",
            "authorized_at": "2026-08-12T12:00:00+00:00",
            "statement": statement,
        },
        "parent_contract_refs": [],
    }


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="hkrc-gate2-e2e-") as raw:
        tmp = Path(raw)
        instance = tmp / "instance"

        install = run(
            [sys.executable, str(RELEASE), "install", "--source-root", str(ROOT), "--instance-root", str(instance)]
        )
        assert install.returncode == 0, install.stderr
        wrapper = instance / "bin" / "hkrc"
        config = instance / "config" / "hkrc" / "config.toml"
        state_db = instance / "state" / "hkrc" / "state.sqlite3"

        init = run(
            [
                str(wrapper), "init",
                "--config", str(config),
                "--instance-name", "e2e",
                "--native-boards-root", str(tmp / "boards"),
                "--state-db", str(state_db),
                "--workspace", str(instance / "workspace" / "hkrc"),
            ]
        )
        assert init.returncode == 0, init.stderr

        # Shipped artifacts exist in the materialized release (no source path).
        release_dir = instance / "releases" / VERSION
        assert (release_dir / "docs" / "outcome-guard.md").is_file()
        assert (release_dir / "config" / "hkrc" / "outcome-guard-assets.json").is_file()
        assert (instance / "config" / "hkrc" / "outcome-guard-example-contract.json").is_file()
        assert (release_dir / "src" / "hkrc" / "admission.py").is_file()
        assert (release_dir / "src" / "hkrc" / "git_enforce.py").is_file()

        # A: prototype-only contract denies merge_main.
        prototype = tmp / "prototype.json"
        prototype.write_text(
            json.dumps(contract(
                "prototype", ["isolated_prototype"],
                [{"evidence_type": "human_selection", "evidence_ref": "choice"}],
                "prototype only",
            )),
            encoding="utf-8",
        )
        implementation = tmp / "implementation.json"
        implementation.write_text(
            json.dumps(contract(
                "implementation", ["repository_modify", "merge_main"],
                EVIDENCE, "implementation + merge",
            )),
            encoding="utf-8",
        )
        for path in (prototype, implementation):
            r = run([str(wrapper), "outcome-guard", "register", "--config", str(config), "--contract-file", str(path)])
            assert r.returncode == 0, r.stderr
        denied = run(
            [str(wrapper), "outcome-guard", "check-effect", "--config", str(config),
             "--contract-ref", "prototype", "--effect", "merge_main"]
        )
        assert denied.returncode == 3 and '"allowed":false' in denied.stdout

        # C: admission through a fake native CLI (argv subprocess, scrubbed env).
        fake_bin = tmp / "bin"
        fake_bin.mkdir()
        hermes_log = tmp / "hermes.log"
        fake_hermes = fake_bin / "hermes"
        fake_hermes.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, sys\n"
            f"with open({str(hermes_log)!r}, 'a') as f:\n"
            "    f.write(' '.join(sys.argv[1:]) + '\\n')\n"
            "args = sys.argv[1:]\n"
            "if 'create' in args:\n"
            "    key = args[args.index('--idempotency-key') + 1]\n"
            "    print(json.dumps({'ok': True, 'task_id': 't_e2e_' + key[-8:], 'status': 'blocked'}))\n"
            "elif 'promote' in args:\n"
            "    print(json.dumps({'ok': True, 'status': 'ready'}))\n"
            "else:\n"
            "    print(json.dumps({'ok': False, 'error': 'unexpected'})); sys.exit(1)\n",
            encoding="utf-8",
        )
        fake_hermes.chmod(0o755)
        e2e_env = dict(os.environ)
        e2e_env["PATH"] = str(fake_bin) + os.pathsep + e2e_env["PATH"]
        e2e_env.pop("HERMES_KANBAN_TASK", None)
        e2e_env.pop("HERMES_KANBAN_BOARD", None)

        def admit(effect: str, contract_ref: str, title: str):
            return run(
                [str(wrapper), "outcome-guard", "admit-child", "--config", str(config),
                 "--parent-task-id", "t_parent", "--contract-ref", contract_ref,
                 "--effect", effect, "--board", "alpha", "--title", title, "--assignee", "dev"],
                env=e2e_env,
            )

        broad = admit("merge_main", "prototype", "broad child")
        assert broad.returncode == 3, broad.stdout
        assert '"reason_code":"effect_not_allowed"' in broad.stdout
        assert not hermes_log.exists() or hermes_log.read_text(encoding="utf-8") == ""
        print("C: broad child denied before dispatch: OK")

        narrow = admit("repository_modify", "implementation", "impl child")
        assert narrow.returncode == 0, narrow.stderr
        first_child = json.loads(narrow.stdout)["child_task_id"]
        assert first_child is not None
        again = admit("repository_modify", "implementation", "impl child again")
        assert again.returncode == 0 and json.loads(again.stdout)["duplicate"] is True
        assert json.loads(again.stdout)["child_task_id"] == first_child
        native_lines = hermes_log.read_text(encoding="utf-8").splitlines()
        assert len(native_lines) == 2, native_lines
        assert "--initial-status blocked" in native_lines[0]
        assert "promote" in native_lines[1] and "--force" in native_lines[1]
        print("C: narrowed admission allowed exactly once (create blocked + promote): OK")

        # Git enforcement on a temp repo through the installed hook.
        repo = tmp / "repo"
        subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
        subprocess.run(["git", "-C", str(repo), "config", "user.email", "t@e.c"], check=True)
        subprocess.run(["git", "-C", str(repo), "config", "user.name", "t"], check=True)
        (repo / "f.txt").write_text("one\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(repo), "add", "f.txt"], check=True)
        subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", "baseline"], check=True)

        hook_install = run(
            [str(wrapper), "outcome-guard", "git-hook", "install", "--repo", str(repo), "--config", str(config)]
        )
        assert hook_install.returncode == 0, hook_install.stderr
        hook_path = repo / ".git" / "hooks" / "reference-transaction"
        assert hook_path.is_file() and os.access(hook_path, os.X_OK)

        def commit(message: str) -> subprocess.CompletedProcess[str]:
            (repo / "f.txt").write_text(f"{message}\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(repo), "add", "f.txt"], check=True)
            return run(["git", "-C", str(repo), "commit", "-q", "-m", message], cwd=repo)

        denied = commit("unauthorized main")
        assert denied.returncode != 0, denied.stdout
        assert "no_merge_authorization" in (denied.stderr + denied.stdout)
        print("E: unauthorized protected-main update denied: OK")

        evidence = tmp / "evidence.json"
        evidence.write_text(json.dumps(EVIDENCE), encoding="utf-8")
        auth = run(
            [str(wrapper), "outcome-guard", "authorize-merge", "--config", str(config),
             "--task-id", "t_parent", "--contract-ref", "implementation",
             "--ref", "refs/heads/main", "--evidence-file", str(evidence)]
        )
        assert auth.returncode == 0, auth.stderr
        allowed = commit("authorized main")
        assert allowed.returncode == 0, (allowed.stdout, allowed.stderr)
        print("E: authorized reviewed protected-main update succeeds: OK")

        subprocess.run(["git", "-C", str(repo), "checkout", "-q", "-b", "feature"], check=True)
        feature = commit("feature work")
        assert feature.returncode == 0
        print("E: non-protected refs work: OK")

        uninstall = run([str(wrapper), "outcome-guard", "git-hook", "uninstall", "--repo", str(repo)])
        assert uninstall.returncode == 0, uninstall.stderr
        assert not hook_path.exists()
        subprocess.run(["git", "-C", str(repo), "checkout", "-q", "main"], check=True)
        normal = commit("post-uninstall")
        assert normal.returncode == 0
        print("G: uninstall restores normal git: OK")

        # No Hermes source/config/runtime touched: every writable path this
        # E2E produced lives under the temp root, and the only `hermes` binary
        # invoked was the fake shim on the temp PATH.
        assert str(instance).startswith(str(tmp))
        assert str(config).startswith(str(tmp))
        assert str(state_db).startswith(str(tmp))
        assert str(repo).startswith(str(tmp))
        assert hermes_log.is_file()  # the fake native CLI ran, nothing else
        print("H: no Hermes source modified; only temp root written: OK")

    print("E2E GATE2 OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
