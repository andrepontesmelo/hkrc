"""Register the ``readonly_file`` toolset: read_file + search_files, no writes.

Why this exists: Hermes enables/disables tools per TOOLSET, and the built-in
``file`` toolset bundles ``patch`` and ``write_file`` with the read tools.  A
profile that needs read-only source access (the HKRC harness-loop analyzer on
profile ``authoritative``) therefore cannot list ``file``; this plugin
contributes a read-only toolset it can list instead.  Verified against
hermes-agent (v0.21.2): there is no per-tool enable/disable config key — every
persisted surface (``hermes tools``, ``platform_toolsets``, ``toolsets``,
``agent.disabled_toolsets``) is toolset-level.
"""

from toolsets import create_custom_toolset

TOOLSET = "readonly_file"
TOOLS = ("read_file", "search_files")


def register(ctx) -> None:  # noqa: ARG001 - plugin entry point signature
    """Plugin entry point: register the read-only toolset."""
    create_custom_toolset(
        TOOLSET,
        "Read-only file access: read_file + search_files, no write/patch tools",
        tools=list(TOOLS),
    )
