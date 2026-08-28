"""Allow ``python -m hkrc`` to invoke the CLI."""

from .cli import main

raise SystemExit(main())
