"""Compatibility surface for the stream-only daemon runtime.

Focused continuous-mode coverage lives in ``test_stream_runtime.py``.  The
legacy native observer tests were intentionally removed with the native
observation path; one-shot native discovery and handoff remain covered by
``test_discovery.py`` and ``test_handoff.py``.
"""