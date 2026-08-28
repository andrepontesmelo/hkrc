# Deterministic event-stream fixture

`event_stream.py` is a test-only, in-process source for exercising the future HKRC event observer. It deliberately does not import HKRC runtime code, open SQLite, inspect Hermes paths, or classify blockers. The fixture models the transport boundary so observer tests can be deterministic and offline.

## Protocol assumptions

- `StreamEvent.event_id` is an opaque ordered resume cursor. IDs may be sparse; consumers must not infer missing events from gaps.
- A consumer calls `connect(cursor=N)` and then reads frames. The server returns retained events with `event_id > N` in ascending order.
- A consumer owns the durable cursor. It should advance only after deciding how to handle a frame; reconnect uses the last safely processed cursor.
- Delivery is at-least-once: `duplicate_ids` intentionally repeats selected frames. Idempotency belongs to the consumer, not this transport fixture.
- `disconnect_after` raises `StreamProtocolError(DISCONNECTED)` and drops the connection. A caller reconnects with its last cursor.
- Retention is represented by `retain_from(F)`. A nonzero cursor older than the retained floor raises `RETENTION_RESET`; connecting at cursor zero is an explicit full-resync choice over retained history.
- Payloads must be mapping-shaped JSON-like values. `malformed_ids` and non-mapping payloads produce `MALFORMED_PAYLOAD` frames with the event cursor, allowing tests to verify whether the observer logs/skips/fails according to its policy.
- Event kinds are strings. Kinds outside `known_kinds` produce `UNKNOWN_EVENT_KIND` frames; the fixture does not silently coerce or discard them.
- `StreamProtocolError` is the transport seam for disconnects and server-directed resets. `StreamEnvelope.error` is the frame-level seam for event decode/type errors. Both carry stable codes and, where applicable, a cursor.

## Test seams

- Use `StreamScenario.from_events(...)` for a fixed ordered history.
- Set `duplicate_ids`, `disconnect_after`, `malformed_ids`, or `known_kinds` to inject one deterministic condition.
- Call `retain_from(...)` between connections to simulate retention trimming.
- Read with `read()` for step-by-step assertions or `frames()` until retained history is exhausted.
- The fixture is intentionally not a production observer implementation. Future observer code should depend on a small protocol interface (connect/read/disconnect) and use this simulator in unit and integration-style tests.

Run its contract tests with:

```bash
uv run pytest tests/test_event_stream_fixture.py -q
```
