# Block relay transport schema 2

New relay measurement records use schema `2`. Existing JSONL files remain
historical evidence and do not change format.

Each physical HTTP or JSON-RPC attempt records a `transport` object with these
fields:

- `lifecycle`: `queued`, `shared`, `dispatched`, `completed`, `timed_out`, or
  `cancelled`.
- `logical_request_id` and `physical_attempt_id`.
- `method`, `purpose`, `resource`, and endpoint identity.
- mode and mode epoch, plus an optional acquired block identity.
- request and response byte counts and monotonic lifecycle timestamps.
- HTTP status or a sanitized error class, retry reason, and
  `entered_transport`.

Logical consumers and physical attempts are separate. A queued or shared
consumer that never dispatches is not a billable physical attempt. A request
that enters the transport remains an attempt even when it times out or is
cancelled.

Analyzers must calculate cost from physical records and an explicit static
tariff. Subscription pricing remains unverified unless the analyzer has an
external, documented source. The relay does not poll billing APIs.
