# Library workload query and idle notification

Use this query to decide when to update an aggregate media index, refresh a
catalogue, or perform another action after Library work has drained.

`POST /unmanic/api/v2/pending/library/workload` accepts
`{"library_ids": [1, 2]}`. Omit `library_ids`, or pass an empty list, to select
all configured Libraries. Unknown IDs and invalid values return HTTP 400.

The response contains `scan_state`, aggregate `idle`, and a `libraries` list.
Each Library has `library_id`, `tasks` counts keyed by `creating`, `pending`,
`in_progress` and `processed`, `outstanding_tasks`, and `idle`.
A Library is idle only when the scanner is idle and its outstanding count is
zero. Scan state applies to the whole scanner, including a paused or scheduled
scan, even when querying a subset of Libraries. An unavailable scanner never
reports idle. Remote tasks marked `complete` do not count as outstanding.

Plugins can implement `emit_library_workload_idle(data)` for
`events.library_workload_idle`. It runs after the scanner returns to idle at the
end of a scan pass, including a pass with no new jobs, and after normal
post-processing removes a local task or marks a remote task complete. The
existing `events.postprocessor_complete` event retains its earlier timing.

Event data contains `library_ids` of observed idle Libraries, `reason`
(`scan_complete` or `task_complete`), and `workload` with the query response.
The post-processing notification checks the completed task's Library; the scan
notification checks all configured Libraries. Busy Libraries are omitted from
`library_ids`. No notification is sent if none of the checked Libraries is idle.

Notifications can repeat. Consumers should coalesce their own actions and query
their full dependency set again before starting work. Queries are observations,
not reservations: new submissions or scans can start afterward. Idle does not
mean previous tasks succeeded; consult task history for failures. These events
do not describe parent-child task relationships or signal a successful batch.
Manual queue deletion and other mutations outside normal post-processing do not
independently emit this notification.
