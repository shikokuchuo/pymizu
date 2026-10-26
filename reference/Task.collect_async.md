# Task.collect_async()

collect_async() -\> asyncio.Future

Usage

``` python
Task.collect_async()
```

The event-driven collect: an asyncio.Future resolving with the task's result (or raising the flattened remote error collect() raises), driven by the submitter's notify endpoint -- never blocks, never polls. Cancelling the Future detaches the waiter (a later blocking collect() works again); it never cancels the task -- cancel() stays the explicit verb.
