# prune()

Remove orphaned shared memory regions.

Usage

[Source](https://github.com/shikokuchuo/pymizu/blob/main/python/pymizu/__init__.py#L1444-L1472)

``` python
prune()
```

Removes the pymizu shared memory regions that dead processes leave behind. Needed only in exceptional circumstances. Cleanup after a crash is automatic as long as any participant of the pool or channel survives it. The survivor detects the death and reclaims the dead process's regions itself. Orphans arise only when every attached process dies at once (for example, the whole process group is killed), and no survivor remains to run that cleanup. They persist until pruned or until the machine reboots.

Regions of running processes are never touched. If another pymizu session on the machine is itself recovering from a crash, do not run this. Values sent by a now-dead peer stay readable by its survivor until consumed, and this function removes them.

A crashed process cannot clean up after itself, and a new process that happens to reuse its PID cannot reap its orphans either (it reads its own PID as alive). Run [prune()](../reference/prune.md#pymizu.prune) while the PID is free, before reuse.

## Returns

`The region names removed, or an empty list if none were. On`  
platforms whose shared memory namespace cannot be enumerated (Windows, where orphans cannot exist), always an empty list.
