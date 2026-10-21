# Design: key/value platform

Group: Group 2

The roster is how the group records who owns which plugin.

## Roster

Each person lists a different plugin filename.

| Student name | CUNY alias | Plugin filename | Feature branch |
| --- | --- | --- | --- |
| Adil Ahmed | ADIL.AHMED22 | request_routing.py | adil.ahmed22/request_routing |
| Amanda Farghli | Amanda.Farghli66 | partitioning.py | Amanda.Farghli66/partitioning |
| Almasur Antor | almasur.antor49 | session_models.py | almasur.antor49/session_models |
| Mamuna Chaudhry | mamuna.chaudhry91 | replication.py |  mamuna.chaudhry91/replication |
| Oluwadamilola Babajide | oluwadamilola.babajide11 | write_completion.py |oluwadamilola.babajide11/write_completion  |

The other five files (leader_replication.py, conflicts.py, processing_guarantees.py, leader_election.py, atomic_commit.py) are not claimed and stay as the provided stubs.

## Platform reminder

Values are opaque bytes, and so are keys. `N` is `len(node.nodes)`, whatever appears in `--peers` or `cluster.json`, so no plugin may assume three nodes. A forwarded Write or Read carries `hop`, and every plugin stops at 8. Every node reads the same membership list, but each process keeps its own store, directory and fault flags.


## Per owned plugin

### request_routing.py (Adil Ahmed)

**Plugin file:** request_routing.py

**Modes we will implement** (see that plugin's section in `week06/README.md`): FORWARD, DIRECTORY_TIER, CLIENT_AWARE. All three.

**One history we will demo** (the fault or change, the Writes/Reads, the bytes you expect on which node): three nodes, key `alpha` is owned by n2, and the client sends the Write to n0, which does not own it.

- FORWARD. n0 sends the Write on to n2 with `hop` 1. n2 stores `one`. The client gets `ok=True owner=n2`. A Read on n2 returns `one`, and a Read sent to n0 with FORWARD is also answered with `one` from n2.
- CLIENT_AWARE. The same kind of Write sent to n1 comes back `ok=False error='not owner' owner=n2`. Nothing is stored. The Read on n2 still returns the old bytes, so the refused call did not put `nope` anywhere.
- DIRECTORY_TIER. n0 looks `alpha` up in its directory, finds n2 and sends the Write there. n2 stores `two`. If the lookup is stale (n0 thinks the owner is n1, but the key was moved to n2), n1 answers `not owner` and names n2, n0 retries once against n2, and the bytes land on n2. The Write is not dropped.
- Hop limit. Two nodes that disagree about the owner (n0 says n1, n1 says n0) pass the Write back and forth. Each pass adds 1 to `hop`. When it passes 8 the Write fails with `hop limit` instead of looping.

**Commands** (provided client, against a `node` cluster): see the Demo section, steps 3 to 8. The routing mode goes on the command with `--request-routing`, for example:

```
python3 -m platform.client --addr 127.0.0.1:9600 write alpha one --partitioning DIRECTORY --request-routing FORWARD
python3 -m platform.client --addr 127.0.0.1:9601 write alpha nope --partitioning DIRECTORY --request-routing CLIENT_AWARE
python3 -m platform.client --addr 127.0.0.1:9600 write alpha two --partitioning DIRECTORY --request-routing DIRECTORY_TIER
```

**A mode combination that should fail, and the error Write or Read returns:** CLIENT_AWARE with a Write on a node that is not the owner returns `ok=False error='not owner'` and names the owner. FORWARD between two nodes that each point at the other returns `ok=False error='hop limit'`. A Write with no `--request-routing` flag on a non owner also returns `not owner`, which is the node's own default.

### partitioning.py (Amanda Farghli)

**Plugin file:** partitioning.py  
**Modes we will implement:**  week06/README.md): HASH_MOD, SLOTS, KEY_RANGE, DIRECTORY.
**One history we will demo:**  (the fault or change, the Writes/Reads, the bytes you expect on which node): starting with a two-node cluster (n0, n1), we write keys across partitions, add a third node (n2), and trigger rebalancing to show how keys move or stay put.
**Commands:**  Commands (provided client, against a node cluster): partitioned writes, node additions, and reads with --partitioning:
**A mode combination that should fail, and the error Write or Read returns:**   Calling a write with --partitioning DIRECTORY that attempts to update a key explicitly pinned to a non-existent node returns ok=False error='unknown node'. Writing with an unsupported or missing partitioning scheme mode string raises ok=False error='unsupported partitioning mode'. Reading a key under KEY_RANGE where the range boundaries are uninitialized or disjoint returns ok=False error='unassigned key range'.

### session_models.py (Almasur Antor)

**Plugin file:** session_models.py

**Modes we will implement** (see that plugin's section in `week06/README.md`): READ_OWN_WRITES, MONOTONIC_READS, and NO_SESSION. All three.

**One history we will demo** (the fault or change, the Writes/Reads, the bytes you expect on which node): `alpha` is pinned to n0 with DIRECTORY. The client writes `alpha=one` in session `s1` on n0, while n1 has no current copy.

- READ_OWN_WRITES. n0 records the log sequence number of the successful Write for session `s1`. A Read using `s1` is sent to n1. Instead of returning n1's stale or missing value, n1 sends the Read to n0. n0 verifies that its current sequence is at least the session's write sequence and returns `one`.
- MONOTONIC_READS. The first Read in session `s2` records the sequence seen on n0. A second Read using `s2` is sent to n1. It is served through n0 and returns the same or a newer sequence. A lower sequence is not returned.
- NO_SESSION. A Read sent to n1 receives no extra session handling. It may return stale bytes, miss the key, or return `not owner`, depending on the normal routing and replication modes.
- Failure. If n0 is paused or unreachable, a strict session Read fails instead of returning stale bytes from n1.

The sequence number is stored separately from the value, so two Writes containing the same bytes are still different versions.

**Commands** (provided client, against a `node` cluster): see the Demo section, steps 9 to 12. For example:

```
python3 -m platform.client --addr 127.0.0.1:9600 write alpha one --partitioning DIRECTORY --session-id s1 --session-models READ_OWN_WRITES
python3 -m platform.client --addr 127.0.0.1:9601 read alpha --partitioning DIRECTORY --session-id s1 --session-models READ_OWN_WRITES
python3 -m platform.client --addr 127.0.0.1:9601 read alpha --partitioning DIRECTORY --session-models NO_SESSION --request-routing CLIENT_AWARE
```

**A mode combination that should fail, and the error Write or Read returns:** READ_OWN_WRITES or MONOTONIC_READS without a session ID returns `ok=False error='session id required'`. If the node holding the session sequence is paused or unreachable, the Read returns `ok=False error='session unavailable'` instead of returning stale bytes.

### replication.py (Mamuna Chaudhry)

**Plugin file:** replication.py  

**Modes we will implement:** (see that plugin’s section in week06/README.md) STATE, LOG, LOG_WITH_SNAPSHOT 

**One history we will demo:**  We will pause a node that is not the owner, write alpha=one and then alpha=two on another node using --write-completion ACK_ONE. Then we will unpause the paused node and check that it catches up. With STATE, the paused node receives the latest value. With LOG, it applies both writes in order. With LOG_WITH_SNAPSHOT, we pause the node across several writes and demonstrate that a snapshot can help it catch up. The final value of alpha should match across the different nodes.

**Commands:** (provided client, against a node cluster): pause a non-owner node, write to another node using ACK_ONE, then unpause the node and verify it catches up. The replication mode goes on the command with --replication, for example:
```
python3 -m platform.client --addr 127.0.0.1:9600 write alpha one --write-completion ACK_ONE --replication STATE
python3 -m platform.client --addr 127.0.0.1:9600 write alpha two --write-completion ACK_ONE --replication LOG
python3 -m platform.client --addr 127.0.0.1:9600 write alpha three --write-completion ACK_ONE --replication LOG_WITH_SNAPSHOT
```
**A mode combination that should fail, and the error Write or Read returns:**  The assignment does not specify a required failure scenario for the replication modes. 

### write_completion.py (Oluwadamilola Babajide)

**Plugin file:** write_completion.py

**Modes we will implement** (see that plugin's section in `week06/README.md`): ACK_ALL, ACK_ONE, ACK_QUORUM. All three.

**One history we will demo** (the fault or change, the Writes/Reads, the bytes you expect on which node): three nodes, the client sends a Write to `n0`, and we pause nodes to test completion requirements based on `W` (where `N=3`).

- ACK_ONE. Pause `n2`. The client sends a Write to `n0`. The Write returns `ok=True` almost immediately because W=1 is met by `n0` itself. A Read on the paused node (`n2`) might return empty or stale bytes. Once `n2` is unpaused, a later Read on it repairs the copy and returns the new bytes.
- ACK_QUORUM. W is computed as `N//2 + 1` (which is 2 for a 3-node cluster). Pause `n2` (fewer than a majority). The client sends a Write to `n0`. It returns `ok=True` because `n0` and `n1` can acknowledge it. Next, pause both `n1` and `n2`. A Write to `n0` times out and does not return `ok`. A later Read hitting only a paused node may still be old until repair.
- ACK_ALL. Pause `n2`. The client sends a Write to `n0`. The Write does not return `ok` (it waits or times out) because W=3 cannot be satisfied. Once `n2` is unpaused, the Write completes, and a Read on `n2` is guaranteed to have the new value.

**Commands** (provided client, against a node cluster): see the Demo section for exact steps. The mode goes on the command with `--write-completion`, for example:

```
python3 -m platform.client --addr 127.0.0.1:9600 write alpha one --write-completion ACK_ONE
python3 -m platform.client --addr 127.0.0.1:9600 write alpha two --write-completion ACK_QUORUM
python3 -m platform.client --addr 127.0.0.1:9600 write alpha three --write-completion ACK_ALL
```
**A mode combination that should fail, and the error Write or Read returns:** ACK_ALL with a Write while any one member node is paused/unreachable returns `ok=False` and a timeout error. ACK_QUORUM with a Write while a majority of nodes are paused (e.g., 2 out of 3 nodes) returns `ok=False` and a timeout error because the required number of acknowledgments (`N//2 + 1`) cannot be reached.

## Tradeoffs

Each row is one fault or change and what that mode gives up compared with the other modes of the same plugin.

### partitioning.py

*Think Distributed Systems* ch. 7; *Designing Data-Intensive Applications* ch. 7.

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| HASH_MOD | Node membership changes (e.g., adding n2 to n0, n1). | Hash modulus changes from N to N+1; writes route to the new computed owner `hash(k) % (N+1)`. | Stale or empty read on new owner until mass rebalance finishes; historical values become inaccessible on old owners without full re-hash migration. | Minimal data movement on resize: almost all existing keys (approx. N/(N+1)) remap and must be transferred across the network. |
| KEY_RANGE | Skewed write workload / hot key range (e.g., sequential timestamps or keys sharing a common prefix). | All writes falling within the active range hammer a single target node (e.g., n0), creating a write bottleneck. | High read latency and queuing on the overloaded partition node; other nodes sit idle. | Uniform load balancing: highly susceptible to hot-spotting, unlike hash-based schemes that scatter contiguous keys. |
| SLOTS | Node added or removed from cluster. | Routes key by hashing to a fixed slot count (S >> N), directing the write to whichever node owns that slot; only approx. 1/(N+1) slots migrate. | Consistent read once the affected slot finishes migrating to the new owner; unaffected slots read immediately with zero disruption. | Locality / efficient range queries: keys are uniformly distributed across discrete slots, requiring scatter-gather scans across nodes for range reads. |
| DIRECTORY | Node addition or membership topology change. | Consults lookup table; honors explicit `pin` by routing the write directly to the pinned node regardless of key value or cluster size. | Returns stored value strictly from the pinned/assigned node listed in the directory tier; a stale directory cache returns `not owner`. | Zero-overhead routing: requires managing, storing, and synchronizing an explicit lookup directory/coordinator state across all nodes. |
| ITEM | Explicit `pin` provided in request or directory tier updated. | Ignores the manual pin entirely and defaults to item/key-level deterministic mapping (e.g., hashing the key directly to determine owner). | Read returns value from the hash-determined owner, refusing to look up or return data from the manually pinned node. | Manual placement control: cannot pin specific items to dedicated physical nodes or geographic locations for regulatory or hardware affinity. |

### replication.py

*Think Distributed Systems* §8.4.4.

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| STATE | A non-owner node is paused while two writes update alpha from one to two | The write succeeds with ACK_ONE, and the current state is replicated rather than the sequence of operations | After the paused node catches up, a read on that node returns two | The system does not need to replicate the full sequence of operations, but it gives up the operation history needed to replay each individual write. |
| LOG | A non-owner node is paused while two writes occur: alpha=one, followed by alpha=two | Each write succeeds with ACK_ONE, and the operations are recorded and replicated in order | After the paused node replays both operations in order, a read on that node returns two | The system preserves operation history, but it may need more storage and time to replay many operations |
| LOG_WITH_SNAPSHOT | A non-owner node remains paused while several writes occur | Writes succeed with ACK_ONE, and the system uses operation logs and can use a snapshot to help the node catch up | After the paused node catches up, a read on that node returns the latest value, matching the other nodes | The system adds complexity by managing snapshots and logs, although snapshots can reduce the amount of history that must be replayed |

### write_completion.py

*Think Distributed Systems* §8.4.3; *Database Internals* ch. 11.

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| ACK_ONE | Pause any one member (e.g., `n2`). | Write still returns `ok=True` immediately since W=1 is met by the receiver. | Read on the paused node (`n2`) may still return empty/old bytes (incomplete write). | Gives up strong consistency and durability in exchange for lowest latency. |
| ACK_QUORUM | Pause fewer than a majority (e.g., 1 of 3 nodes). | Write returns `ok=True` once `N//2 + 1` nodes acknowledge the write. | Read on the paused node may return old bytes until it is repaired. | Gives up perfectly consistent reads on all nodes to tolerate a minority node failure. |
| ACK_ALL | Pause any one member (e.g., `n2`). | Write does not return `ok` (it waits or times out) since W=N cannot be met. | After unpause, a Read on the paused node (`n2`) is guaranteed to have the new value. | Gives up write availability (a single node failure blocks all writes). |

### session_models.py

*Database Internals* ch. 11 (session models).

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| READ_OWN_WRITES | `alpha` is pinned to n0, and `alpha=one` succeeds in session `s1` while n1 is stale or missing the key | n0 stores the bytes and records the Write's sequence number for `s1` and `alpha` | A Read sent to n1 is served through n0 and returns `one`; it fails if n0 is unreachable | Cross-node Reads need an extra RPC and lose availability when the owner is unreachable |
| MONOTONIC_READS | Session `s2` first reads `alpha` on n0 and later sends the same Read to n1 | Writes use the normal path; the first Read records the sequence it observed | The Read sent to n1 is served through n0 and returns the same or a newer sequence; a lower sequence is refused | Reads depend on the node holding the session sequence and may fail during a pause or split |
| NO_SESSION | n1 is behind or does not have `alpha` | The Write uses the normal completion and replication modes; no session sequence is enforced | A Read on n1 may return old bytes, miss the key, or return `not owner` | There is no read-own-writes or monotonic-read guarantee |

### request_routing.py

*Designing Data-Intensive Applications* ch. 7 (request routing). A Write sent to a node that does not own the key. In all rows below, `alpha` is owned by n2 and the Write is sent to n0 or n1.

| Mode | Fault or change | What Write does | What Read returns on which node | What you gave up |
| --- | --- | --- | --- | --- |
| FORWARD | Write `alpha=one` sent to n0, which is not the owner | n0 copies the request with `hop` + 1 and sends it to n2. n2 stores it, and the client sees `ok=True owner=n2` | `one` on n2. A Read sent to n0 with FORWARD is forwarded and returns `one` with `served_by=n2` | An extra hop on every request. If the owner is paused or unreachable the Write fails with `owner unreachable` and stores nothing |
| FORWARD | Two nodes point at each other for key `loop` | Each pass adds 1 to `hop`. After 8 the Write returns `ok=False error='hop limit'` | Nothing was stored (Status shows `key_count=0` on both nodes) | The forwarder trusts its own idea of the owner, so a wrong directory shows up as a loop that only the hop limit stops |
| DIRECTORY_TIER | Write `alpha=two` sent to n0, directory is current | n0 looks up the owner (n2) and sends there. `ok=True owner=n2` | `two` on n2 | Needs a directory that every node keeps up to date. Costs one lookup before the send |
| DIRECTORY_TIER | Stale lookup: n0 thinks `beta` is on n1, but it moved to n2 | n0 sends to n1, n1 answers `not owner` and names n2, n0 retries once to n2. `ok=True owner=n2` | `one` on n2. A Read sent straight to n1 returns `ok=False error='not owner'`, since n1 never stored it | One extra round trip when the lookup is stale. Only one retry, so a second stale answer is returned to the client as the error |
| CLIENT_AWARE | Write `alpha=nope` sent to n1 | Returns `ok=False error='not owner' owner=n2` and does not forward | `alpha` on n2 is unchanged (still the earlier bytes). The refused call stored nothing | The client has to learn the owner and send again, but the servers do no forwarding at all |

## Protocol notes

**request_routing.py (Adil).** What is on the wire is the normal `Write` or `Read` RPC, with the `hop` field of the request. `request_routing.on_write` runs only when the receiving node is not the owner. It does one of three things depending on `--request-routing`.

- FORWARD: build a copy of the request with `hop` + 1 (`copy_with_hop` in `util.py`) and call `node.rpc(owner, "Write", copy)`. Before sending, check `hop`. If it is already 8 or more, return `ok=False error='hop limit'` and do not send. The reply from the owner is passed back to the client as it is. If `node.rpc` raises `RpcFailed`, return `ok=False error='owner unreachable'`. Read works the same way with `"Read"`.
- DIRECTORY_TIER: the owner that `node.py` hands to the plugin comes from this node's own directory, so that is the lookup. Send the Write there with `hop` + 1. If the node that receives it is not the owner, it answers `not owner` and puts the owner it believes in the `owner` field. The first sender then sends the same bytes one more time to that node, and that is the only retry. A node that gets a request whose `hop` is already above 0 does not route again. It answers `not owner` and stops, so two stale directories cannot start a chain of retries.
- CLIENT_AWARE: return `ok=False error='not owner' owner=<owner>` right away. Nothing is forwarded and nothing is stored.

Because `node.rpc` goes through the fault flags, a paused or dropped owner shows up as `RpcFailed` and not as a hang.

## Demo

All steps use the provided client from `week06/`. The commands in steps 1 to 8 were run against a real three node cluster started from a copy of `week06/`, and the outputs shown are what came back. The pin command goes to every node because the partitioning plugin used for the check keeps pins on each node separately. If the group's partitioning plugin shares pins between nodes, the pin lines can become one command.

1. Start three nodes, one terminal each.

   ```
   cd week06
   python3 -m platform.node --node-id n0 --port 9600 --config cluster.json
   python3 -m platform.node --node-id n1 --port 9601 --config cluster.json
   python3 -m platform.node --node-id n2 --port 9602 --config cluster.json
   ```

   Each prints `[nX] listening on 127.0.0.1:960X peers=['n0', 'n1', 'n2']`. Check one with `python3 -m platform.client --addr 127.0.0.1:9600 health`, which prints `ok=True node_id=n0`.

2. Pin `alpha` to n2 on every node, so all three agree that n2 is the owner.

   ```
   python3 -m platform.client --addr 127.0.0.1:9600 pin alpha n2 --partitioning DIRECTORY
   python3 -m platform.client --addr 127.0.0.1:9601 pin alpha n2 --partitioning DIRECTORY
   python3 -m platform.client --addr 127.0.0.1:9602 pin alpha n2 --partitioning DIRECTORY
   ```

   Each prints `ok=True peers=['n0', 'n1', 'n2']`.

3. FORWARD. Write on n0, then Read on the owner and on n0.

   ```
   python3 -m platform.client --addr 127.0.0.1:9600 write alpha one --partitioning DIRECTORY --request-routing FORWARD
   python3 -m platform.client --addr 127.0.0.1:9602 read alpha --partitioning DIRECTORY
   python3 -m platform.client --addr 127.0.0.1:9600 read alpha --partitioning DIRECTORY --request-routing FORWARD
   ```

   Output: `ok=True error='' owner=n2 acked_by=['n2']`, then `ok=True error='' found=True served_by=n2 value=one` twice.

4. CLIENT_AWARE. Write on n1, then check that n2 did not change.

   ```
   python3 -m platform.client --addr 127.0.0.1:9601 write alpha nope --partitioning DIRECTORY --request-routing CLIENT_AWARE
   python3 -m platform.client --addr 127.0.0.1:9602 read alpha --partitioning DIRECTORY
   ```

   Output: `ok=False error='not owner' owner=n2 acked_by=[]`, then `ok=True error='' found=True served_by=n2 value=one`.

5. DIRECTORY_TIER with a current lookup.

   ```
   python3 -m platform.client --addr 127.0.0.1:9600 write alpha two --partitioning DIRECTORY --request-routing DIRECTORY_TIER
   python3 -m platform.client --addr 127.0.0.1:9602 read alpha --partitioning DIRECTORY
   ```

   Output: `ok=True error='' owner=n2 acked_by=['n2']`, then `value=two` on n2.

6. DIRECTORY_TIER with a stale lookup. n0 is told `beta` is on n1, while n1 and n2 know it is on n2.

   ```
   python3 -m platform.client --addr 127.0.0.1:9600 pin beta n1 --partitioning DIRECTORY
   python3 -m platform.client --addr 127.0.0.1:9601 pin beta n2 --partitioning DIRECTORY
   python3 -m platform.client --addr 127.0.0.1:9602 pin beta n2 --partitioning DIRECTORY
   python3 -m platform.client --addr 127.0.0.1:9600 write beta one --partitioning DIRECTORY --request-routing DIRECTORY_TIER
   python3 -m platform.client --addr 127.0.0.1:9602 read beta --partitioning DIRECTORY
   ```

   Output: the Write is `ok=True error='' owner=n2 acked_by=['n2']` even though n0 first asked n1, and the Read on n2 is `value=one`. A Read sent to n1 (`--addr 127.0.0.1:9601 read beta --partitioning DIRECTORY`) gives `ok=False error='not owner' found=False served_by=n1`, because n1 only passed the Write along.

7. Hop limit. n0 says `loop` is on n1, and n1 says it is on n0.

   ```
   python3 -m platform.client --addr 127.0.0.1:9600 pin loop n1 --partitioning DIRECTORY
   python3 -m platform.client --addr 127.0.0.1:9601 pin loop n0 --partitioning DIRECTORY
   python3 -m platform.client --addr 127.0.0.1:9600 write loop x --partitioning DIRECTORY --request-routing FORWARD
   ```

   Output: `ok=False error='hop limit' owner=n1 acked_by=[]`.

8. Owner paused. Pause n2, send a FORWARD Write to n0, unpause, send it again.

   ```
   python3 -m platform.client --addr 127.0.0.1:9602 fault --pause on
   python3 -m platform.client --addr 127.0.0.1:9600 write alpha three --partitioning DIRECTORY --request-routing FORWARD
   python3 -m platform.client --addr 127.0.0.1:9602 fault --pause off
   python3 -m platform.client --addr 127.0.0.1:9600 write alpha three --partitioning DIRECTORY --request-routing FORWARD
   python3 -m platform.client --addr 127.0.0.1:9602 read alpha --partitioning DIRECTORY
   ```

   Output: `ok=True pause=True drop_to=[] delay_ms=0`, then the first Write is `ok=False error='owner unreachable' owner=n2 acked_by=[]`, then `ok=True pause=False ...`, then the second Write is `ok=True error='' owner=n2 acked_by=['n2']`, and the Read is `value=three`.

9. READ_OWN_WRITES across nodes. Pin `alpha` to n0 on every node, replacing the earlier n2 pin, then Write on n0 and Read through n1.

   ```
   python3 -m platform.client --addr 127.0.0.1:9600 pin alpha n0 --partitioning DIRECTORY
   python3 -m platform.client --addr 127.0.0.1:9601 pin alpha n0 --partitioning DIRECTORY
   python3 -m platform.client --addr 127.0.0.1:9602 pin alpha n0 --partitioning DIRECTORY
   python3 -m platform.client --addr 127.0.0.1:9600 write alpha one --partitioning DIRECTORY --session-id s1 --session-models READ_OWN_WRITES
   python3 -m platform.client --addr 127.0.0.1:9601 read alpha --partitioning DIRECTORY --session-id s1 --session-models READ_OWN_WRITES
   ```

   Expected output: the Write succeeds on n0. The Read sent to n1 returns `ok=True found=True served_by=n0 value=one`.

10. MONOTONIC_READS across nodes.

   ```
   python3 -m platform.client --addr 127.0.0.1:9600 read alpha --partitioning DIRECTORY --session-id s2 --session-models MONOTONIC_READS
   python3 -m platform.client --addr 127.0.0.1:9601 read alpha --partitioning DIRECTORY --session-id s2 --session-models MONOTONIC_READS
   ```

   Expected output: both Reads return `one`. The second Read cannot return a sequence lower than the first.

11. NO_SESSION on n1.

   ```
   python3 -m platform.client --addr 127.0.0.1:9601 read alpha --partitioning DIRECTORY --session-models NO_SESSION --request-routing CLIENT_AWARE
   ```

   Expected output: n1 returns `ok=False error='not owner'`. NO_SESSION does not add a redirect or consistency check.

12. Pause the node holding the session sequence.

   ```
   python3 -m platform.client --addr 127.0.0.1:9600 fault --pause on
   python3 -m platform.client --addr 127.0.0.1:9601 read alpha --partitioning DIRECTORY --session-id s1 --session-models READ_OWN_WRITES
   python3 -m platform.client --addr 127.0.0.1:9600 fault --pause off
   ```

   Expected output: the Read returns `ok=False error='session unavailable'` instead of returning stale bytes from n1.

## What we are not building this week

We are not building a full Raft, MVCC, or any application data type, and no web UI, TLS or real database. State lives in memory on the processes. The five plugins nobody in the group owns (leader_replication, conflicts, processing_guarantees, leader_election, atomic_commit) stay as the provided stubs, so the group does not do leader election, conflict merging, exactly once processing or two phase commit this week.

## Git

Each plugin change is a pull request from the named feature branch. The team lead merges. The group repo is the submission.
