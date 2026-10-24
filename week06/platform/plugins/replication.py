"""What a copy on the wire looks like, how a node applies it, and how a lagging node catches up.

In the readings: state-based vs log-based replication — Think Distributed
Systems §8.4.4.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed, encode_store, decode_store``.
"""
from __future__ import annotations
from platform import node_pb2
from platform.node import Node
from platform.util import mode, RpcFailed, encode_store, decode_store
from .base import PluginBase

# send a snapshot when a peer is more than five operations behind
SNAPSHOT_LAG = 5

class Plugin(PluginBase):
    def install(
        self, node: Node, key: bytes, value: bytes, modes: node_pb2.Modes
    ) -> node_pb2.ReplicateRequest:
        """Apply a Write to local store/log and return the ReplicateRequest to send."""
        replication_mode = (mode(modes, "replication") or "LOG").upper()

        with node.lock:
            index = len(node.log)
            node.log.append((index, key, value))
            node.store[key] = value
            node.last_applied = max(node.last_applied, index)

            request = node_pb2.ReplicateRequest(
                key=key,
                value=value,
                index=index,
                source=node.node_id,
                modes=modes,
            )

            if replication_mode == "STATE":
                request.kind = "STATE"
                request.snapshot_blob = encode_store(node.store)
            else:
                request.kind = "LOG"

            return request

    def on_replicate(
        self, node: Node, request: node_pb2.ReplicateRequest
    ) -> node_pb2.ReplicateResponse:
        """Apply an incoming ReplicateRequest. Return ReplicateResponse."""
        kind = (request.kind or "").upper()
        index = int(request.index)

        with node.lock:
            if kind in ("STATE", "SNAPSHOT"):
                # dont let an older snapshot overwrite newer data
                if index < node.last_applied:
                    return node_pb2.ReplicateResponse(
                        ok=True, applied_index=node.last_applied
                    )

                snapshot = decode_store(request.snapshot_blob)
                node.store.clear()
                node.store.update(snapshot)
                node.last_applied = index

                # preserve the correct next log index. The snapshot replaces
                # the old history, so these earlier entries are placeholders
                node.log = [
                    (-1, b"", b"") for _ in range(max(0, index + 1))
                ]

                return node_pb2.ReplicateResponse(
                    ok=True, applied_index=node.last_applied
                )

            if kind != "LOG":
                return node_pb2.ReplicateResponse(
                    ok=False,
                    error=f"unknown replication kind: {kind or '(empty)'}",
                    applied_index=node.last_applied,
                )

            # repeated delivery of an already-applied operation is safe
            if index <= node.last_applied:
                return node_pb2.ReplicateResponse(
                    ok=True, applied_index=node.last_applied
                )

            # operations need to be applied in order
            if index != node.last_applied + 1:
                return node_pb2.ReplicateResponse(
                    ok=False,
                    error=f"log gap: expected {node.last_applied + 1}, got {index}",
                    applied_index=node.last_applied,
                )

            while len(node.log) < index:
                node.log.append((-1, b"", b""))

            entry = (index, request.key, request.value)
            if len(node.log) == index:
                node.log.append(entry)
            else:
                node.log[index] = entry

            node.store[request.key] = request.value
            node.last_applied = index

            return node_pb2.ReplicateResponse(
                ok=True, applied_index=node.last_applied
            )

    def catch_up(self, node: Node, modes: node_pb2.Modes) -> None:
        """Push a log suffix, or a snapshot if a peer is far behind, using a per-peer index."""
        replication_mode = (mode(modes, "replication") or "LOG").upper()

        with node.lock:
            peers = list(node.nodes)
            local_log = list(node.log)
            local_store = dict(node.store)
            last_index = node.last_applied

            # track confirmed progress separately for each peer
            if not hasattr(node, "_replication_peer_index"):
                node._replication_peer_index = {}
            peer_indexes = node._replication_peer_index

        # ignore placeholder entries created when applying a snapshot
        entries = sorted(
            (entry for entry in local_log if entry[0] >= 0),
            key=lambda entry: entry[0],
        )
        earliest_index = entries[0][0] if entries else last_index + 1

        for peer in peers:
            if peer == node.node_id:
                continue

            with node.lock:
                peer_index = int(peer_indexes.get(peer, -1))

            try:
                if replication_mode == "STATE":
                    request = node_pb2.ReplicateRequest(
                        kind="STATE",
                        index=last_index,
                        snapshot_blob=encode_store(local_store),
                        source=node.node_id,
                        modes=modes,
                    )
                    response = node.rpc(peer, "Replicate", request)

                    if response.ok:
                        with node.lock:
                            peer_indexes[peer] = min(
                                int(response.applied_index), last_index
                            )
                    continue

                missing_history = peer_index < earliest_index - 1
                far_behind = last_index - peer_index > SNAPSHOT_LAG

                if (
                    replication_mode == "LOG_WITH_SNAPSHOT"
                    and last_index >= 0
                    and (far_behind or missing_history)
                ):
                    request = node_pb2.ReplicateRequest(
                        kind="SNAPSHOT",
                        index=last_index,
                        snapshot_blob=encode_store(local_store),
                        source=node.node_id,
                        modes=modes,
                    )
                    response = node.rpc(peer, "Replicate", request)

                    if not response.ok:
                        continue

                    peer_index = min(
                        int(response.applied_index), last_index
                    )
                    with node.lock:
                        peer_indexes[peer] = peer_index

                # send missing log entries from oldest to newest
                for index, key, value in entries:
                    if index <= peer_index:
                        continue

                    request = node_pb2.ReplicateRequest(
                        key=key,
                        value=value,
                        kind="LOG",
                        index=index,
                        source=node.node_id,
                        modes=modes,
                    )
                    response = node.rpc(peer, "Replicate", request)

                    if not response.ok:
                        # leave progress at the last confirmed operation
                        break

                    acknowledged = min(
                        int(response.applied_index), last_index
                    )
                    with node.lock:
                        peer_indexes[peer] = acknowledged
                    peer_index = acknowledged

            except RpcFailed:
                # try again on a later write
                continue
