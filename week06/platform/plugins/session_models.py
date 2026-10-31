"""Rules for two Reads after a Write on the same session.

In the readings: session models (read-own-writes, monotonic reads) —
Database Internals ch. 11.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed, copy_with_hop``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node
from platform.util import MAX_HOP, RpcFailed, copy_with_hop, hop_count, mode

from .base import PluginBase


class Plugin(PluginBase):
    def on_write(self, node: Node, request: node_pb2.WriteRequest, value: bytes) -> None:
        """Record a per-session sequence (not just the last bytes) for this Write."""
        if not request.session_id:
            return

        with node.lock:
            sequence = None
            for log_sequence, key, log_value in reversed(node.log):
                if log_sequence >= 0 and key == request.key and log_value == value:
                    sequence = int(log_sequence)
                    break
            if sequence is not None:
                writes = node.sessions_write.setdefault(request.session_id, {})
                previous = writes.get(request.key)
                if type(previous) is not int or sequence > previous:
                    writes[request.key] = sequence

    def on_read(
        self, node: Node, request: node_pb2.ReadRequest, owner: str
    ) -> node_pb2.ReadResponse | None:
        """READ_OWN_WRITES may wait or pull from a peer; MONOTONIC_READS refuse a lower seq."""
        session_mode = mode(request.modes, "session_models").upper()

        if session_mode == "NO_SESSION":
            return None
        if session_mode not in ("READ_OWN_WRITES", "MONOTONIC_READS"):
            return None
        if not request.session_id:
            return node_pb2.ReadResponse(
                ok=False,
                error="session id required",
                served_by=node.node_id,
            )

        # Sequence numbers are meaningful only within one node's log. Route to
        # the owner before checking a watermark so unrelated logs are never compared.
        if owner != node.node_id:
            if hop_count(request) >= MAX_HOP:
                return node_pb2.ReadResponse(
                    ok=False,
                    error="hop limit",
                    served_by=node.node_id,
                )
            try:
                return node.rpc(owner, "Read", copy_with_hop(request))
            except RpcFailed:
                return node_pb2.ReadResponse(
                    ok=False,
                    error="session unavailable",
                    served_by=node.node_id,
                )

        with node.lock:
            found = request.key in node.store
            value = node.store.get(request.key, b"")
            version = self._latest_version(node, request.key) if found else None
            sequence = version[0] if version is not None else -1

            if found and (version is None or version[1] != value):
                return node_pb2.ReadResponse(
                    ok=False,
                    error="session unavailable",
                    served_by=node.node_id,
                )

            if session_mode == "READ_OWN_WRITES":
                required = node.sessions_write.get(request.session_id, {}).get(
                    request.key
                )
                if type(required) is int and sequence < required:
                    return node_pb2.ReadResponse(
                        ok=False,
                        error="session unavailable",
                        served_by=node.node_id,
                    )
            else:
                if not hasattr(node, "_session_read_watermarks"):
                    node._session_read_watermarks = {}
                reads = node._session_read_watermarks.setdefault(
                    request.session_id, {}
                )
                required = reads.get(request.key)
                if type(required) is not int:
                    required = node.sessions_read.get(request.session_id, {}).get(
                        request.key
                    )
                if type(required) is int and sequence < required:
                    return node_pb2.ReadResponse(
                        ok=False,
                        error="session unavailable",
                        served_by=node.node_id,
                    )
                reads[request.key] = sequence
                node.sessions_read.setdefault(request.session_id, {})[
                    request.key
                ] = sequence

        return node_pb2.ReadResponse(
            ok=True,
            value=value,
            found=found,
            served_by=node.node_id,
        )

    @staticmethod
    def _latest_version(node: Node, key: bytes) -> tuple[int, bytes] | None:
        for sequence, entry_key, value in reversed(node.log):
            if sequence >= 0 and entry_key == key:
                return int(sequence), value
        return None
