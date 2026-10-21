"""Write/Read that arrived on a node that does not own the key.

In the readings: request routing — Designing Data-Intensive Applications ch. 7.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed, copy_with_hop``.
"""
from __future__ import annotations

from platform import node_pb2
from platform.node import Node
from platform.util import MAX_HOP, RpcFailed, copy_with_hop, hop_count, mode

from .base import PluginBase


class Plugin(PluginBase):
    def on_write(
        self, node: Node, request: node_pb2.WriteRequest, owner: str
    ) -> node_pb2.WriteResponse | None:
        """If this node should not store the Write, forward, look up, or reject.

        Return a WriteResponse, or None to mean “handle it here.” Increment
        ``request.hop`` when forwarding; stop after 8. A stale directory lookup
        should retry once rather than drop the bytes.
        """
        m = mode(request.modes, "request_routing")

        if m == "FORWARD":
            # send it on to the owner, the owner's answer goes back to the client
            return self._send_write(node, request, owner)

        if m == "DIRECTORY_TIER":
            # a request that was already sent on once is not routed again
            if hop_count(request) > 0:
                return node_pb2.WriteResponse(ok=False, error="not owner", owner=owner)
            first = self._send_write(node, request, owner)
            # retry only when the node we asked said "not owner" and named someone else
            if first.ok or first.error != "not owner":
                return first
            if first.owner in ("", owner, node.node_id):
                return first
            return self._send_write(node, request, first.owner)  # the one retry

        if m == "CLIENT_AWARE":
            # no forwarding and nothing stored, the client has to ask the owner
            return node_pb2.WriteResponse(ok=False, error="not owner", owner=owner)

        # no routing mode: None, so the node answers "not owner" by default
        return None

    def on_read(
        self, node: Node, request: node_pb2.ReadRequest, owner: str
    ) -> node_pb2.ReadResponse | None:
        """Same idea for Read. Return a ReadResponse or None."""
        m = mode(request.modes, "request_routing")

        # this node owns the key, so it answers itself
        if owner == node.node_id:
            return None

        if m == "FORWARD":
            return self._send_read(node, request, owner)

        if m == "DIRECTORY_TIER":
            # ReadResponse has no owner field, so a Read gets no retry
            if hop_count(request) > 0:
                return node_pb2.ReadResponse(ok=False, error="not owner", served_by=node.node_id)
            return self._send_read(node, request, owner)

        if m == "CLIENT_AWARE":
            return node_pb2.ReadResponse(ok=False, error="not owner", served_by=node.node_id)

        # no routing mode: same as the node default, "not owner" unless the bytes are here
        if request.key not in node.store:
            return node_pb2.ReadResponse(ok=False, error="not owner", served_by=node.node_id)
        return None

    def _send_write(self, node, request, target):
        # stop at 8 forwards instead of looping
        if hop_count(request) >= MAX_HOP:
            return node_pb2.WriteResponse(ok=False, error="hop limit", owner=target)
        try:
            return node.rpc(target, "Write", copy_with_hop(request))
        except RpcFailed:
            return node_pb2.WriteResponse(ok=False, error="owner unreachable", owner=target)

    def _send_read(self, node, request, target):
        if hop_count(request) >= MAX_HOP:
            return node_pb2.ReadResponse(ok=False, error="hop limit", served_by=node.node_id)
        try:
            return node.rpc(target, "Read", copy_with_hop(request))
        except RpcFailed:
            return node_pb2.ReadResponse(ok=False, error="owner unreachable", served_by=node.node_id)
