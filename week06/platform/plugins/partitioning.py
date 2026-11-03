"""Which node stores a key, and what AddNode / pin do to that map.

In the readings: partitioning / sharding — Think Distributed Systems ch. 7;
Designing Data-Intensive Applications ch. 7.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed``.
"""
from __future__ import annotations

import hashlib

from platform import node_pb2
from platform.node import Node
from platform.util import RpcFailed, mode

from .base import PluginBase


def _stable_hash(*parts: bytes) -> int:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(len(part).to_bytes(8, "big"))
        digest.update(part)
    return int.from_bytes(digest.digest(), "big")


class Plugin(PluginBase):
    def owner(self, node: Node, key: bytes, mode: str) -> str:
        """Which node id should own ``key`` under this partitioning mode.

        HASH_MOD remaps most keys when membership changes; SLOTS moves about
        ``1/(N+1)``; KEY_RANGE keeps neighbors together; DIRECTORY honors pin;
        ITEM ignores pin. ``N`` is ``len(node.nodes)``.
        """
        nodes = sorted(node.nodes, key=lambda node_id: (len(node_id), node_id))
        if not nodes:
            return node.node_id

        partitioning_mode = (mode or "HASH_MOD").upper()

        if partitioning_mode == "DIRECTORY":
            pinned_node = node.directory.get(key)
            if pinned_node in nodes:
                return pinned_node
            partitioning_mode = "HASH_MOD"

        if partitioning_mode == "HASH_MOD":
            return nodes[_stable_hash(key) % len(nodes)]

        if partitioning_mode == "SLOTS":
            if node.n_slots <= 0:
                raise ValueError("node.n_slots must be positive")
            slot = _stable_hash(key) % node.n_slots
            slot_bytes = slot.to_bytes(8, "big")
            return max(
                nodes,
                key=lambda node_id: _stable_hash(
                    slot_bytes, node_id.encode("utf-8")
                ),
            )

        if partitioning_mode == "KEY_RANGE":
            prefix = int.from_bytes(key[:2].ljust(2, b"\x00"), "big")
            range_index = prefix * len(nodes) // (1 << 16)
            return nodes[range_index]

        if partitioning_mode == "ITEM":
            return max(
                nodes,
                key=lambda node_id: _stable_hash(key, node_id.encode("utf-8")),
            )

        raise ValueError(f"unknown partitioning mode: {mode!r}")

    def on_add_node(self, node: Node, request: node_pb2.AddNodeRequest) -> node_pb2.StatusResponse:
        """Handle ``AddNode`` / pin. Return a StatusResponse. Move bytes that changed owner."""
        partitioning_mode = (mode(request.modes, "partitioning")
            or node.last_partitioning
            or "HASH_MOD"
        ).upper()

        if request.pin_key:
            with node.lock:
                valid_target = request.node_id in node.nodes
                if valid_target and partitioning_mode != "ITEM":
                    node.directory[request.pin_key] = request.node_id
                    node.last_partitioning = partitioning_mode

            response = node.status_now()
            if not valid_target or partitioning_mode == "ITEM":
                response.ok = False
            return response

        if not request.node_id:
            response = node.status_now()
            response.ok = False
            return response

        with node.lock:
            if request.node_id not in node.nodes:
                node.nodes.append(request.node_id)
                node.nodes.sort(key=lambda node_id: (len(node_id), node_id))
            if request.host:
                node.peers[request.node_id] = (request.host, int(request.port or 0))
            node.last_partitioning = partitioning_mode

        return node.status_now()

    def rebalance(self, node: Node, modes: node_pb2.Modes) -> None:
        """Send keys this node holds to their current owner if that owner is someone else."""
        partitioning_mode = (
            mode(modes, "partitioning") or node.last_partitioning or "HASH_MOD"
        ).upper()
        effective_modes = node_pb2.Modes()
        if modes is not None:
            effective_modes.CopyFrom(modes)
        effective_modes.partitioning = partitioning_mode
        with node.lock:
            node.last_partitioning = partitioning_mode
            items = list(node.store.items())

        for key, value in items:
            owner = self.owner(node, key, partitioning_mode)
            if owner == node.node_id:
                continue

            if (
                partitioning_mode == "DIRECTORY"
                and node.directory.get(key) == owner
            ):
                pin = node_pb2.AddNodeRequest(
                    node_id=owner, pin_key=key, modes=effective_modes
                )
                for peer in node.nodes:
                    if peer == node.node_id:
                        continue
                    response = node.rpc(peer, "AddNode", pin)
                    if not response.ok:
                        raise RpcFailed(
                            f"could not sync directory entry for key {key!r} "
                            f"to {peer}"
                        )

            request = node_pb2.WriteRequest(
                key=key, value=value, modes=effective_modes
            )
            response = node.rpc(owner, "Write", request)
            if not response.ok:
                raise RpcFailed(
                    f"could not move key {key!r} to {owner}: "
                    f"{response.error or 'write rejected'}"
                )

            with node.lock:
                if node.store.get(key) == value:
                    del node.store[key]
