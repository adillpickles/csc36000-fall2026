"""When Write returns: send copies, wait for W, time out if a node is paused.

In the readings: synchronous / asynchronous / hybrid (quorum) replication —
Think Distributed Systems §8.4.3; Database Internals ch. 11.

See week06/README.md. Useful imports: ``from platform import node_pb2``
and ``from platform.util import mode, RpcFailed``.
"""
from __future__ import annotations

import concurrent.futures
import threading

from platform import node_pb2
from platform.node import Node
from platform.util import mode, RpcFailed

from .base import PluginBase


class Plugin(PluginBase):
    def __init__(self):
        super().__init__()
        # thread safe to remember failed writes for handoff
        self.pending_hints = []
        self.lock = threading.Lock()

    def spread(
        self,
        node: Node,
        key: bytes,
        value: bytes,
        owner: str,
        modes: node_pb2.Modes,
    ) -> node_pb2.WriteResponse:
       
        """ Install locally (via the replication plugin), send copies, wait for W acks.W comes from the current membership ``len(node.nodes)``: 1, majority
        (``N//2 + 1``), or all N. Return a WriteResponse: ok if W members acked, otherwise ``ack timeout``. Remember failed copies so ``repair`` / ``on_read``
        can finish an incomplete write after the peer is reachable again."""
        
        # determine cluster size 
        N = len(node.nodes)
        completion_mode = mode(modes, "write_completion")
        
        if completion_mode == "ACK_ONE": # mark as done when one node saves the data 
            W = 1
        elif completion_mode == "ACK_QUORUM": #  mark as done when majority of nodes save data 
            W = (N // 2) + 1
        elif completion_mode == "ACK_ALL":  # mark as done when all nodessaves the data
            W = N
        else:
            W = 1  # default to one 

        # Compute first ack 
        node.store[key] = value 
        acks = 1 

        peers=[] # intilze empty server list 

        # loop through every node in cluster excluding current node 
        curr_node=node.node_id
        for n in node.nodes:
            if n!=curr_node:
                peers.append(n)

        # instlize empty list to store offline servers(nodes) as to retry later in repair function 
        offline_nodes=[]
        
    # send data to all peers simultaneously using threads so not stuck waiting for failed nod 
        if len(peers) > 0:
            # assign threads for each peer 
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(peers)) as executor: # each peer is a thread 
                # create write request 
                req=node_pb2.WriteRequest(key=key, value=value, owner=owner)

                thread_to_peer = {}
                for p in peers: 
                    thread=executor.submit(node.rpc, p, "Write", req)
                    thread_to_peer[thread] = p
                
                #check if each thread received message 
                for thread in concurrent.futures.as_completed(thread_to_peer):
                    p=thread_to_peer[thread]

                    try: 
                        resp=thread.result(timeout=5.0)
                        if resp!=None:
                            acks=acks+1
                            
                    except Exception:
                        offline_nodes.append(p)
                        
        if len(offline_nodes) > 0:
            with self.lock:
                for p in offline_nodes:
                    self.pending_hints.append((p, key, value, owner))
                        
         # check if required W acks
        if acks >= W:
            return node_pb2.WriteResponse(ok=True)
        else:
            return node_pb2.WriteResponse(ok=False, error="ack timeout")


    def repair(self, node: Node, modes: node_pb2.Modes) -> None:
        """Hinted handoff: retry copies that timed out while a peer was paused."""
        
        with self.lock:
            # Check if list is empty (no nodes failed) 
            if len(self.pending_hints) == 0:
                return 
            
            # copy list
            hints_to_retry = []
            for hint in self.pending_hints:
                hints_to_retry.append(hint)
                
            #delet old list
            self.pending_hints = []

        failed_again = [] 
        
        # try to resend to nodes that were offline 
        for peer, key, value, owner in hints_to_retry:
            try:
                req = node_pb2.WriteRequest(key=key, value=value, owner=owner)
                resp = node.rpc(peer, "Write", req) 
                
                if resp==None:
                    failed_again.append((peer, key, value, owner)) # if no response add to failed again list
            except Exception:
                # Still unreachable, put it back in the queue
                failed_again.append((peer, key, value, owner))
                
        # Save ones that failed again for the next repair cycle
        if failed_again:
            with self.lock:
                self.pending_hints.extend(failed_again)


    def on_read(
        self, node: Node, request: node_pb2.ReadRequest, owner: str
    ) -> node_pb2.ReadResponse | None:
        """Read repair: if this node is missing ``key``, pull from a peer and store it."""
        
        # if node already has a key (write completed successfully)
        if request.key in node.store:
            return node_pb2.ReadResponse(value=node.store[request.key], ok=True)

        # 2. if key is missing (incomplete write) ask other nodes for the data
        curr_node = node.node_id
            if n != curr_node:
                try: 
                    # ask peer if they have file
                    resp = node.rpc(n, "Read", request)
                    
                    # if successful save and pass it over
                    if resp != None:
                        node.store[request.key] = resp.value
                        return node_pb2.ReadResponse(value=resp.value, ok=True)
                except Exception:
                    # if peer is offline move to next one
                    continue

        return None # no node in the cluster actually saved the data

        
        #
