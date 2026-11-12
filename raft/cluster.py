"""
raft/cluster.py
Distributed Cluster Coordinator and Virtual Transport Network.
Coordinates Raft nodes and integrates with the Chaos Partition Engine.
"""

from __future__ import annotations
from typing import Dict, List, Optional, Any, Callable
from .node import RaftNode
from .models import NodeState, RequestVoteArgs, AppendEntriesArgs


class RaftCluster:
    """
    Simulates a distributed ensemble of Raft consensus nodes interconnected via a virtual network.
    """

    def __init__(self, node_ids: List[str]):
        self.node_ids = list(node_ids)
        self.nodes: Dict[str, RaftNode] = {}
        # Dynamic network filter: filter_fn(src, dst, rpc_payload) -> bool (True = deliver, False = drop)
        self.network_filter: Optional[Callable[[str, str, Any], bool]] = None

        # Initialize nodes
        for nid in self.node_ids:
            self.nodes[nid] = RaftNode(
                node_id=nid,
                peers=self.node_ids,
                transport_sender=self._send_rpc,
            )

    def _send_rpc(self, src: str, dst: str, payload: Any) -> Any:
        """Virtual network packet delivery layer with dynamic chaos partition filtering."""
        if self.network_filter and not self.network_filter(src, dst, payload):
            # Packet dropped by network partition / chaos rule!
            return None

        target_node = self.nodes.get(dst)
        if not target_node:
            return None

        # Deliver to target node's RPC handler
        if isinstance(payload, RequestVoteArgs):
            reply = target_node.handle_request_vote(payload)
        elif isinstance(payload, AppendEntriesArgs):
            reply = target_node.handle_append_entries(payload)
        else:
            reply = None

        # Check return path (reverse direction) for asymmetric partition drop
        if self.network_filter and not self.network_filter(dst, src, reply):
            return None

        return reply

    def get_leader(self) -> Optional[RaftNode]:
        """Finds the current recognized leader in the cluster."""
        leaders = [n for n in self.nodes.values() if n.state == NodeState.LEADER]
        return leaders[0] if len(leaders) == 1 else None

    def get_all_leaders(self) -> List[RaftNode]:
        """Returns all nodes claiming to be leader (used to detect split-brain)."""
        return [n for n in self.nodes.values() if n.state == NodeState.LEADER]

    def trigger_elections(self) -> None:
        """Triggers candidate elections across non-leader nodes."""
        for n in self.nodes.values():
            if n.state != NodeState.LEADER:
                n.start_election()
                if n.state == NodeState.LEADER:
                    break

    def broadcast_heartbeats(self) -> None:
        """Forces all active leaders to broadcast AppendEntries heartbeats."""
        for n in self.nodes.values():
            if n.state == NodeState.LEADER:
                n.broadcast_append_entries()
