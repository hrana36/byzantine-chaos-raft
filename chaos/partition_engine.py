"""
chaos/partition_engine.py
Chaos Engineering & Fault-Injection Engine for Distributed Raft Clusters.
Injects symmetric partitions, asymmetric network cuts, and randomized packet loss.
"""

from __future__ import annotations
import random
from typing import Set, Tuple, List, Optional, Any, Callable
from raft.cluster import RaftCluster


class PartitionEngine:
    """
    Simulates real-world network partitions, cross-AZ cuts, and Byzantine link degradation.
    """

    def __init__(self, cluster: RaftCluster):
        self.cluster = cluster
        self.blocked_pairs: Set[Tuple[str, str]] = set()
        self.packet_loss_rate: float = 0.0

        # Wire up network filter to the virtual cluster
        self.cluster.network_filter = self._filter_network_packet

    def _filter_network_packet(self, src: str, dst: str, payload: Any) -> bool:
        """Determines if a packet should be transmitted or dropped."""
        # Check explicit directional partition
        if (src, dst) in self.blocked_pairs:
            return False

        # Check random link packet loss
        if self.packet_loss_rate > 0.0 and random.random() < self.packet_loss_rate:
            return False

        return True

    def isolate_node(self, node_id: str) -> None:
        """Completely disconnects a single node from all peers (Symmetric Partition)."""
        for peer in self.cluster.node_ids:
            if peer != node_id:
                self.blocked_pairs.add((node_id, peer))
                self.blocked_pairs.add((peer, node_id))

    def create_split_partition(self, group_a: List[str], group_b: List[str]) -> None:
        """
        Creates a symmetric network partition isolating Group A from Group B:
        No traffic can pass between any node in A and any node in B.
        """
        for a in group_a:
            for b in group_b:
                self.blocked_pairs.add((a, b))
                self.blocked_pairs.add((b, a))

    def create_asymmetric_partition(self, src_isolated: str, dst_peers: List[str]) -> None:
        """
        Creates an asymmetric partition where src_isolated can SEND to dst_peers,
        but CANNOT receive replies (or vice versa).
        """
        for dst in dst_peers:
            self.blocked_pairs.add((dst, src_isolated))

    def heal_network(self) -> None:
        """Removes all partitions and packet drop rules, restoring complete network connectivity."""
        self.blocked_pairs.clear()
        self.packet_loss_rate = 0.0
