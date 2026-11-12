"""
tests/test_raft_chaos.py
Comprehensive Pytest test suite for Raft Distributed Consensus and Byzantine Chaos Engine.
"""

import pytest
from raft.cluster import RaftCluster
from raft.models import NodeState
from chaos.partition_engine import PartitionEngine
from chaos.linearizability_checker import LinearizabilityChecker


@pytest.fixture
def cluster():
    node_ids = ["node-1", "node-2", "node-3", "node-4", "node-5"]
    return RaftCluster(node_ids)


def test_leader_election(cluster):
    cluster.trigger_elections()
    leader = cluster.get_leader()
    assert leader is not None
    assert leader.state == NodeState.LEADER
    assert leader.current_term >= 1


def test_log_replication_on_majority(cluster):
    cluster.trigger_elections()
    leader = cluster.get_leader()
    assert leader is not None

    idx = leader.propose_command("SET x = 10")
    assert idx == 1
    # Check that majority of nodes committed the entry
    committed_nodes = sum(1 for n in cluster.nodes.values() if len(n.state_machine) >= 1)
    assert committed_nodes >= 3  # Majority quorum for 5 nodes


def test_minority_partition_cannot_commit(cluster):
    cluster.trigger_elections()
    leader = cluster.get_leader()
    assert leader is not None

    chaos = PartitionEngine(cluster)
    # Isolate leader and one peer into minority (2 nodes), rest in majority (3 nodes)
    minority = [leader.node_id, "node-2"]
    majority = [n for n in cluster.node_ids if n not in minority]
    chaos.create_split_partition(minority, majority)

    # Leader in minority attempts to propose command
    initial_applied = len(leader.state_machine)
    leader.propose_command("UNAUTHORIZED WRITE IN MINORITY")

    # Command MUST NOT commit because minority cannot achieve majority quorum!
    assert len(leader.state_machine) == initial_applied


def test_partition_healing_reconvergence(cluster):
    cluster.trigger_elections()
    leader = cluster.get_leader()
    assert leader is not None

    chaos = PartitionEngine(cluster)
    minority = ["node-1", "node-2"]
    majority = ["node-3", "node-4", "node-5"]
    chaos.create_split_partition(minority, majority)

    # Majority elects leader and commits data
    cluster.trigger_elections()
    maj_leader = cluster.get_leader()
    if maj_leader and maj_leader.node_id in majority:
        maj_leader.propose_command("MAJORITY WRITE")

    # Heal network
    chaos.heal_network()
    cluster.broadcast_heartbeats()

    # Verify linearizability
    report = LinearizabilityChecker.verify_cluster_invariants(cluster)
    assert report.is_safe
    assert not report.split_brain_detected
    assert report.divergent_entries_found == 0
