"""
raft/models.py
Data models and RPC payloads for the Raft Distributed Consensus Protocol.
Conforms to Ongaro & Ousterhout (USENIX ATC 2014) Raft specification.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Any, Optional
import time


class NodeState(str, Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


@dataclass
class LogEntry:
    """Represents a state machine transition entry in the replicated log."""
    index: int
    term: int
    command: Any
    committed: bool = False
    timestamp: float = field(default_factory=time.time)


@dataclass
class RequestVoteArgs:
    """Invoked by candidates to gather votes during leader election."""
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteReply:
    term: int
    vote_granted: bool


@dataclass
class AppendEntriesArgs:
    """Invoked by leader to replicate log entries and serve as heartbeats."""
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry] = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    match_index: int = 0
