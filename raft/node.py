"""
raft/node.py
Raft Consensus State Machine implementation.
Handles Leader Election, Log Replication, Heartbeats, and Commit Safety.
"""

from __future__ import annotations
from typing import Dict, List, Optional, Any, Callable
import time
import random
from .models import (
    NodeState,
    LogEntry,
    RequestVoteArgs,
    RequestVoteReply,
    AppendEntriesArgs,
    AppendEntriesReply,
)


class RaftNode:
    """
    Individual distributed node executing the Raft consensus protocol.
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        transport_sender: Optional[Callable[[str, str, Any], Any]] = None,
    ):
        self.node_id = node_id
        self.peers = [p for p in peers if p != node_id]
        self.transport_sender = transport_sender

        # Persistent state
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = [LogEntry(index=0, term=0, command=None, committed=True)]

        # Volatile state on all servers
        self.state = NodeState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        self.last_heartbeat_time = time.time()
        self.election_timeout = random.uniform(0.15, 0.30)  # 150 - 300 ms

        # Volatile state on leaders
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Applied state machine storage
        self.state_machine: List[Any] = []

    @property
    def last_log_index(self) -> int:
        return self.log[-1].index

    @property
    def last_log_term(self) -> int:
        return self.log[-1].term

    def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        """RPC Handler: RequestVote."""
        if args.term > self.current_term:
            self.become_follower(args.term)

        # Rule 1: Reply false if term < currentTerm
        if args.term < self.current_term:
            return RequestVoteReply(term=self.current_term, vote_granted=False)

        # Rule 2: If votedFor is null or candidateId, and candidate's log is up-to-date
        log_ok = (
            args.last_log_term > self.last_log_term or
            (args.last_log_term == self.last_log_term and args.last_log_index >= self.last_log_index)
        )

        can_vote = (self.voted_for is None or self.voted_for == args.candidate_id) and log_ok

        if can_vote:
            self.voted_for = args.candidate_id
            self.last_heartbeat_time = time.time()
            return RequestVoteReply(term=self.current_term, vote_granted=True)

        return RequestVoteReply(term=self.current_term, vote_granted=False)

    def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        """RPC Handler: AppendEntries (Log Replication & Heartbeat)."""
        if args.term > self.current_term:
            self.become_follower(args.term)

        # Rule 1: Reply false if term < currentTerm
        if args.term < self.current_term:
            return AppendEntriesReply(term=self.current_term, success=False)

        # Valid heartbeat received from legitimate leader
        self.last_heartbeat_time = time.time()
        if self.state == NodeState.CANDIDATE:
            self.become_follower(args.term)

        # Rule 2: Reply false if log doesn't contain entry at prevLogIndex matching prevLogTerm
        if len(self.log) <= args.prev_log_index or self.log[args.prev_log_index].term != args.prev_log_term:
            return AppendEntriesReply(term=self.current_term, success=False, match_index=len(self.log) - 1)

        # Rule 3 & 4: Append new entries not already in log
        insert_idx = args.prev_log_index + 1
        for entry in args.entries:
            if insert_idx < len(self.log):
                if self.log[insert_idx].term != entry.term:
                    self.log = self.log[:insert_idx]
                    self.log.append(entry)
            else:
                self.log.append(entry)
            insert_idx += 1

        # Rule 5: Update commitIndex
        if args.leader_commit > self.commit_index:
            self.commit_index = min(args.leader_commit, self.last_log_index)
            self._apply_entries()

        return AppendEntriesReply(term=self.current_term, success=True, match_index=self.last_log_index)

    def start_election(self) -> None:
        """Transitions to Candidate state and broadcasts RequestVote RPCs."""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat_time = time.time()
        self.election_timeout = random.uniform(0.15, 0.30)

        votes = 1  # Voted for self
        majority = (len(self.peers) + 1) // 2 + 1

        args = RequestVoteArgs(
            term=self.current_term,
            candidate_id=self.node_id,
            last_log_index=self.last_log_index,
            last_log_term=self.last_log_term,
        )

        for peer in self.peers:
            if self.transport_sender:
                reply: Optional[RequestVoteReply] = self.transport_sender(self.node_id, peer, args)
                if reply:
                    if reply.term > self.current_term:
                        self.become_follower(reply.term)
                        return
                    if reply.vote_granted and self.state == NodeState.CANDIDATE:
                        votes += 1
                        if votes >= majority:
                            self.become_leader()
                            return

    def become_leader(self) -> None:
        """Transitions to Leader state and initializes tracking indices."""
        self.state = NodeState.LEADER
        for peer in self.peers:
            self.next_index[peer] = self.last_log_index + 1
            self.match_index[peer] = 0
        self.broadcast_append_entries()

    def become_follower(self, term: int) -> None:
        """Demotes node to Follower state."""
        self.state = NodeState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.last_heartbeat_time = time.time()

    def propose_command(self, command: Any) -> Optional[int]:
        """Submits a new command to the replicated log. Only valid on Leader."""
        if self.state != NodeState.LEADER:
            return None

        entry = LogEntry(
            index=self.last_log_index + 1,
            term=self.current_term,
            command=command,
            committed=False,
        )
        self.log.append(entry)
        self.broadcast_append_entries()
        return entry.index

    def broadcast_append_entries(self) -> None:
        """Dispatches AppendEntries RPCs to all peers."""
        if self.state != NodeState.LEADER:
            return

        for peer in self.peers:
            prev_idx = self.next_index.get(peer, 1) - 1
            prev_term = self.log[prev_idx].term if prev_idx < len(self.log) else 0
            entries_to_send = self.log[prev_idx + 1 :]

            args = AppendEntriesArgs(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_idx,
                prev_log_term=prev_term,
                entries=entries_to_send,
                leader_commit=self.commit_index,
            )

            if self.transport_sender:
                reply: Optional[AppendEntriesReply] = self.transport_sender(self.node_id, peer, args)
                if reply:
                    if reply.term > self.current_term:
                        self.become_follower(reply.term)
                        return
                    if reply.success:
                        self.next_index[peer] = reply.match_index + 1
                        self.match_index[peer] = reply.match_index
                        self._check_commit_majority()
                    else:
                        self.next_index[peer] = max(1, self.next_index.get(peer, 1) - 1)

    def _check_commit_majority(self) -> None:
        """Checks if a log entry has been replicated on a majority of servers."""
        if self.state != NodeState.LEADER:
            return

        for idx in range(self.commit_index + 1, len(self.log)):
            if self.log[idx].term == self.current_term:
                replicated_count = 1  # Count self
                for peer in self.peers:
                    if self.match_index.get(peer, 0) >= idx:
                        replicated_count += 1

                majority = (len(self.peers) + 1) // 2 + 1
                if replicated_count >= majority:
                    self.commit_index = idx
                    self._apply_entries()

    def _apply_entries(self) -> None:
        """Applies committed entries to the local state machine."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            entry.committed = True
            if entry.command is not None:
                self.state_machine.append(entry.command)
