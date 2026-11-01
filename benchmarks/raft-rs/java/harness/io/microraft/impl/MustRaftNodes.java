package io.microraft.impl;

import java.time.Clock;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;

import io.microraft.RaftConfig;
import io.microraft.RaftEndpoint;
import io.microraft.executor.RaftNodeExecutor;
import io.microraft.model.RaftModelFactory;
import io.microraft.model.impl.log.DefaultRaftGroupMembersViewOrBuilder;
import io.microraft.model.log.RaftGroupMembersView;
import io.microraft.persistence.RaftStore;
import io.microraft.report.RaftNodeReportListener;
import io.microraft.statemachine.StateMachine;
import io.microraft.transport.Transport;

/**
 * Harness code (not part of the vendored core), in the core's package only to reach the
 * package-private {@link RaftNodeImpl} constructor. It does what
 * {@code RaftNodeBuilderImpl.build()} does for a fresh group (members = voting members, log index
 * 0), and nothing else. The builder is bypassed because its field initialiser
 * {@code executor = new DefaultRaftNodeExecutor()} runs on every builder construction, whatever
 * executor is set later, and that constructor creates a {@code ThreadGroup} and a
 * {@code ScheduledExecutorService}: no thread is started, but one {@code ThreadGroup} per node per
 * explored execution would stay registered in its parent group for the whole run.
 */
public final class MustRaftNodes {

    private MustRaftNodes() {}

    /** A node of a fresh group whose members are all voting members, as the builder makes it. */
    @SuppressWarnings("checkstyle:parameternumber")
    public static RaftNodeImpl create(Object groupId, RaftEndpoint localEndpoint, List<RaftEndpoint> members,
            RaftConfig config, RaftNodeExecutor executor, StateMachine stateMachine, Transport transport,
            RaftModelFactory modelFactory, RaftStore store, RaftNodeReportListener listener, Random random,
            Clock clock) {
        LinkedHashSet<RaftEndpoint> all = new LinkedHashSet<>(members);
        RaftGroupMembersView view = new DefaultRaftGroupMembersViewOrBuilder().setLogIndex(0).setMembers(all)
                .setVotingMembers(new LinkedHashSet<>(all)).build();
        return new RaftNodeImpl(groupId, localEndpoint, view, config, executor, stateMachine, transport,
                modelFactory, store, listener, random, clock);
    }
}
