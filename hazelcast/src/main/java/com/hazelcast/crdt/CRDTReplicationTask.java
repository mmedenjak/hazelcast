/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.crdt;

import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;

import java.util.Collection;
import java.util.Map;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;

/**
 * Task responsible for replicating the CRDT states for all
 * {@link CRDTReplicationAwareService}. This task is a runnable that is
 * meant to be executed by an executor. The task may be interrupted in
 * which case some CRDT states may not be replicated.
 */
class CRDTReplicationTask implements Runnable {
    /** The task name */
    public static final String TASK_NAME = "hz:CRDTReplicationTask";
    private final NodeEngine nodeEngine;
    private final int maxTargets;
    private final ILogger logger;
    private final CRDTReplicationService replicationService;
    private int lastTargetIndex;

    CRDTReplicationTask(NodeEngine nodeEngine, int maxTargets, CRDTReplicationService replicationService) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.maxTargets = maxTargets;
        this.replicationService = replicationService;
    }

    @Override
    public void run() {
        try {
            final Collection<Member> viableTargets = nodeEngine.getClusterService().getMembers(NON_LOCAL_MEMBER_SELECTOR);
            if (viableTargets.size() == 0) {
                return;
            }
            final Member[] targets = pickTargets(viableTargets, lastTargetIndex, maxTargets);
            lastTargetIndex = (lastTargetIndex + targets.length) % viableTargets.size();
            for (CRDTReplicationAwareService service : replicationService.getReplicationServices()) {
                for (Member target : targets) {
                    replicate(service, target);
                }
            }
        } finally {
            // we left the interrupt status unchanged while replicating so we clear it here
            Thread.interrupted();
        }
    }

    /**
     * Performs replication of a {@link CRDTReplicationAwareService} to the
     * given target. The service may optimise the returned operation based on
     * the target member and the previous successful replication operations.
     *
     * @param service the service to replicate
     * @param target  the target to replicate to
     * @see CRDTReplicationAwareService
     */
    private void replicate(CRDTReplicationAwareService service, Member target) {
        if (Thread.currentThread().isInterrupted()) {
            return;
        }
        final Map<String, Long> replicatedCRDTVersions =
                replicationService.getReplicationVersionVector(service.getName(), target.getUuid());

        final OperationService operationService = nodeEngine.getOperationService();
        final CRDTReplicationOperation replicationOperation = service.prepareReplicationOperation(replicatedCRDTVersions);
        if (replicationOperation == null) {
            logger.finest("Skipping replication of " + service.getName() + " for target " + target);
            return;
        }
        try {
            logger.finest("Replicating " + service.getName() + " to " + target);
            operationService.invokeOnTarget(null, replicationOperation.getOperation(), target.getAddress()).join();
            replicationService.setReplicationVersionVector(service.getName(), target.getUuid(),
                    replicationOperation.getVersionVector());
        } catch (Exception e) {
            if (logger.isFineEnabled()) {
                logger.fine("Failed replication of " + service.getName() + " for target " + target, e);
            } else {
                logger.info("Failed replication of " + service.getName() + " for target " + target);
            }
        }
    }

    /**
     * Picks up to {@code maxTargets} from the provided {@code members}
     * collection. The {@code startingIndex} parameter determines which
     * subset of members can be returned. By increasing the parameter by the
     * size of the returned array you can imitate a "sliding window", meaning
     * that each time it is invoked it will rotate through a list of viable
     * targets and return a sub-collection based on the previous method call.
     * A member may be skipped if the collection of viable targets changes
     * between two invocations but if the collection does not change,
     * eventually all targets should be returned by this method.
     *
     * @param members       a collection of members to choose from
     * @param startingIndex the index of the first returned member
     * @param maxTargets    the maximum number of members to return
     * @return the chosen targets
     * @see CRDTReplicationConfig#getMaxConcurrentReplicationTargets()
     */
    private Member[] pickTargets(Collection<Member> members, int startingIndex, int maxTargets) {
        final Member[] viableTargetArray = members.toArray(new Member[0]);
        final Member[] pickedTargets = new Member[Math.min(maxTargets, viableTargetArray.length)];

        for (int i = 0; i < pickedTargets.length; i++) {
            startingIndex = (startingIndex + 1) % viableTargetArray.length;
            pickedTargets[i] = viableTargetArray[startingIndex];
        }
        return pickedTargets;
    }
}
