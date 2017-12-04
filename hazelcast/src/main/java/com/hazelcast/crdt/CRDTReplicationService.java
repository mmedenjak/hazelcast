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
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;

/**
 * Service that handles replication of CRDT data for all CRDT implementations.
 * The CRDT implementations for which it performs replication must implement
 * {@link CRDTReplicationAwareService}.
 * <p>
 * The replication is performed periodically on an executor with the name
 * {@value com.hazelcast.crdt.CRDTReplicationTask#TASK_NAME}. You may
 * configure this executor accordingly.
 *
 * @see CRDTReplicationConfig#getReplicationPeriodMillis()
 * @see CRDTReplicationConfig#getMaxConcurrentReplicationTargets()
 */
public class CRDTReplicationService implements ManagedService {
    /** The name of this service */
    public static final String SERVICE_NAME = "hz:impl:CRDTReplicationService";
    private ScheduledFuture<?> replicationTask;
    private volatile Exception initializationException;
    private NodeEngine nodeEngine;
    private int maxTargets;
    private ILogger logger;
    private CRDTReplicationVersionVectors replicationVersionVectors;


    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        final CRDTReplicationConfig replicationConfig = nodeEngine.getConfig().getCRDTReplicationConfig();

        final int replicationPeriod = replicationConfig != null
                ? replicationConfig.getReplicationPeriodMillis()
                : CRDTReplicationConfig.DEFAULT_REPLICATION_PERIOD_MILLIS;
        this.maxTargets = replicationConfig != null
                ? replicationConfig.getMaxConcurrentReplicationTargets()
                : CRDTReplicationConfig.DEFAULT_MAX_CONCURRENT_REPLICATION_TARGETS;
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.replicationVersionVectors = new CRDTReplicationVersionVectors();

        this.replicationTask = nodeEngine.getExecutionService().scheduleWithRepetition(
                CRDTReplicationTask.TASK_NAME, new CRDTReplicationTask(nodeEngine, maxTargets, this),
                replicationPeriod, replicationPeriod, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns the exception that occurred on initialization of this service
     * or {@code null} if the service has not yet been initialized or there
     * was no exception.
     */
    public Exception getInitializationException() {
        return initializationException;
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        ScheduledFuture<?> task = replicationTask;
        if (task != null) {
            replicationTask = null;
            task.cancel(terminate);
        }
    }


    /**
     * Attempts to replicate only the unreplicated CRDT state to any non-local
     * member in the cluster. The state may be unreplicated because the CRDT
     * state has been changed (via mutation or merge with an another CRDT) but
     * has not yet been disseminated through the usual replication mechanism
     * to any member.
     * This method will iterate through the member list and try and replicate
     * to at least one member. The method returns once all of the unreplicated
     * state has been replicated successfully or when there are no more members
     * to attempt processing.
     *
     * @see CRDTReplicationTask
     */
    public void syncReplicateDirtyCRDTs() {
        for (CRDTReplicationAwareService service : getReplicationServices()) {
            final CRDTReplicationOperation replicationOperation = service.prepareReplicationOperation(
                    replicationVersionVectors.getLatestVersionVector(service.getName()));
            if (replicationOperation == null) {
                logger.fine("Skipping replication since all CRDTs are replicated");
                continue;
            }
            if (!tryProcessOnOtherMembers(replicationOperation.getOperation(), service.getName())) {
                logger.warning("Failed replication of CRDTs for " + service.getName() + ". CRDT state may be lost.");
            }
        }
    }

    /**
     * Attempts to process the {@code operation} on at least one non-local
     * member. The method will iterate through the member list and try once on
     * each member.
     * The method returns as soon as the first member successfully processes
     * the operation or once there are no more members to try.
     *
     * @param serviceName the service name
     * @return {@code true} if at least one member successfully processed the
     * operation, {@code false} otherwise.
     */
    private boolean tryProcessOnOtherMembers(Operation operation, String serviceName) {
        final OperationService operationService = nodeEngine.getOperationService();
        final Collection<Member> targets = nodeEngine.getClusterService().getMembers(NON_LOCAL_MEMBER_SELECTOR);
        for (Member target : targets) {
            try {
                logger.fine("Replicating " + serviceName + " to " + target);
                operationService.createInvocationBuilder(null, operation, target.getAddress())
                                .setTryCount(1)
                                .invoke().join();
                return true;
            } catch (Exception e) {
                logger.fine("Failed replication of " + serviceName + " for target " + target, e);
            }
        }
        return false;
    }

    /** Returns a collection of all known CRDT replication aware services */
    Collection<CRDTReplicationAwareService> getReplicationServices() {
        return nodeEngine.getServices(CRDTReplicationAwareService.class);
    }

    /**
     * Returns the version vector for the given {@code serviceName} and
     * {@code memberUUID}.
     * The version vector is a map from CRDT name to the last successfully
     * replicated CRDT state version. All CRDTs in this map should be of the
     * same type.
     * If there is no version vector for the given parameters, this method
     * returns an empty map.
     *
     * @param serviceName the service name
     * @param memberUUID  the target member UUID
     * @return the last successfully replicated CRDT state version vector or
     * an empty map if the CRDTs have not yet been replicated to this member
     * @see CRDTReplicationAwareService
     */
    Map<String, Long> getReplicationVersionVector(String serviceName, String memberUUID) {
        return replicationVersionVectors.getReplicationVersionVector(serviceName, memberUUID);
    }

    /**
     * Sets the version vector for the given {@code serviceName} and
     * {@code memberUUID}.
     * The version vector is a map from CRDT name to the last successfully
     * replicated CRDT state version. All CRDTs in this map should be of the
     * same type.
     *
     * @param serviceName   the service name
     * @param memberUUID    the target member UUID
     * @param versionVector the version vector to set
     * @see CRDTReplicationAwareService
     */
    void setReplicationVersionVector(String serviceName, String memberUUID, Map<String, Long> versionVector) {
        replicationVersionVectors.setReplicationVersionVector(serviceName, memberUUID, versionVector);
    }
}
