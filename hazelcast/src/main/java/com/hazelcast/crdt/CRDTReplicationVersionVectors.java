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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains the version vectors for tracking updates to all CRDTs. The
 * updates can either be a mutation invoked by the user or a merge with a
 * CRDT received by a replication operation.
 * Each version vector is a map from CRDT name to the last successful
 * replicated CRDT state version. Each CRDT service and member in the
 * cluster has a version vector with which we know the last successfully
 * replicated CRDT state to any member at any point in time.
 *
 * @see CRDTReplicationAwareService
 */
class CRDTReplicationVersionVectors {
    /**
     * Map from composite key (CRDT service name and target member UUID) to a
     * version vector. The version vector is a map from CRDT name to the last
     * successfully replicated CRDT state version (to the target member
     * contained in the composite key).
     */
    private ConcurrentMap<ReplicationVersionVectorIdentifier, Map<String, Long>> versionVectors
            = new ConcurrentHashMap<ReplicationVersionVectorIdentifier, Map<String, Long>>();

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
        final Map<String, Long> versions = versionVectors.get(new ReplicationVersionVectorIdentifier(serviceName, memberUUID));
        return versions != null ? versions : Collections.<String, Long>emptyMap();
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
        versionVectors.put(new ReplicationVersionVectorIdentifier(serviceName, memberUUID),
                Collections.unmodifiableMap(versionVector));
    }

    /**
     * Returns the version vector for the given {@code serviceName}. The
     * vector will contain the latest successfully replicated CRDT state
     * versions that any member has observed.
     *
     * @param serviceName the CRDT service name
     * @return the version vector for all members
     * @see CRDTReplicationAwareService
     */
    Map<String, Long> getLatestVersionVector(String serviceName) {
        final HashMap<String, Long> latestVersionVector = new HashMap<String, Long>();
        for (Entry<ReplicationVersionVectorIdentifier, Map<String, Long>> versionVectorEntry : versionVectors.entrySet()) {
            if (versionVectorEntry.getKey().serviceName.equals(serviceName)) {
                final Map<String, Long> versionVector = versionVectorEntry.getValue();
                for (Entry<String, Long> crdtReplicatedVersion : versionVector.entrySet()) {
                    final String crdtName = crdtReplicatedVersion.getKey();
                    final Long crdtVersion = crdtReplicatedVersion.getValue();
                    if (!latestVersionVector.containsKey(crdtName) || latestVersionVector.get(crdtName) < crdtVersion) {
                        latestVersionVector.put(crdtName, crdtVersion);
                    }
                }
            }
        }
        return latestVersionVector;
    }

    /**
     * An identifier for a CRDT version vector. The vector is identified by
     * the CRDT service name and the target member UUID.
     *
     * @see CRDTReplicationAwareService
     */
    private static class ReplicationVersionVectorIdentifier {
        final String memberUUID;
        final String serviceName;

        ReplicationVersionVectorIdentifier(String serviceName, String memberUUID) {
            this.serviceName = checkNotNull(serviceName, "Service name must not be null");
            this.memberUUID = checkNotNull(memberUUID, "Member UUID must not be null");
        }

        @Override
        @SuppressWarnings("checkstyle:innerassignment")
        public boolean equals(Object o) {
            final ReplicationVersionVectorIdentifier that;
            return this == o || o instanceof ReplicationVersionVectorIdentifier
                    && this.serviceName.equals((that = (ReplicationVersionVectorIdentifier) o).serviceName)
                    && this.memberUUID.equals(that.memberUUID);
        }

        @Override
        public int hashCode() {
            int result = memberUUID.hashCode();
            result = 31 * result + serviceName.hashCode();
            return result;
        }
    }
}
