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

import java.util.Map;

/**
 * Represents a service implementing a CRDT that may be replicated to
 * other replicas of the same CRDT. The service may optimize the
 * replication operation contents depending on the provided version vector.
 * The version vector is a map from CRDT name to the last successfully
 * replicated CRDT state version. All CRDTs in this map should be of the
 * same type.
 *
 * @param <T> CRDT implementation type
 */
public interface CRDTReplicationAwareService<T> {
    /**
     * Returns a replication operation for the provided version vector.
     * The version vector is a map from CRDT name to the last successfully
     * replicated CRDT state version. All CRDTs in this map should be of the
     * same type.
     */
    CRDTReplicationOperation prepareReplicationOperation(Map<String, Long> versionVector);

    /** Returns the name of the service */
    String getName();

    /**
     * Performs a merge of the local {@code name} CRDT with the provided state.
     *
     * @param name  the CRDT name
     * @param value the CRDT state to merge into the local state
     */
    void merge(String name, T value);
}
