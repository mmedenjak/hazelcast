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

import com.hazelcast.spi.Operation;

import java.util.Map;

/**
 * A container for a CRDT replication operation. It addition to the
 * operation, it contains additional information that may allow a specific
 * CRDT implementation service to optimise further replication operation
 * contents. For instance, the next replication operation may choose to
 * replicate only CRDT states which have changed from the previous
 * replication operation.
 */
public class CRDTReplicationOperation {
    private final Operation operation;
    private final Map<String, Long> versionVector;

    public CRDTReplicationOperation(Operation operation, Map<String, Long> versionVector) {
        this.operation = operation;
        this.versionVector = versionVector;
    }

    public Operation getOperation() {
        return operation;
    }

    public Map<String, Long> getVersionVector() {
        return versionVector;
    }
}
