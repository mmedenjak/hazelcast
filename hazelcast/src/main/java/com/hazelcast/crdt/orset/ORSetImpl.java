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

package com.hazelcast.crdt.orset;

import com.hazelcast.crdt.CRDT;
import com.hazelcast.crdt.CRDTDataSerializerHook;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * OR (Observed-Remove) CRDT set where each replica is identified by an
 * integer.
 *
 * @param <E> the type of elements maintained by this set
 * @see ClusterService#getMemberListJoinVersion()
 */
public class ORSetImpl<E> implements CRDT<ORSetImpl<E>>, IdentifiedDataSerializable {
    private int replicaId;
    private volatile long version = Long.MIN_VALUE;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    public ORSetImpl(int replicaId) {
        this.replicaId = replicaId;
    }

    public ORSetImpl() {
    }

    public E get() {
        readLock.lock();
        try {
            return null;
        } finally {
            readLock.unlock();
        }
    }

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "The field is updated under lock and read with no lock")
    public E add(E item) {
        writeLock.lock();
        try {
            version++;
            return item;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "The field is updated under lock and read with no lock")
    public void merge(ORSetImpl<E> other) {
        writeLock.lock();
        try {
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public int getFactoryId() {
        return CRDTDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CRDTDataSerializerHook.OR_SET;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        readLock.lock();
        try {
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }
}
