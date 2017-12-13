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
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
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
    private int localReplicaId;
    private volatile long version = Long.MIN_VALUE;
    private Map<E, ReplicaTimestamps> localState = new ConcurrentHashMap<E, ReplicaTimestamps>();
    private ReplicaTimestamps lastObservedReplicaTimestamps = new ReplicaTimestamps();

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    public ORSetImpl(int localReplicaId) {
        this.localReplicaId = localReplicaId;
        this.lastObservedReplicaTimestamps.setReplicaTimestamp(localReplicaId, Long.MIN_VALUE);
    }

    public ORSetImpl() {
    }

    public int size() {
        readLock.lock();
        try {
            return localState.size();
        } finally {
            readLock.unlock();
        }
    }

    public boolean isEmpty() {
        readLock.lock();
        try {
            return localState.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    public boolean contains(Object o) {
        readLock.lock();
        try {
            return localState.containsKey(o);
        } finally {
            readLock.unlock();
        }
    }

    public Collection<E> getAll() {
        readLock.lock();
        try {
            return localState.keySet();
        } finally {
            readLock.unlock();
        }
    }


    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "The field is updated under lock and read with no lock")
    public boolean add(E item) {
        writeLock.lock();
        try {
            version++;
            final long nextTimestamp = lastObservedReplicaTimestamps.getTimestampForReplica(localReplicaId) + 1;
            final boolean newItem = !localState.containsKey(item);
            if (newItem) {
                localState.put(item, new ReplicaTimestamps());
            }
            localState.get(item).setReplicaTimestamp(localReplicaId, nextTimestamp);
            lastObservedReplicaTimestamps.setReplicaTimestamp(localReplicaId, nextTimestamp);
            return newItem;
        } finally {
            writeLock.unlock();
        }
    }

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "The field is updated under lock and read with no lock")
    boolean remove(Object o) {
        writeLock.lock();
        try {
            version++;
            return localState.remove(o) != null;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "The field is updated under lock and read with no lock")
    public void merge(ORSetImpl<E> other) {
        writeLock.lock();
        try {
            version++;
            for (Entry<E, ReplicaTimestamps> remoteEntry : other.localState.entrySet()) {
                final E item = remoteEntry.getKey();
                final ReplicaTimestamps remoteTimestamps = remoteEntry.getValue();
                final ReplicaTimestamps localTimestamps = localState.get(item);

                if (localTimestamps != null) {
                    localTimestamps.merge(remoteTimestamps);
                }

            }
            final Iterator<Entry<E, ReplicaTimestamps>> localStateIterator = localState.entrySet().iterator();

            while (localStateIterator.hasNext()) {
                final Entry<E, ReplicaTimestamps> localEntry = localStateIterator.next();
                final E item = localEntry.getKey();
                final ReplicaTimestamps localTimestamps = localEntry.getValue();
                if (!other.localState.containsKey(item)
                        && localTimestamps.isBefore(other.lastObservedReplicaTimestamps)) {
                    localStateIterator.remove();
                }
            }
            this.lastObservedReplicaTimestamps.merge(other.lastObservedReplicaTimestamps);
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


    private static class ReplicaTimestamps {
        private final Map<Integer, Long> replicaTimestamps = new ConcurrentHashMap<Integer, Long>();

        public Long getTimestampForReplica(int replicaId) {
            return replicaTimestamps.get(replicaId);
        }

        public void setReplicaTimestamp(int replicaId, long timestamp) {
            replicaTimestamps.put(replicaId, timestamp);
        }

        public void merge(ReplicaTimestamps other) {
            for (Entry<Integer, Long> entry : other.replicaTimestamps.entrySet()) {
                final Integer replicaId = entry.getKey();
                final Long mergingTimestamp = entry.getValue();
                final long localTimestamp = replicaTimestamps.containsKey(replicaId)
                        ? replicaTimestamps.get(replicaId)
                        : Long.MIN_VALUE;
                replicaTimestamps.put(replicaId, Math.max(localTimestamp, mergingTimestamp));
            }
        }

        public boolean isBefore(ReplicaTimestamps other) {
            return false;
        }
    }
}
