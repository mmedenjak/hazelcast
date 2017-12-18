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

package com.hazelcast.crdt.pncounter;

import com.hazelcast.crdt.CRDT;
import com.hazelcast.crdt.CRDTDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.MapUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * PN (Positive-Negative) CRDT counter where each replica is identified by
 * a String.
 */
public class PNCounterImpl implements CRDT<PNCounterImpl>, IdentifiedDataSerializable {
    private String replicaId;
    private Map<String, long[]> state = new ConcurrentHashMap<String, long[]>();
    private volatile long version = Long.MIN_VALUE;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    PNCounterImpl(String replicaId) {
        this.replicaId = replicaId;
    }

    public PNCounterImpl() {
    }

    /**
     * Returns the current value of the counter.
     */
    public long get() {
        readLock.lock();
        try {
            long value = 0;
            for (long[] pnValue : state.values()) {
                value += pnValue[0];
                value -= pnValue[1];
            }
            return value;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the previous value
     */
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "The field is updated under lock and read with no lock")
    public long getAndAdd(long delta) {
        writeLock.lock();
        try {
            if (delta < 0) {
                return getAndSubtract(-delta);
            }
            final long previousValue = get();
            final long[] pnValues = state.containsKey(replicaId) ? state.get(replicaId) : new long[]{0, 0};
            pnValues[0] += delta;
            state.put(replicaId, pnValues);
            version++;
            return previousValue;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the updated value
     */
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "The field is updated under lock and read with no lock")
    public long addAndGet(long delta) {
        writeLock.lock();
        try {
            if (delta < 0) {
                return subtractAndGet(-delta);
            }
            final long[] pnValues = state.containsKey(replicaId) ? state.get(replicaId) : new long[]{0, 0};
            pnValues[0] += delta;
            state.put(replicaId, pnValues);
            version++;
            return get();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Subtracts the given value from the current value.
     *
     * @param delta the value to add
     * @return the previous value
     */
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "The field is updated under lock and read with no lock")
    public long getAndSubtract(long delta) {
        writeLock.lock();
        try {
            if (delta < 0) {
                return getAndAdd(-delta);
            }
            final long previousValue = get();
            final long[] pnValues = state.containsKey(replicaId) ? state.get(replicaId) : new long[]{0, 0};
            pnValues[1] += delta;
            state.put(replicaId, pnValues);
            version++;
            return previousValue;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Subtracts the given value from the current value.
     *
     * @param delta the value to subtract
     * @return the updated value
     */
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "The field is updated under lock and read with no lock")
    public long subtractAndGet(long delta) {
        writeLock.lock();
        try {
            if (delta < 0) {
                return addAndGet(-delta);
            }
            final long[] pnValues = state.containsKey(replicaId) ? state.get(replicaId) : new long[]{0, 0};
            pnValues[1] += delta;
            state.put(replicaId, pnValues);
            version++;
            return get();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "The field is updated under lock and read with no lock")
    public void merge(PNCounterImpl other) {
        writeLock.lock();
        try {
            for (Entry<String, long[]> pnCounterEntry : other.state.entrySet()) {
                final String replicaId = pnCounterEntry.getKey();
                final long[] pnOtherValues = pnCounterEntry.getValue();
                final long[] pnValues = state.containsKey(replicaId) ? state.get(replicaId) : new long[]{0, 0};
                pnValues[0] = Math.max(pnValues[0], pnOtherValues[0]);
                pnValues[1] = Math.max(pnValues[1], pnOtherValues[1]);
                version++;
                state.put(replicaId, pnValues);
            }
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
        return CRDTDataSerializerHook.PN_COUNTER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        readLock.lock();
        try {
            out.writeInt(state.size());
            for (Entry<String, long[]> replicaState : state.entrySet()) {
                final String replicaID = replicaState.getKey();
                final long[] replicaCounts = replicaState.getValue();
                out.writeUTF(replicaID);
                out.writeLong(replicaCounts[0]);
                out.writeLong(replicaCounts[1]);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        final int stateSize = in.readInt();
        state = MapUtil.createHashMap(stateSize);
        for (int i = 0; i < stateSize; i++) {
            final String replicaID = in.readUTF();
            final long[] replicaCounts = {in.readLong(), in.readLong()};
            state.put(replicaID, replicaCounts);
        }
    }
}
