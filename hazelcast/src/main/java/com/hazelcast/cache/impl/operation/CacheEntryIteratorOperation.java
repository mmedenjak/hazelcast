/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.CacheEntryIterationResult;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ReadonlyOperation;

import java.io.IOException;

/**
 * <p>Provides iterator functionality for ICache.</p>
 * <p>
 * Initializes and grabs a number of entries defined by <code>size</code> parameter from the
 * {@link com.hazelcast.cache.impl.ICacheRecordStore} with the last table index.
 * </p>
 *
 * @see com.hazelcast.cache.impl.ICacheRecordStore#fetchEntries(IterationPointer[], int)
 */
public class CacheEntryIteratorOperation extends AbstractCacheOperation implements ReadonlyOperation {

    private IterationPointer[] pointers;
    private int size;

    public CacheEntryIteratorOperation() {
    }

    public CacheEntryIteratorOperation(String name, IterationPointer[] pointers, int size) {
        super(name, new HeapData());
        this.pointers = pointers;
        this.size = size;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.ENTRY_ITERATOR;
    }

    @Override
    public void run()
            throws Exception {
        final CacheEntryIterationResult iterator = this.cache.fetchEntries(pointers, size);
        response = iterator;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        if (out.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            out.writeInt(pointers.length);
            for (IterationPointer pointer : pointers) {
                out.writeInt(pointer.getIndex());
                out.writeInt(pointer.getSize());
            }
        } else {
            // RU_COMPAT_3_9
            out.writeInt(pointers[pointers.length - 1].getIndex());
        }
        out.writeInt(size);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);

        if (in.getVersion().isGreaterOrEqual(Versions.V3_10)) {
            final int pointersCount = in.readInt();
            pointers = new IterationPointer[pointersCount];
            for (int i = 0; i < pointersCount; i++) {
                pointers[i] = new IterationPointer(in.readInt(), in.readInt());
            }
        } else {
            // RU_COMPAT_3_9
            final int tableIndex = in.readInt();
            pointers = new IterationPointer[]{new IterationPointer(tableIndex, -1)};
        }
        size = in.readInt();
    }
}
