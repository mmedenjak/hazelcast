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

package com.hazelcast.map.impl.query;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;

/**
 * Represents a partial query result on a segment of the map.
 * The remaining query results may be retrieved using the {@link #pointers}
 * which defines the iteration state.
 */
public class ResultSegment implements IdentifiedDataSerializable, Versioned {
    private Result result;
    private IterationPointer[] pointers;

    public ResultSegment() {
    }

    public ResultSegment(Result result, IterationPointer[] pointers) {
        this.result = result;
        this.pointers = pointers;
    }

    public Result getResult() {
        return result;
    }

    public void setResult(Result result) {
        this.result = result;
    }

    /**
     * Returns the iteration pointers representing the current iteration state.
     */
    public IterationPointer[] getPointers() {
        return pointers;
    }

    /**
     * Sets the iteration pointers representing the current iteration state.
     */
    public void setPointers(IterationPointer[] pointers) {
        this.pointers = pointers;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.RESULT_SEGMENT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(result);
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
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        result = in.readObject();
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
    }
}
