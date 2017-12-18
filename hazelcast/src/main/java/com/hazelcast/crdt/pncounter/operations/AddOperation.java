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

package com.hazelcast.crdt.pncounter.operations;

import com.hazelcast.crdt.CRDTDataSerializerHook;
import com.hazelcast.crdt.pncounter.PNCounterImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Addition/subtraction operation for a
 * {@link com.hazelcast.crdt.pncounter.PNCounter}.
 */
public class AddOperation extends AbstractPNCounterOperation {
    private boolean getBeforeUpdate;
    private long delta;
    private long result;

    /**
     * Creates the addition operation.
     *
     * @param name            the name of the PNCounter
     * @param delta           the delta to add to the counter value, can be negative
     * @param getBeforeUpdate {@code true} if the operation should return the
     *                        counter value before the addition, {@code false}
     *                        if it should return the value after the addition
     */
    public AddOperation(String name, long delta, boolean getBeforeUpdate) {
        super(name);
        this.delta = delta;
        this.getBeforeUpdate = getBeforeUpdate;
    }

    public AddOperation() {
    }

    @Override
    public void run() throws Exception {
        final PNCounterImpl counter = getPNCounter();
        result = getBeforeUpdate ? counter.getAndAdd(delta) : counter.addAndGet(delta);
    }

    @Override
    public Long getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeBoolean(getBeforeUpdate);
        out.writeLong(delta);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        getBeforeUpdate = in.readBoolean();
        delta = in.readLong();
    }

    @Override
    public int getId() {
        return CRDTDataSerializerHook.PN_COUNTER_ADD_OPERATION;
    }
}
