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

import com.hazelcast.crdt.pncounter.PNCounterImpl;

/**
 * Addition/subtraction operation for a
 * {@link com.hazelcast.crdt.pncounter.PNCounter}.
 * The operation is meant to be invoked locally and will throw exceptions
 * when being (de)serialized.
 */
public class AddOperation extends AbstractPNCounterOperation {
    private final boolean getBeforeUpdate;
    private final long delta;
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

    @Override
    public void run() throws Exception {
        final PNCounterImpl counter = getPNCounter();
        result = getBeforeUpdate ? counter.getAndAdd(delta) : counter.addAndGet(delta);
    }

    @Override
    public Long getResponse() {
        return result;
    }
}
