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

import com.hazelcast.core.DistributedObject;

/**
 * PN (Positive-Negative) CRDT counter.
 * <p>
 * The counter supports adding and subtracting values as well as
 * retrieving the current counter value.
 * <p>
 * Each replica of this counter can perform operations locally without
 * coordination with the other replicas, thus increasing availability.
 * <p>
 * The counter guarantees that whenever two nodes have received the
 * same set of updates, possibly in a different order, their state is
 * identical, and any conflicting updates are merged automatically.
 * If no new updates are made to the shared state, all nodes that can
 * communicate will eventually have the same data.
 *
 * @since 3.10
 */
public interface PNCounter extends DistributedObject {
    /**
     * Returns the current value of the counter.
     */
    long get();

    /**
     * Adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the previous value
     */
    long getAndAdd(long delta);

    /**
     * Adds the given value to the current value.
     *
     * @param delta the value to add
     * @return the updated value
     */
    long addAndGet(long delta);

    /**
     * Subtracts the given value from the current value.
     *
     * @param delta the value to add
     * @return the previous value
     */
    long getAndSubtract(long delta);

    /**
     * Subtracts the given value from the current value.
     *
     * @param delta the value to subtract
     * @return the updated value
     */
    long subtractAndGet(long delta);

    /**
     * Decrements by one the current value.
     *
     * @return the updated value
     */
    long decrementAndGet();

    /**
     * Increments by one the current value.
     *
     * @return the updated value
     */
    long incrementAndGet();

    /**
     * Decrements by one the current value.
     *
     * @return the previous value
     */
    long getAndDecrement();

    /**
     * Increments by one the current value.
     *
     * @return the previous value
     */
    long getAndIncrement();
}
