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

package com.hazelcast.spi.merge;

import com.hazelcast.cardinality.impl.HyperLogLogHolder;
import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.spi.impl.merge.FullMergingEntryHolderImpl;

/**
 * Only available for HyperLogLog backed {@link com.hazelcast.cardinality.CardinalityEstimator}.
 * <p>
 * Uses the default merge algorithm from HyperLogLog research, keeping the max register value of the two given instances.
 * The result should be the union to the two HyperLogLog estimations.
 *
 * @since 3.10
 */
public class HitsAndAccessTimeMergePolicy<K, V> extends AbstractSplitBrainMergePolicy<V, FullMergingEntryHolderImpl<K,V>> {

    @Override
    public V merge(FullMergingEntryHolderImpl<K, V> mergingValue,
                   FullMergingEntryHolderImpl<K, V> existingValue) {
        final long mergingHits = mergingValue.getHits();
        final long existingHits = existingValue.getHits();
        final long mergingAccessTime = mergingValue.getLastAccessTime();
        final long existingAccessTime = existingValue.getLastAccessTime();
        // TODO
        return null;
    }

    @Override
    public int getId() {
        return 0;
    }
}
