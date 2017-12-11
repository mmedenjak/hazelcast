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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.cluster.ClusterService;

/**
 * OR (Observed-Remove) CRDT set.
 * <p>
 * The set supports adding and subtracting items as well as querying the
 * set contents.
 * Each replica of this set can perform operations locally without
 * coordination with the other replicas, thus increasing availability.
 * The set guarantees that whenever two nodes have received the
 * same set of updates, possibly in a different order, their state is
 * identical, and any conflicting updates are merged automatically.
 * If no new updates are made to the shared state, all nodes that can
 * communicate will eventually have the same data.
 * <p>
 * Each replica is identified by an integer. This identifier should be
 * unique and there should not be any other replica with the same ID, even
 * if the member with that ID is no longer alive. For this identifier, we
 * use the {@link ClusterService#getMemberListJoinVersion()} which should
 * be unique with some exceptions.
 * <p>
 * The updates to this set are applied locally when invoked on a member.
 * When invoking updates from a hazelcast client, the invocation is remote.
 * This may lead to indeterminate state - the update may be applied but the
 * response has not been received. In this case, the caller will be notified
 * with a {@link com.hazelcast.spi.exception.TargetDisconnectedException}.
 *
 * @param <T> set item type
 * @since 3.10
 */
public interface ORSet<T> extends DistributedObject {

}
