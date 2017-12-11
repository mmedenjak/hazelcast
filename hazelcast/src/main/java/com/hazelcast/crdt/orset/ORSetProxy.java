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

import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import static com.hazelcast.crdt.orset.ORSetService.SERVICE_NAME;

/**
 * Member proxy implementation for a {@link ORSet}.
 *
 * @param <T> set item type
 */
public class ORSetProxy<T> extends AbstractDistributedObject<ORSetService> implements ORSet<T> {
    /** The set name */
    private final String name;

    ORSetProxy(String name, NodeEngine nodeEngine, ORSetService service) {
        super(nodeEngine, service);
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }


    /**
     * Invokes the {@code operation} locally on this member, blocks and returns
     * the result of the invocation.
     *
     * @param operation the operation to invoke
     * @param <E>       the result type
     * @return the result of the invocation
     */
    private <E> E invokeLocally(Operation operation) {
        operation.setValidateTarget(false);
        final NodeEngine nodeEngine = getNodeEngine();
        final InternalCompletableFuture<E> future =
                nodeEngine.getOperationService()
                          .invokeOnTarget(SERVICE_NAME, operation, nodeEngine.getThisAddress());
        return future.join();
    }

    @Override
    public String toString() {
        return "ORSet{name='" + name + "\'}";
    }
}
