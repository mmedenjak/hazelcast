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

import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.core.Member;
import com.hazelcast.crdt.pncounter.operations.AddOperation;
import com.hazelcast.crdt.pncounter.operations.GetOperation;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.crdt.pncounter.PNCounterService.SERVICE_NAME;

/**
 * Member proxy implementation for a {@link PNCounter}.
 */
public class PNCounterProxy extends AbstractDistributedObject<PNCounterService> implements PNCounter {
    /** The counter name */
    private final String name;
    private volatile Address targetAddress;

    PNCounterProxy(String name, NodeEngine nodeEngine, PNCounterService service) {
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

    @Override
    public long get() {
        return invoke(new GetOperation(name));
    }

    @Override
    public long getAndAdd(long delta) {
        return invoke(new AddOperation(name, delta, true));
    }

    @Override
    public long addAndGet(long delta) {
        return invoke(new AddOperation(name, delta, false));
    }

    @Override
    public long getAndSubtract(long delta) {
        return invoke(new AddOperation(name, -delta, true));
    }

    @Override
    public long subtractAndGet(long delta) {
        return invoke(new AddOperation(name, -delta, false));
    }

    @Override
    public long decrementAndGet() {
        return invoke(new AddOperation(name, -1, false));
    }

    @Override
    public long incrementAndGet() {
        return invoke(new AddOperation(name, 1, false));
    }

    @Override
    public long getAndDecrement() {
        return invoke(new AddOperation(name, -1, true));
    }

    @Override
    public long getAndIncrement() {
        return invoke(new AddOperation(name, 1, true));
    }

    /**
     * Invokes the {@code operation}, blocks and returns the result of the
     * invocation.
     * If the member on which this method is invoked is a lite member, the
     * operation will be invoked on a cluster member. The cluster member is
     * chosen randomly once the first operation is about to be invoked and if
     * we detect that the previously chosen member is no longer a data member
     * of the cluster.
     *
     * @param operation the operation to invoke
     * @param <E>       the result type
     * @return the result of the invocation
     * @throws IllegalStateException if the cluster does not contain any data members
     * @throws UnsupportedOperationException if the cluster version is less than 3.10
     * @see ClusterService#getClusterVersion()
     */
    private <E> E invoke(Operation operation) {
        if (getNodeEngine().getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
            throw new UnsupportedOperationException(
                    "PNCounter operations are not supported when cluster version is less than 3.10");
        }
        operation.setValidateTarget(false);
        final NodeEngine nodeEngine = getNodeEngine();
        final InternalCompletableFuture<E> future =
                nodeEngine.getOperationService()
                          .invokeOnTarget(SERVICE_NAME, operation, getCRDTOperationTarget());
        return future.join();
    }

    /**
     * Returns the target on which this proxy should invoke a CRDT operation.
     * If the member on which this proxy is located is a data member, the
     * operation is invoked locally. Otherwise, a random data cluster member
     * is chosen once the first operation is about to be invoked.
     * All further invocations of this method will return that member until
     * the method detects that that member is no longer a data member of the
     * cluster. At that point, a new data member is chosen.
     *
     * @return the address to which CRDT operations should be sent
     * @throws IllegalStateException if the cluster does not contain any data members
     */
    private Address getCRDTOperationTarget() {
        if (!getNodeEngine().getLocalMember().isLiteMember()) {
            return getNodeEngine().getThisAddress();
        }

        final Collection<Member> dataMembers = getNodeEngine().getClusterService()
                                                              .getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);
        if (dataMembers.size() == 0) {
            throw new IllegalStateException(
                    "Cannot invoke operations on a CRDT because the cluster does not contain any data members");
        }
        final ArrayList<Address> dataMemberAddresses = new ArrayList<Address>(dataMembers.size());
        for (Member member : dataMembers) {
            dataMemberAddresses.add(member.getAddress());
        }

        if (targetAddress == null || !dataMemberAddresses.contains(targetAddress)) {
            synchronized (this) {
                if (targetAddress == null || !dataMemberAddresses.contains(targetAddress)) {
                    final int memberIndex = ThreadLocalRandomProvider.get().nextInt(dataMemberAddresses.size());
                    targetAddress = dataMemberAddresses.get(memberIndex);
                }
            }
        }
        return targetAddress;
    }

    @Override
    public String toString() {
        return "PNCounter{name='" + name + "\'}";
    }
}
