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

import com.hazelcast.crdt.CRDTReplicationAwareService;
import com.hazelcast.crdt.CRDTReplicationOperation;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.UuidUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Service responsible for {@link PNCounter} proxies and replication operation.
 */
public class PNCounterService implements ManagedService, RemoteService, CRDTReplicationAwareService<PNCounterImpl> {
    /** The name under which this service is registered */
    public static final String SERVICE_NAME = "hz:impl:PNCounterService";

    private NodeEngine nodeEngine;
    /** Map from counter name to counter implementations */
    private final ConcurrentMap<String, PNCounterImpl> counters = new ConcurrentHashMap<String, PNCounterImpl>();
    /** Constructor function for counter implementations */
    private final ConstructorFunction<String, PNCounterImpl> counterConstructorFn =
            new ConstructorFunction<String, PNCounterImpl>() {
                @Override
                public PNCounterImpl createNew(String name) {
                    return new PNCounterImpl(UuidUtil.newUnsecureUuidString());
                }
            };

    /**
     * Returns the counter with the given {@code name}.
     */
    public PNCounterImpl getCounter(String name) {
        return getOrPutIfAbsent(counters, name, counterConstructorFn);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void reset() {
        counters.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public PNCounterProxy createDistributedObject(String objectName) {
        return new PNCounterProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        counters.remove(objectName);
    }

    @Override
    public CRDTReplicationOperation prepareReplicationOperation(Map<String, Long> versionVector) {
        final HashMap<String, Long> currentVector = new HashMap<String, Long>();
        final HashMap<String, PNCounterImpl> counters = new HashMap<String, PNCounterImpl>();
        for (Entry<String, PNCounterImpl> counterEntry : this.counters.entrySet()) {
            final String counterName = counterEntry.getKey();
            final long currentCounterVersion = counterEntry.getValue().getVersion();
            if (!versionVector.containsKey(counterName) || versionVector.get(counterName) < currentCounterVersion) {
                counters.put(counterName, counterEntry.getValue());
            }
            currentVector.put(counterName, currentCounterVersion);
        }

        return counters.isEmpty()
                ? null
                : new CRDTReplicationOperation(new PNCounterReplicationOperation(counters), currentVector);
    }

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void merge(String name, PNCounterImpl value) {
        getCounter(name).merge(value);
    }
}
