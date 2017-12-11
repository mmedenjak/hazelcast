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

import com.hazelcast.crdt.CRDTReplicationAwareService;
import com.hazelcast.crdt.CRDTReplicationOperation;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.util.ConstructorFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Service responsible for {@link ORSet} proxies and replication operation.
 */
public class ORSetService implements ManagedService, RemoteService, CRDTReplicationAwareService<ORSetImpl<?>> {
    /** The name under which this service is registered */
    public static final String SERVICE_NAME = "hz:impl:ORSetService";

    private NodeEngine nodeEngine;
    /** Map from set name to set implementation */
    private final ConcurrentMap<String, ORSetImpl<?>> sets = new ConcurrentHashMap<String, ORSetImpl<?>>();

    /** Constructor function for set implementations */
    private final ConstructorFunction<String, ORSetImpl<?>> setConstructorFn =
            new ConstructorFunction<String, ORSetImpl<?>>() {
                @Override
                public ORSetImpl<?> createNew(String name) {
                    return new ORSetImpl<Object>(nodeEngine.getClusterService().getMemberListJoinVersion());
                }
            };

    /**
     * Returns the set with the given {@code name}.
     * @param <T> set item type
     */
    @SuppressWarnings("unchecked")
    public <T> ORSetImpl<T> getSet(String name) {
        return (ORSetImpl<T>) getOrPutIfAbsent(sets, name, setConstructorFn);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void reset() {
        sets.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public ORSetProxy<?> createDistributedObject(String objectName) {
        return new ORSetProxy<Object>(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        sets.remove(objectName);
    }

    @Override
    public CRDTReplicationOperation prepareReplicationOperation(Map<String, Long> versionVector) {
        final HashMap<String, Long> currentVector = new HashMap<String, Long>();
        final HashMap<String, ORSetImpl<?>> sets = new HashMap<String, ORSetImpl<?>>();
        for (Entry<String, ORSetImpl<?>> setEntry : this.sets.entrySet()) {
            final String setName = setEntry.getKey();
            final long currentSetVersion = setEntry.getValue().getVersion();
            if (!versionVector.containsKey(setName) || versionVector.get(setName) < currentSetVersion) {
                sets.put(setName, setEntry.getValue());
            }
            currentVector.put(setName, currentSetVersion);
        }

        return sets.isEmpty()
                ? null
                : new CRDTReplicationOperation(new ORSetReplicationOperation(sets), currentVector);
    }

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void merge(String name, ORSetImpl<?> value) {
        getSet(name).merge((ORSetImpl<Object>) value);
    }
}
