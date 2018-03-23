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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractLocalOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.Clock;

import java.util.concurrent.ConcurrentMap;

/**
 * Clears expired records.
 */
public class CleanMerkleTree extends AbstractLocalOperation implements PartitionAwareOperation, MutatingOperation,
        IdentifiedDataSerializable {

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        if (getNodeEngine().getLocalMember().isLiteMember()) {
            // this operation shouldn't run on lite members. This situation can potentially be seen
            // when converting a data-member to lite-member during merge operations.
            return;
        }

        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(getPartitionId());
        ConcurrentMap<String, RecordStore> recordStores = partitionContainer.getMaps();
        for (final RecordStore recordStore : recordStores.values()) {
            recordStore.cleanMerkleTree();
        }
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

}
