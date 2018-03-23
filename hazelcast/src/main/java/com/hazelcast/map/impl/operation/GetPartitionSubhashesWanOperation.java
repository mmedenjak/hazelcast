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

import com.hazelcast.internal.cluster.impl.operations.WanReplicationOperation;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.SetUtil;
import com.hazelcast.wan.PartitionDiff;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class GetPartitionSubhashesWanOperation extends Operation implements IdentifiedDataSerializable, WanReplicationOperation {

    private Set<PartitionDiff> diffPartitions;
    private String mapName;
    private Map<PartitionDiff, int[]> hashes;

    public GetPartitionSubhashesWanOperation() {
    }

    public GetPartitionSubhashesWanOperation(String mapName, Set<PartitionDiff> diffPartitions) {
        this.mapName = mapName;
        this.diffPartitions = diffPartitions;
    }

    @Override
    public void run() {
        final MapService service = getNodeEngine().getService(MapService.SERVICE_NAME);
        hashes = service.getMapServiceContext().getLocalSubrangeHashes(mapName, diffPartitions);
    }

    @Override
    public Object getResponse() {
        return hashes;
    }


    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(mapName);
        out.writeInt(diffPartitions.size());
        for (PartitionDiff diffPartition : diffPartitions) {
            out.writeObject(diffPartition);
        }

    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        mapName = in.readUTF();
        final int diffPartitionsSize = in.readInt();
        diffPartitions = SetUtil.createHashSet(diffPartitionsSize);
        for (int i = 0; i < diffPartitionsSize; i++) {
            diffPartitions.add((PartitionDiff) in.readObject());
        }
    }


    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.GET_PARTITION_RANGE_SUB_HASHES_WAN;
    }
}
