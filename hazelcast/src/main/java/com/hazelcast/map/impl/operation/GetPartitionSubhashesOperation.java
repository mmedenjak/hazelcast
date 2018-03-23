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

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.recordstore.MerkleTreeNode;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;

import java.io.IOException;

public class GetPartitionSubhashesOperation extends MapOperation implements PartitionAwareOperation, ReadonlyOperation {

    private int rangeMinKeyHash;
    private int rangeMaxKeyHash;
    private int[] hashes;


    public GetPartitionSubhashesOperation() {
    }

    public GetPartitionSubhashesOperation(String name, int rangeMinKeyHash, int rangeMaxKeyHash) {
        super(name);
        this.rangeMinKeyHash = rangeMinKeyHash;
        this.rangeMaxKeyHash = rangeMaxKeyHash;
    }

    @Override
    public void run() {
        recordStore.checkIfLoaded();
        final MerkleTreeNode tree = recordStore.getStorage().getTree();
        hashes = tree.getRangeSubHashes(rangeMinKeyHash, rangeMaxKeyHash);
    }

    @Override
    public Object getResponse() {
        return hashes;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.GET_PARTITION_RANGE_SUB_HASHES;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(rangeMinKeyHash);
        out.writeInt(rangeMaxKeyHash);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        rangeMinKeyHash = in.readInt();
        rangeMaxKeyHash = in.readInt();
    }
}
