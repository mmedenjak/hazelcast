package com.hazelcast.wan;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.io.Serializable;

public class PartitionDiff implements IdentifiedDataSerializable, Serializable {
    public int partitionId;
    public int rangeMinHash;
    public int rangeMaxHash;

    public PartitionDiff() {
    }

    public PartitionDiff(int partitionId, int rangeMinHash, int rangeMaxHash) {
        this.partitionId = partitionId;
        this.rangeMinHash = rangeMinHash;
        this.rangeMaxHash = rangeMaxHash;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.PARTITION_DIFF;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeInt(rangeMinHash);
        out.writeInt(rangeMaxHash);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        partitionId = in.readInt();
        rangeMinHash = in.readInt();
        rangeMaxHash = in.readInt();
    }

    @Override
    public String toString() {
        return "PartitionDiff{" + "partitionId=" + partitionId +
                ", rangeMinHash=" + rangeMinHash +
                ", rangeMaxHash=" + rangeMaxHash +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionDiff that = (PartitionDiff) o;

        if (partitionId != that.partitionId) {
            return false;
        }
        if (rangeMinHash != that.rangeMinHash) {
            return false;
        }
        return rangeMaxHash == that.rangeMaxHash;
    }

    @Override
    public int hashCode() {
        int result = partitionId;
        result = 31 * result + rangeMinHash;
        result = 31 * result + rangeMaxHash;
        return result;
    }
}