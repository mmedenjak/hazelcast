package com.hazelcast.map;

import com.hazelcast.internal.serialization.impl.DefaultPortableReaderWriter;
import com.hazelcast.map.impl.operation.PortableReaderWriterSetter;

public class PortableWrapper extends PortableEmployee implements PortableReaderWriterSetter {
    private DefaultPortableReaderWriter rw;

    @Override
    public int getAge() {
        return rw.readInt("age");
    }

    @Override
    public void setAge(int age) {
        rw.writeInt("age", age);
    }

    @Override
    public boolean getIsEmployee() {
        return rw.readBoolean("isEmployee");
    }

    @Override
    public void setIsEmployee(boolean employee) {
        rw.writeBoolean("isEmployee", employee);
    }

    @Override
    public long getId() {
        return rw.readLong("id");
    }

    @Override
    public void setId(long id) {
        rw.writeLong("id", id);
    }

    @Override
    public void setPortableReaderWriter(DefaultPortableReaderWriter w) {
        rw = w;
    }
}