package com.hazelcast.map;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortableDuty implements Portable {

    private String desc;
    private int dayOfWeek;

    public PortableDuty() {
    }

    public PortableDuty(String desc, int dayOfWeek) {
        this.desc = desc;
        this.dayOfWeek = dayOfWeek;
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getClassId() {
        return 3;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("desc", desc);
        writer.writeInt("dayOfWeek", dayOfWeek);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        desc = reader.readUTF("desc");
        dayOfWeek = reader.readInt("dayOfWeek");
    }
}