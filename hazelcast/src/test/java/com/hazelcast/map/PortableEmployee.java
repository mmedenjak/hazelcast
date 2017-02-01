package com.hazelcast.map;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortableEmployee implements Portable {

    private long id;
    private String name;
    private int age;
    private boolean isEmployee;
    Portable[] duties;

    public PortableEmployee() {
    }

    public PortableEmployee(int age, boolean isEmployee, long id, String name, Portable[] duties) {
        this.age = age;
        this.isEmployee = isEmployee;
        this.id = id;
        this.name = name;
        this.duties = duties;
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getClassId() {
        return 2;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeBoolean("isEmployee", isEmployee);
        writer.writeInt("age", age);
        writer.writeLong("id", id);
        writer.writeUTF("name", name);
        writer.writePortableArray("duties", duties);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        isEmployee = reader.readBoolean("isEmployee");
        age = reader.readInt("age");
        id = reader.readLong("id");
        name = reader.readUTF("name");
        duties = reader.readPortableArray("duties");
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public boolean getIsEmployee() {
        return isEmployee;
    }

    public void setIsEmployee(boolean employee) {
        isEmployee = employee;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }
}