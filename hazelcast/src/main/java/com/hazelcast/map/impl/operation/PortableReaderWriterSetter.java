package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.serialization.impl.DefaultPortableReaderWriter;

public interface PortableReaderWriterSetter {
    void setPortableReaderWriter(DefaultPortableReaderWriter w);
}