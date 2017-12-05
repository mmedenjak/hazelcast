package com.hazelcast.crdt;

import com.pholser.junit.quickcheck.random.SourceOfRandomness;

import java.util.List;

public class Operation<T, S> {
    protected final SourceOfRandomness rnd;
    protected final int crdtIndex;

    public Operation(SourceOfRandomness rnd) {
        this.rnd = rnd;
        this.crdtIndex = rnd.nextInt(0, 10000);
    }

    protected T getCRDT(List<T> crdts) {
        return crdts.get(crdtIndex % crdts.size());
    }

    protected void perform(List<T> crdts, S state) {
        perform(getCRDT(crdts), state);
    }

    protected void perform(T crdt, S state) {
        // intended for override
    }
}