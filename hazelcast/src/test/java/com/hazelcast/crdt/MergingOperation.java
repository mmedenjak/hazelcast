package com.hazelcast.crdt;

import com.pholser.junit.quickcheck.random.SourceOfRandomness;

import java.util.List;

public class MergingOperation<C extends CRDT<C>, S> extends Operation<C, S> {
    private final int sourceIdx;

    MergingOperation(SourceOfRandomness rnd) {
        super(rnd);
        this.sourceIdx = rnd.nextInt();
    }

    @Override
    protected void perform(List<C> crdts, S state) {
        final C target = getCRDT(crdts);
        int sourceIdx = rnd.nextInt(crdts.size());
        if (crdts.get(sourceIdx) == target) {
            sourceIdx = (sourceIdx + 1) % crdts.size();
        }
        target.merge(crdts.get(sourceIdx));
    }

    @Override
    public String toString() {
        return "Merge(" + crdtIndex + "," + sourceIdx + ")";
    }
}