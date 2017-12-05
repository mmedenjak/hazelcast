package com.hazelcast.crdt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public abstract class BaseCRDTPropertyTest<C extends CRDT<C>, H, S> {

    protected void strongEventualConsistency(int counterCount, Operation<C, H>[] operations) {
        final List<C> crdts = setupCRDTs(counterCount);
        final H state = getStateHolder();
        for (Operation<C, H> operation : operations) {
            operation.perform(crdts, state);
        }
        mergeAll(crdts);
        assertState(crdts, state, counterCount, operations);
    }

    protected abstract C getCRDT(int i);

    protected abstract H getStateHolder();

    protected abstract S getStateValue(C t);

    protected abstract S getStateValue(H t);

    protected void assertState(List<C> crdts, H stateHolder, int counterCount, Operation<C, H>[] operations) {
        final Object[] actuals = new Object[crdts.size()];
        final Object[] expecteds = new Object[crdts.size()];
        for (int i = 0; i < crdts.size(); i++) {
            actuals[i] = getStateValue(crdts.get(i));
            expecteds[i] = getStateValue(stateHolder);
        }
        assertArrayEquals("States differ for " + counterCount + " with operations " + Arrays.toString(operations),
                expecteds, actuals);
    }

    private List<C> setupCRDTs(int size) {
        final List<C> counters = new ArrayList<C>(size);
        for (int i = 0; i < size; i++) {
            counters.add(getCRDT(i));
        }
        return counters;
    }

    private void mergeAll(List<C> crdts) {
        for (C c1 : crdts) {
            for (C c2 : crdts) {
                if (c1 != c2) {
                    c1.merge(c2);
                }
            }
        }
    }
}