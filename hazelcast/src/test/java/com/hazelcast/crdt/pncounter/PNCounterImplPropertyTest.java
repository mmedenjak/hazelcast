package com.hazelcast.crdt.pncounter;

import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

@RunWith(JUnitQuickcheck.class)
@Category({QuickTest.class, ParallelTest.class})
public class PNCounterImplPropertyTest {

    @Property
    public void strongEventualConsistency(@InRange(minInt = 2, maxInt = 5) int counterCount,
                                          @InRange(minInt = 50, maxInt = 100) int rounds,
                                          double incrementFrequency,
                                          double decrementFrequency,
                                          double mergeFrequency) {
        final PNCounterImpl[] counters = setupCounters(counterCount);
        int value = 0;
        final Random rnd = new Random();
        for (int i = 0; i < rounds; i++) {
            for (int j = 0; j < counters.length; j++) {
                final PNCounterImpl counter = counters[j];
                if (rnd.nextDouble() < incrementFrequency) {
                    value++;
                    counter.addAndGet(1);
                }
                if (rnd.nextDouble() < decrementFrequency) {
                    value--;
                    counter.addAndGet(-1);
                }
                if (rnd.nextDouble() < mergeFrequency) {
                    final PNCounterImpl previous = counters[Math.max(0, j - 1)];
                    counter.merge(previous);
                }
            }
            mergeCounters(counters);
            assertAllCounters(counters, value);
        }
    }


    private PNCounterImpl[] setupCounters(int size) {
        final PNCounterImpl[] counters = new PNCounterImpl[size];
        for (int i = 0; i < size; i++) {
            counters[i] = new PNCounterImpl(i);
        }
        return counters;
    }

    private void assertAllCounters(PNCounterImpl[] counters, long expected) {
        final long[] actuals = new long[counters.length];
        final long[] expecteds = new long[counters.length];
        for (int i = 0; i < counters.length; i++) {
            actuals[i] = counters[i].get();
            expecteds[i] = expected;
        }
        assertArrayEquals("Counter values differ", expecteds, actuals);
    }

    private void mergeCounters(PNCounterImpl[] counters) {
        for (int i = 0; i < counters.length; i++) {
            for (int j = 0; j < counters.length; j++) {
                if (i != j) {
                    counters[i].merge(counters[j]);
                }
            }
        }
    }
}