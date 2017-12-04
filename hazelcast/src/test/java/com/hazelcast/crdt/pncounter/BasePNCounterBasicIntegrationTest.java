package com.hazelcast.crdt.pncounter;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Base implementation for simple {@link PNCounter} tests.
 * Concrete implementations must provide the two {@link HazelcastInstance}s
 * which will be used to perform the tests.
 */
public abstract class BasePNCounterBasicIntegrationTest extends HazelcastTestSupport {

    @Test
    public void testSimpleReplication() {
        final PNCounter counter1 = getInstance1().getPNCounter("counter");
        final PNCounter counter2 = getInstance2().getPNCounter("counter");

        assertEquals(5, counter1.addAndGet(5));

        assertCounterValueEventually(5, counter1);
        assertCounterValueEventually(5, counter2);
    }

    protected abstract HazelcastInstance getInstance1();

    protected abstract HazelcastInstance getInstance2();

    private void assertCounterValueEventually(final long expectedValue, final PNCounter counter) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expectedValue, counter.get());
            }
        });
    }
}