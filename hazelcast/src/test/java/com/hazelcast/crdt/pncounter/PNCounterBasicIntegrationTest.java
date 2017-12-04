package com.hazelcast.crdt.pncounter;

import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Member implementation for basic
 * {@link com.hazelcast.crdt.pncounter.PNCounter} integration tests
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PNCounterBasicIntegrationTest extends BasePNCounterBasicIntegrationTest {

    private HazelcastInstance[] instances;

    @Before
    public void setup() {
        final Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "5")
                .setCRDTReplicationConfig(new CRDTReplicationConfig().setReplicationPeriodMillis(200)
                                                                     .setMaxConcurrentReplicationTargets(Integer.MAX_VALUE));
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        instances = factory.newInstances(config);
    }

    protected HazelcastInstance getInstance1() {
        return instances[0];
    }

    protected HazelcastInstance getInstance2() {
        return instances[1];
    }
}