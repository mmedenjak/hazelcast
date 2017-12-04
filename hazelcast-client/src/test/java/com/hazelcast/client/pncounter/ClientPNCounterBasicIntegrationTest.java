package com.hazelcast.client.pncounter;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.crdt.pncounter.BasePNCounterBasicIntegrationTest;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Client implementation for basic
 * {@link com.hazelcast.crdt.pncounter.PNCounter} integration tests
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientPNCounterBasicIntegrationTest extends BasePNCounterBasicIntegrationTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private HazelcastInstance[] members;
    private HazelcastInstance[] clients;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        final Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "5")
                .setCRDTReplicationConfig(new CRDTReplicationConfig().setReplicationPeriodMillis(200)
                                                                     .setMaxConcurrentReplicationTargets(Integer.MAX_VALUE));

        members = hazelcastFactory.newInstances(config, 2);
        clients = new HazelcastInstance[]{hazelcastFactory.newHazelcastClient(), hazelcastFactory.newHazelcastClient()};
    }

    protected HazelcastInstance getInstance1() {
        return clients[0];
    }

    protected HazelcastInstance getInstance2() {
        return clients[1];
    }
}