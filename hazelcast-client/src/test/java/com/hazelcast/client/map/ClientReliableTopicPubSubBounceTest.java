/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.Repeat;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.jitter.JitterRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * TODO
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
@Repeat(50)
public class ClientReliableTopicPubSubBounceTest extends HazelcastTestSupport {
    private static final int CONCURRENCY = 4;
    private static final String TEST_TOPIC_NAME = "topic";

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig()).clusterSize(4).driverCount(4).build();

    @Rule
    public JitterRule jitterRule = new JitterRule();

    private Subscriber[] subscribers = new Subscriber[CONCURRENCY];
    private Publisher publisher;

    @Before
    public void setup() {
        for (int i = 0; i < CONCURRENCY; i++) {
            Subscriber subscriber = new Subscriber();
            bounceMemberRule.getNextTestDriver()
                            .getReliableTopic(TEST_TOPIC_NAME)
                            .addMessageListener(subscriber);
            subscribers[i] = subscriber;
        }
    }

    @Test
    public void testQuery() {
        this.publisher = new Publisher((bounceMemberRule.getNextTestDriver()));
        bounceMemberRule.testRepeatedly(new Publisher[]{publisher}, MINUTES.toSeconds(1));
    }

    @After
    public void assertSubscribersGotAll() {
        final long published = publisher.getCount();
        System.out.println("Published " + published);
        for (final Subscriber subscriber : subscribers) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    Assert.assertEquals(published, subscriber.getCount());
                }
            });
        }
    }

    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.addRingBufferConfig(new RingbufferConfig("topic")
                .setTimeToLiveSeconds(5));
        return config;
    }

    public static class Subscriber implements MessageListener<Object> {
        private long count = 0;

        public void onMessage(Message<Object> message) {
            count++;
        }

        public long getCount() {
            return count;
        }
    }

    public static class Publisher implements Runnable {
        private final ITopic<Object> topic;
        private long count = 0;

        Publisher(HazelcastInstance instance) {
            topic = instance.getReliableTopic(TEST_TOPIC_NAME);
        }

        @Override
        public void run() {
            topic.publish(new byte[0]);
            count++;
        }

        public long getCount() {
            return count;
        }
    }
}
