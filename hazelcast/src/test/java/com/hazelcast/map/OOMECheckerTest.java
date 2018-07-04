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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Random;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OOMECheckerTest extends HazelcastTestSupport {

    public static final boolean OOME_PROTECTION = false;

    protected Config getConfig() {
        Config cfg = super.getConfig();
        cfg.getOomeProtectionConfig()
           .setEnabled(OOME_PROTECTION)
           .setMinFreePercentage(20)
           .setEvictPercentage(30);
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        return cfg;
    }

    @Test
    public void testMapInstanceOOME() {
        final HazelcastInstance instance = createHazelcastInstance();
        final IMap<Object, Object> map = instance.getMap("mappy");

        for (int i = 0; i < 10000; i++) {
            map.put(i, new byte[100000]);
        }
    }

    @Test
    public void testMapInstanceOOME2() {
        final HazelcastInstance instance = createHazelcastInstance();
        final IMap<Object, Object> map = instance.getMap("mappy");

        for (int i = 0; i < 1000000; i++) {
            map.put(i % 100, new byte[100000]);
        }
    }


    @Test
    public void testRingbufferInstanceOOME() {
        final HazelcastInstance instance = createHazelcastInstance();
        Ringbuffer<Object> ringbuffer = instance.getRingbuffer("ringbuffer");

        for (int i = 0; i < 10000; i++) {
            ringbuffer.add(new byte[100000]);
        }
    }

    @Test
    public void testMixedInstanceOOME() {
        final HazelcastInstance instance = createHazelcastInstance();
        IMap<Object, Object> map = instance.getMap("map");
        Ringbuffer<Object> ringbuffer = instance.getRingbuffer("ringbuffer");
        ISet<Object> set = instance.getSet("set");
        IList<Object> list = instance.getList("list");
        Random random = new Random();

        for (int i = 0; i < 10000; i++) {

            switch (random.nextInt(4)){
                case 0:
                    map.put(i, new byte[100000]);
                    break;
                case 1:
                    ringbuffer.add(new byte[100000]);
                    break;
                case 2:
                    list.add(new byte[100000]);
                    break;
                case 3:
                    set.add(new byte[100000]);
            }
        }
    }
}
