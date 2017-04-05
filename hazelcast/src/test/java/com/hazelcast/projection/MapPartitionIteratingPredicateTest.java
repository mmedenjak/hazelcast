/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.projection;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.PartitionIteratingPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapPartitionIteratingPredicateTest extends HazelcastTestSupport {

    private static int size = 100;
    private static int pageSize = 5;

    private IMap<String, Integer> map;

    @Before
    public void setup() {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance local = nodeFactory.newHazelcastInstance(config);
        nodeFactory.newHazelcastInstance(config);
        map = local.getMap(randomString());
        for (int i = 0; i < size; i++) {
            map.put(generateKeyForPartition(local, 1), i);
        }
    }

    @Test
    public void testPartitionIterator() {
        final PartitionIteratingPredicate<Integer, Integer> p = new PartitionIteratingPredicate<Integer, Integer>(
                new TruePredicate(), pageSize, 1);
        final ArrayList<Integer> values = new ArrayList<Integer>();
        Collection<Integer> fetched;
        while ((fetched = map.values(p)).size() > 0) {
            values.addAll(fetched);
        }
        final Collection<Integer> vals = map.values();
        assertContainsAll(values, vals);
        assertEquals(vals.size(), values.size());
    }
}
