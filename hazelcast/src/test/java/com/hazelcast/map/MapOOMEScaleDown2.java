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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class MapOOMEScaleDown2 {

    public static void main(String[] args) {
        Config cfg = new Config();
        cfg.getOomeProtectionConfig().setEnabled(false);
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);

        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();

        final IMap<Object, Object> map = instance.getMap("mappy");
        for (int i = 0; i < 10000; i++) {
            map.put(i, new byte[100000]);
        }
        instance.shutdown();
    }
}
