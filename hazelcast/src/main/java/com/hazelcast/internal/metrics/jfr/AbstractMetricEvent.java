/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.jfr;

import com.hazelcast.internal.metrics.ProbeUnit;
import jdk.jfr.Category;
import jdk.jfr.Event;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

@Category({"Hazelcast", "Metrics"})
@StackTrace(false)
public abstract class AbstractMetricEvent extends Event {
    @Name("discriminator")
    String discriminator;

    @Name("prefix")
    String prefix;

    @Name("metric")
    String metric;

    @Name("unit")
    ProbeUnit unit;

    // TODO map? really?
    @Name("tags")
    Map<String, String> tags;

    void tag(String tag, String tagValue) {
        if (tags == null) {
            tags = createHashMap(1);
        }
        tags.put(tag, tagValue);
    }
}
