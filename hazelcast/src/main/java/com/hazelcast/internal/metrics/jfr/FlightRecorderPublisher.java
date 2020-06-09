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

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsPublisher;

public class FlightRecorderPublisher implements MetricsPublisher {
    @Override
    public void publishLong(MetricDescriptor descriptor, long value) {
        MetricLongEvent event = new MetricLongEvent();
        fillMetadata(descriptor, event);
        event.value = value;
        event.commit();
    }

    @Override
    public void publishDouble(MetricDescriptor descriptor, double value) {
        MetricDoubleEvent event = new MetricDoubleEvent();
        fillMetadata(descriptor, event);
        event.value = value;
        event.commit();
    }

    private void fillMetadata(MetricDescriptor descriptor, AbstractMetricEvent event) {
        event.prefix = descriptor.prefix();
        event.metric = descriptor.metric();
        event.discriminator = descriptor.discriminator() + ":" + descriptor.discriminatorValue();
        event.unit = descriptor.unit();
        descriptor.readTags(event::tag);
    }

    @Override
    public String name() {
        return "Flight Recorder publisher";
    }
}