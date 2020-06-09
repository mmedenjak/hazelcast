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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsPublisher;
import jdk.jfr.AnnotationElement;
import jdk.jfr.Category;
import jdk.jfr.Event;
import jdk.jfr.EventFactory;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
import jdk.jfr.ValueDescriptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

public class FlightRecorderPublisher implements MetricsPublisher {
    private final Map<String, Event> metricEvents = new HashMap<>();

    @Override
    public void publishLong(MetricDescriptor descriptor, long value) {
        Event event = eventFor(descriptor, long.class);
        fillMetadata(descriptor, event);
        event.set(4, value);
        event.end();
        event.commit();
    }

    @Override
    public void publishDouble(MetricDescriptor descriptor, double value) {
        Event event = eventFor(descriptor, double.class);
        fillMetadata(descriptor, event);
        event.set(4, value);
        event.end();
        event.commit();
    }

    private Event eventFor(MetricDescriptor descriptor, Class valueClass) {
        return metricEvents.computeIfAbsent(descriptor.prefix(), (FunctionEx<String, Event>) prefix -> {
            prefix = prefix != null ? prefix : "Generic";
            ArrayList<ValueDescriptor> fields = new ArrayList<>(1);
            fields.add(new ValueDescriptor(String.class, "prefix", singletonList(new AnnotationElement(Label.class, "Prefix"))));
            fields.add(new ValueDescriptor(String.class, "metric", singletonList(new AnnotationElement(Label.class, "Metric"))));
            String discriminator = descriptor.discriminator() != null ? descriptor.discriminator() : "Discriminator";
            fields.add(new ValueDescriptor(String.class, discriminator,
                    singletonList(new AnnotationElement(Label.class, discriminator))));
            fields.add(new ValueDescriptor(String.class, "unit", singletonList(new AnnotationElement(Label.class, "Unit"))));
            fields.add(new ValueDescriptor(valueClass, "value", singletonList(new AnnotationElement(Label.class, "Value"))));

            for (int i = 0; i < descriptor.tagCount(); i++) {
                String tag = descriptor.tag(i);
                fields.add(new ValueDescriptor(String.class, tag, singletonList(new AnnotationElement(Label.class, tag))));
            }

            List<AnnotationElement> eventAnnotations = new ArrayList<>();
            eventAnnotations.add(new AnnotationElement(Name.class, prefix));
            eventAnnotations.add(new AnnotationElement(Label.class, prefix));
            eventAnnotations.add(new AnnotationElement(Category.class, new String[]{"Hazelcast", "Metrics"}));
            eventAnnotations.add(new AnnotationElement(StackTrace.class, false));

            EventFactory factory = EventFactory.create(eventAnnotations, fields);

            return factory.newEvent();
        });
    }

    private void fillMetadata(MetricDescriptor descriptor, Event event) {
        event.set(0, descriptor.prefix());
        event.set(1, descriptor.metric());
        event.set(2, descriptor.discriminatorValue());
        event.set(3, descriptor.unit() != null ? descriptor.unit().toString() : null);
        //        for (int i = 0; i < descriptor.tagCount(); i++) {
        //            event.set(5 + i, descriptor.tagValue(i));
        //        }
    }

    @Override
    public String name() {
        return "Flight Recorder publisher";
    }
}
