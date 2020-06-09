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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.instance.impl.Node;
import jdk.jfr.Event;
import jdk.jfr.FlightRecorder;

public class JfrService implements JfrEventProvider {
    private final boolean enabled;

    public JfrService(Node node) {
        this.enabled = FlightRecorder.isAvailable() && FlightRecorder.isInitialized();
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public <T extends Event> JfrEvent<T> provide(JfrEventFactory<T> eventFactory) {
        if (!enabled) {
            return (NopJfrEvent<T>) new NopJfrEvent();
        }

        return new RealJfrEvent<>(eventFactory.create());
    }
}
