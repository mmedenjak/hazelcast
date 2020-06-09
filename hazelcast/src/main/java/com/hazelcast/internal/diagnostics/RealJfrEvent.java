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

import jdk.jfr.Event;

import java.util.function.Consumer;

class RealJfrEvent<T extends Event> implements JfrEvent<T> {
    private final T event;

    RealJfrEvent(T event) {
        this.event = event;
    }

    public void begin() {
        event.begin();
    }

    public void end() {
        event.end();
    }

    public void commit() {
        event.commit();
    }

    public boolean isEnabled() {
        return event.isEnabled();
    }

    public boolean shouldCommit() {
        return event.shouldCommit();
    }

    public void set(int index, Object value) {
        event.set(index, value);
    }

    @Override
    public void mutate(Consumer<T> action) {
        action.accept(event);
    }
}
