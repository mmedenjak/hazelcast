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

package com.hazelcast.map.impl.journal;

import com.hazelcast.nio.serialization.Data;

/**
 * The event journal item for map events. It contains serialized
 * values for key and all values included in map mutations as well as
 * the event type.
 */
public class InternalEventJournalMapEvent {
    protected Data dataKey;
    protected Data dataNewValue;
    protected Data dataOldValue;
    protected int eventType;

    public InternalEventJournalMapEvent() {
    }

    public InternalEventJournalMapEvent(Data dataKey, Data dataNewValue, Data dataOldValue, int eventType) {
        this.eventType = eventType;
        this.dataKey = dataKey;
        this.dataNewValue = dataNewValue;
        this.dataOldValue = dataOldValue;
    }

    public Data getDataKey() {
        return dataKey;
    }

    public Data getDataNewValue() {
        return dataNewValue;
    }

    public Data getDataOldValue() {
        return dataOldValue;
    }

    /**
     * Return the integer defining the event type. It can be turned into an
     * enum value by calling {@link com.hazelcast.core.EntryEventType#getByType(int)}.
     *
     * @return the integer defining the event type
     */
    public int getEventType() {
        return eventType;
    }
}
