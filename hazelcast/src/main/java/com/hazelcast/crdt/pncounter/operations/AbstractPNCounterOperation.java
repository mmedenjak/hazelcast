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

package com.hazelcast.crdt.pncounter.operations;

import com.hazelcast.crdt.CRDTDataSerializerHook;
import com.hazelcast.crdt.pncounter.PNCounterImpl;
import com.hazelcast.crdt.pncounter.PNCounterService;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;

import static com.hazelcast.crdt.pncounter.PNCounterService.SERVICE_NAME;

/**
 * Base class for {@link com.hazelcast.crdt.pncounter.PNCounter} query and
 * mutation operation implementations. It will throw an exception on all
 * serialization and deserialization invocations as CRDT operations must
 * be invoked locally on a member.
 */
public abstract class AbstractPNCounterOperation extends Operation implements IdentifiedDataSerializable {
    protected String name;
    private PNCounterImpl counter;

    AbstractPNCounterOperation() {
    }

    AbstractPNCounterOperation(String name) {
        this.name = name;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    PNCounterImpl getPNCounter() {
        if (counter != null) {
            return counter;
        }
        final PNCounterService service = getService();
        this.counter = service.getCounter(name);
        return counter;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", name=").append(name);
    }

    @Override
    public int getFactoryId() {
        return CRDTDataSerializerHook.F_ID;
    }
}
