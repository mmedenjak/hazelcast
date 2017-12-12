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

package com.hazelcast.crdt.orset.operations;

import com.hazelcast.crdt.orset.ORSetImpl;
import com.hazelcast.crdt.orset.ORSetService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;

import static com.hazelcast.crdt.orset.ORSetService.SERVICE_NAME;

/**
 * Base class for {@link com.hazelcast.crdt.orset.ORSet} query and
 * mutation operation implementations. It will throw an exception on all
 * serialization and deserialization invocations as CRDT operations must
 * be invoked locally on a member.
 *
 * @param <E> the type of elements maintained by this set
 * @since 3.10
 */
public abstract class AbstractORSetOperation<E> extends Operation {
    protected String name;
    private ORSetImpl<E> set;

    AbstractORSetOperation(String name) {
        this.name = name;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    ORSetImpl<E> getORSet() {
        if (set != null) {
            return set;
        }
        final ORSetService service = getService();
        this.set = service.getSet(name);
        return set;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new IllegalStateException("Operation is intended to be run locally");
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new IllegalStateException("Operation is intended to be run locally");
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", name=").append(name);
    }
}
