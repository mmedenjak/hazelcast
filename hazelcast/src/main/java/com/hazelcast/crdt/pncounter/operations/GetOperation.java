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

/**
 * Query operation to retrieve the current value of the
 * {@link com.hazelcast.crdt.pncounter.PNCounter}.
 * The operation is meant to be invoked locally and will throw exceptions
 * when being (de)serialized.
 */
public class GetOperation extends AbstractPNCounterOperation {
    private long result;

    /**
     * Constructs the
     * @param name name of the {@link com.hazelcast.crdt.pncounter.PNCounter}
     */
    public GetOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        result = getPNCounter().get();
    }

    @Override
    public Long getResponse() {
        return result;
    }
}
