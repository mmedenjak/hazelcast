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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.PNCounterAddCodec;
import com.hazelcast.client.impl.protocol.codec.PNCounterGetCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.crdt.pncounter.PNCounter;

/**
 * Client proxy implementation for a {@link PNCounter}.
 */
public class ClientPNCounterProxy extends ClientProxy implements PNCounter {

    /**
     * Creates a client {@link PNCounter} proxy
     *
     * @param serviceName the service name
     * @param objectName  the PNCounter name
     * @param context     the client context containing references to services
     *                    and configuration
     */
    public ClientPNCounterProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);
    }

    @Override
    public String toString() {
        return "PNCounter{name='" + name + "\'}";
    }

    @Override
    public long get() {
        final ClientMessage request = PNCounterGetCodec.encodeRequest(name);
        final ClientMessage response = invoke(request);
        final PNCounterGetCodec.ResponseParameters resultParameters = PNCounterGetCodec.decodeResponse(response);
        return resultParameters.response;
    }

    @Override
    public long getAndAdd(long delta) {
        return invokeAdd(delta, true);
    }

    @Override
    public long addAndGet(long delta) {
        return invokeAdd(delta, false);
    }

    @Override
    public long getAndSubtract(long delta) {
        return invokeAdd(-delta, true);
    }

    @Override
    public long subtractAndGet(long delta) {
        return invokeAdd(-delta, false);
    }

    @Override
    public long decrementAndGet() {
        return invokeAdd(-1, false);
    }

    @Override
    public long incrementAndGet() {
        return invokeAdd(1, false);
    }

    @Override
    public long getAndDecrement() {
        return invokeAdd(-1, true);
    }

    @Override
    public long getAndIncrement() {
        return invokeAdd(1, true);
    }

    /**
     * Invokes a {@link ClientMessage} to add a delta to the counter value and
     * returns the counter value before the update if {@code getBeforeUpdate}
     * is {@code true}, otherwise returns the counter value after the update.
     *
     * @param delta           the delta to add to the counter value, can be negative
     * @param getBeforeUpdate {@code true} if the operation should return the
     *                        counter value before the addition, {@code false}
     *                        if it should return the value after the addition
     */
    private long invokeAdd(long delta, boolean getBeforeUpdate) {
        final ClientMessage request = PNCounterAddCodec.encodeRequest(name, delta, getBeforeUpdate);
        final ClientMessage response = invoke(request);
        final PNCounterAddCodec.ResponseParameters resultParameters = PNCounterAddCodec.decodeResponse(response);
        return resultParameters.response;
    }
}
