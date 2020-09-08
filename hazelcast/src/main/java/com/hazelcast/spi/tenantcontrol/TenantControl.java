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

package com.hazelcast.spi.tenantcontrol;

import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.annotation.Beta;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.tenantcontrol.NoopTenantControl;

/**
 * Hooks for multi-tenancy for application servers
 * Hazelcast uses separate threads to invoke operations
 * this interface acts a hook to establish a thread-local tenant context
 * so that operation invocations into application servers are handled correctly
 * This is used by application servers to establish thread context for class loading,
 * CDI, EJB and JPA invocations
 *
 * @author lprimak
 */
@Beta
public interface TenantControl extends DataSerializable {
    /**
     * Default no-op tenant control
     */
    TenantControl NOOP_TENANT_CONTROL = new NoopTenantControl();

    /**
     * Establish this tenant's thread-local context
     * Particular TenantControl implementation will control the details of how
     *
     * @return handle to be able to close the tenant's scope.
     */
    Closeable setTenant();

    /**
     * To be called when Hazelcast object is created
     * @param destroyEventContext hook to decouple any Hazelcast object when the tenant is destroyed,
     * This is used, for example, to delete all associated caches from the application when
     * it gets undeployed, so there are no ClassCastExceptions afterwards
     * can be null if no context is necessary
     */
    void distributedObjectCreated(DestroyEventContext destroyEventContext);

    /**
     * To be called when the Hazelcast object attached to this tenant is destroyed.
     * The implementor may unregister it's own event listeners here.
     * This is used with conjunction with DestroyEvent, because
     * the listeners are probably used to call the DestroyEvent,
     * this just acts as the other event that will decouple
     * Hazelcast object from the tenant
     * This is so the TenantControl itself can be garbage collected
     */
    void distributedObjectDestroyed();

    /**
     * Checks if tenant app is loaded at the current time and classes are available
     *
     * @param op passed so the tenant can filter on who is calling
     * @return true if tenant is loaded and classes are available
     */
    boolean isAvailable(Operation op);

    /**
     * clean up the thread to avoid potential class loader leaks
     */
    void clearThreadContext();

    /**
     * same to Java's Closeable interface, except close() method does not throw IOException
     */
    interface Closeable extends AutoCloseable {
        /**
         * Same as Java's close() except no exception is thrown
         */
        @Override
        void close();
    }
}
