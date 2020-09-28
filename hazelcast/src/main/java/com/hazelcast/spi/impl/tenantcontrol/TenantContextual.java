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

package com.hazelcast.spi.impl.tenantcontrol;

import com.hazelcast.spi.tenantcontrol.TenantControl;
import com.hazelcast.spi.tenantcontrol.TenantControl.Closeable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Represents a value that requires tenant control context to be accessed
 *
 * @author lprimak
 * @param <T> object type
 */
public class TenantContextual<T> {
    private static final TenantContextual<?> noop;

    private T contextual;
    private volatile boolean initialized;
    private final Supplier<T> initFunction;
    private final Supplier<Boolean> existsFunction;
    private final TenantControl tenantControl;
    private final Lock lock = new ReentrantLock();


    @SuppressWarnings("unchecked")
    static public<T> TenantContextual<T> create(Supplier<T> initFunction, Supplier<Boolean> existsFunction, TenantControl tenantControl) {
        if (tenantControl == TenantControl.NOOP_TENANT_CONTROL && !existsFunction.get()) {
            return (TenantContextual<T>) noop;
        } else {
            return new TenantContextual<>(initFunction, existsFunction, tenantControl);
        }
    }

    static {
        noop = new TenantContextual<>(() -> null, () -> false, new NoopTenantControl());
    }

    private TenantContextual(Supplier<T> initFunction, Supplier<Boolean> existsFunction, TenantControl tenantControl) {
        this.initFunction = initFunction;
        this.existsFunction = existsFunction;
        this.tenantControl = tenantControl;
    }

    /**
     *
     * @return underlying object, initialize within Tenant Control when necessary
     */
    public T get() {
        boolean localInitialized = this.initialized;
        if (!localInitialized) {
            lock.lock();
            try {
                if (!initialized) {
                    if (exists()) {
                        try (Closeable tenantContext = tenantControl.setTenant()) {
                            contextual = initFunction.get();
                        }
                    }
                    initialized = true;
                }
            } finally {
                lock.unlock();
            }
        }
        return contextual;
    }

    /**
     * could be called isNull() for the underlying object
     *
     * @return true if the underlying object exists (it not null)
     */
    public Boolean exists() {
        return existsFunction.get();
    }

    public TenantControl getTenantControl() {
        return tenantControl;
    }

    /**
     * method to return delegate having same contextual and creation methods
     * as the current class
     *
     * @param delegate
     * @return newly-created delegate
     */
    public TenantContextual<T> delegate(T delegate) {
        if (this == noop) {
            return this;
        }
        TenantContextual<T> newContextual = new TenantContextual<>(initFunction, existsFunction, tenantControl);
        newContextual.initialized = true;
        newContextual.contextual = delegate;
        return newContextual;
    }
}
