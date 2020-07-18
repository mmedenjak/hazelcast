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

package com.hazelcast.cache.impl.tenantcontrol;

import com.hazelcast.cache.impl.CacheProxy;
import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.tenantcontrol.DestroyEventContext;


public class CacheDestroyEventContext implements DestroyEventContext {
    private final String cacheName;

    public CacheDestroyEventContext(String cacheName) {
        this.cacheName = cacheName;
    }

    @Override
    public void tenantUnavailable(HazelcastInstance instance) {
        CacheProxy cache = (CacheProxy) instance.getCacheManager().getCache(cacheName);
        cache.reSerializeCacheConfig();
        CacheService cacheService = (CacheService) cache.getService();
        CacheConfig cacheConfig = cacheService.getCacheConfig(cache.getPrefixedName());
        cacheService.reSerializeCacheConfig(cacheConfig);
    }
}
