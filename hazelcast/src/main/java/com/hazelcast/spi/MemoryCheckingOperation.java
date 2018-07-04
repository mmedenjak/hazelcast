/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.MemorySize;

public class MemoryCheckingOperation extends Operation {

    public CallStatus call() throws Exception {
        yield();
        return super.call();
    }

    public void yield() {
        final MemoryChecker checker = getNodeEngine().getMemoryChecker();
        final String serviceName = getServiceName();
        final int partitionId = this.getPartitionId();
        long totalEvicted = 0;
        if (checker.needsEviction(serviceName, partitionId)) {
            final ILogger logger = getLogger();
            for (EvictionSupportingService service : getNodeEngine().getServices(EvictionSupportingService.class)) {
                final long evicted = service.evict(checker, partitionId);
                totalEvicted += evicted;
            }
            final boolean success = checker.needsEviction(serviceName, partitionId);
            if (totalEvicted > 0) {
                logger.info("Eviction "
                        + (success ? "" : "not ")
                        + "complete for service " + serviceName + " and partition " + partitionId + " with evicted "
                        + MemorySize.toPrettyString(totalEvicted));
            }
        }
    }
}
