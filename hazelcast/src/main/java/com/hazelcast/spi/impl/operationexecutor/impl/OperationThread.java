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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.metrics.ExcludedMetricTargets;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.internal.util.executor.HazelcastManagedThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_DISCRIMINATOR_THREAD;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_THREAD_COMPLETED_OPERATION_BATCH_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_THREAD_COMPLETED_OPERATION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_THREAD_COMPLETED_PACKET_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_THREAD_COMPLETED_PARTITION_SPECIFIC_RUNNABLE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_THREAD_COMPLETED_RUNNABLE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_THREAD_COMPLETED_TOTAL_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_THREAD_ERROR_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_PREFIX_THREAD;
import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

/**
 * The OperationThread is responsible for processing operations, packets
 * containing operations and runnable's.
 * <p>
 * There are 2 flavors of OperationThread:
 * - threads that deal with operations for a specific partition
 * - threads that deal with non partition specific tasks
 * <p>
 * The actual processing of an operation is forwarded to the {@link OperationRunner}.
 */
@ExcludedMetricTargets(MANAGEMENT_CENTER)
public abstract class OperationThread extends HazelcastManagedThread implements StaticMetricsProvider {

    final int threadId;
    final OperationQueue queue;
    // This field wil only be accessed by the thread itself when doing 'self'
    // calls. So no need for any form of synchronization.
    OperationRunner currentRunner;

    // All these counters are updated by this OperationThread (so a single writer)
    // and are read by the MetricsRegistry.
    @Probe(name = OPERATION_METRIC_THREAD_COMPLETED_TOTAL_COUNT)
    private final SwCounter completedTotalCount = newSwCounter();
    @Probe(name = OPERATION_METRIC_THREAD_COMPLETED_PACKET_COUNT)
    private final SwCounter completedPacketCount = newSwCounter();
    @Probe(name = OPERATION_METRIC_THREAD_COMPLETED_OPERATION_COUNT)
    private final SwCounter completedOperationCount = newSwCounter();
    @Probe(name = OPERATION_METRIC_THREAD_COMPLETED_PARTITION_SPECIFIC_RUNNABLE_COUNT)
    private final SwCounter completedPartitionSpecificRunnableCount = newSwCounter();
    @Probe(name = OPERATION_METRIC_THREAD_COMPLETED_RUNNABLE_COUNT)
    private final SwCounter completedRunnableCount = newSwCounter();
    @Probe(name = OPERATION_METRIC_THREAD_ERROR_COUNT)
    private final SwCounter errorCount = newSwCounter();
    @Probe(name = OPERATION_METRIC_THREAD_COMPLETED_OPERATION_BATCH_COUNT)
    private final SwCounter completedOperationBatchCount = newSwCounter();

    private final boolean priority;
    private final NodeExtension nodeExtension;
    private final ILogger logger;
    private volatile boolean shutdown;

    public OperationThread(String name,
                           int threadId,
                           OperationQueue queue,
                           ILogger logger,
                           NodeExtension nodeExtension,
                           boolean priority,
                           ClassLoader configClassLoader) {
        super(name);
        setContextClassLoader(configClassLoader);
        this.queue = queue;
        this.threadId = threadId;
        this.logger = logger;
        this.nodeExtension = nodeExtension;
        this.priority = priority;
    }

    public int getThreadId() {
        return threadId;
    }

    public abstract OperationRunner operationRunner(int partitionId);

    @Override
    public final void executeRun() {
        nodeExtension.onThreadStart(this);
        try {
            while (!shutdown) {
                Object task;
                try {
                    task = queue.take(priority);
                } catch (InterruptedException e) {
                    continue;
                }

                process(task);
            }
        } catch (Throwable t) {
            inspectOutOfMemoryError(t);
            logger.severe(t);
        } finally {
            nodeExtension.onThreadStop(this);
        }
    }

    private void process(Object task) {
        try {
            SwCounter counter;
            // process() will return it's appropriate counter to be incremented
            // if process() returns null instead, which means that task's tenant
            // is not available, the task will be put in the back of the queue
            // this, however, conflates the meaning of SwCounter return value
            // design choice needed to be made, otherwise there would be a lot of
            // code duplication. Wishing Java had tuples (although they bring their own problems)
            if (task.getClass() == Packet.class) {
                counter = process((Packet) task);
            } else if (task instanceof Operation) {
                counter = process((Operation) task);
            } else if (task instanceof PartitionSpecificRunnable) {
                counter = process((PartitionSpecificRunnable) task);
            } else if (task instanceof Runnable) {
                counter = process((Runnable) task);
            } else if (task instanceof TaskBatch) {
                counter = process((TaskBatch) task);
            } else {
                throw new IllegalStateException("Unhandled task:" + task);
            }
            if (counter != null) {
                counter.inc();
                completedTotalCount.inc();
            } else {
                // retry later if not ready
                queue.add(task, priority);
            }
        } catch (Throwable t) {
            errorCount.inc();
            inspectOutOfMemoryError(t);
            logger.severe("Failed to process: " + task + " on: " + getName(), t);
        } finally {
            currentRunner = null;
        }
    }

    private SwCounter process(Operation operation) {
        currentRunner = operationRunner(operation.getPartitionId());
        try {
            return currentRunner.run(operation) ? null : completedOperationCount;
        } finally {
            operation.clearThreadContext();
        }
    }

    private SwCounter process(Packet packet) throws Exception {
        currentRunner = operationRunner(packet.getPartitionId());
        return currentRunner.run(packet) ? null : completedPacketCount;
    }

    private SwCounter process(PartitionSpecificRunnable runnable) {
        currentRunner = operationRunner(runnable.getPartitionId());
        currentRunner.run(runnable);
        return completedPartitionSpecificRunnableCount;
    }

    private SwCounter process(Runnable runnable) {
        runnable.run();
        return completedRunnableCount;
    }

    private SwCounter process(TaskBatch batch) {
        Object task = batch.next();
        if (task == null) {
            return completedOperationBatchCount;
        }

        SwCounter counter = null;
        try {
            if (task instanceof Operation) {
                counter = process((Operation) task);
            } else if (task instanceof Runnable) {
                counter = process((Runnable) task);
            } else {
                throw new IllegalStateException("Unhandled task: " + task + " from " + batch.taskFactory());
            }
            if (counter == null) {
                queue.add(task, false);
            } else {
                counter.inc();
            }
        } finally {
            queue.add(batch, false);
        }
        return null;
    }

    @Override
    public void provideStaticMetrics(MetricsRegistry registry) {
        MetricDescriptor descriptor = registry
                .newMetricDescriptor()
                .withPrefix(OPERATION_PREFIX_THREAD)
                .withDiscriminator(OPERATION_DISCRIMINATOR_THREAD, getName());
        registry.registerStaticMetrics(descriptor, this);
    }

    public final void shutdown() {
        shutdown = true;
        interrupt();
    }

    public final void awaitTermination(int timeout, TimeUnit unit) throws InterruptedException {
        join(unit.toMillis(timeout));
    }
}
