package com.hazelcast.spi;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.MemoryInfoAccessor;
import com.hazelcast.util.RuntimeMemoryInfoAccessor;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.memory.MemorySize.toPrettyString;
import static java.lang.String.format;
import static java.lang.System.getProperty;


public class MemoryChecker implements Runnable {
    public static final double MIN_FREE_PERCENTAGE = 10;
    public static final double EVICTION_PERCENTAGE = 20;
    protected static final double ONE_HUNDRED_PERCENT = 100D;
    protected final MemoryInfoAccessor memoryInfoAccessor = getMemoryInfoAccessor();
    private final ILogger logger;
    private long totalMemory;
    private long freeMemory;
    private long maxMemory;
    private AtomicLong evict = new AtomicLong();

    public MemoryChecker(NodeEngineImpl nodeEngine) {
        nodeEngine.getExecutionService().schedule(this, 1, TimeUnit.SECONDS);
        logger = nodeEngine.getLogger(this.getClass());
    }

    protected static MemoryInfoAccessor getMemoryInfoAccessor() {
        MemoryInfoAccessor pluggedMemoryInfoAccessor = getPluggedMemoryInfoAccessor();
        return pluggedMemoryInfoAccessor != null ? pluggedMemoryInfoAccessor : new RuntimeMemoryInfoAccessor();
    }

    private static MemoryInfoAccessor getPluggedMemoryInfoAccessor() {
        String memoryInfoAccessorImpl = getProperty("hazelcast.memory.info.accessor.impl");
        if (memoryInfoAccessorImpl == null) {
            return null;
        }

        try {
            return ClassLoaderUtil.newInstance(null, memoryInfoAccessorImpl);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    public long evict(long bytes) {
        return evict.addAndGet(-bytes);
    }

    public boolean needsEviction() {
        return evict.get() > 0;
    }

    @Override
    public void run() {
        final long totalMemory = getTotalMemory();
        final long freeMemory = getFreeMemory();
        final long maxMemory = getMaxMemory();
        if (totalMemory == this.totalMemory && freeMemory == this.freeMemory && maxMemory == this.maxMemory) {
            return;
        }

        this.totalMemory = totalMemory;
        this.freeMemory = freeMemory;
        this.maxMemory = maxMemory;
        long availableMemory = this.freeMemory + (this.maxMemory - this.totalMemory);

        if (this.totalMemory > 0 && this.freeMemory > 0 && this.maxMemory > 0 && availableMemory > 0) {
            double actualFreePercentage = ONE_HUNDRED_PERCENT * availableMemory / this.maxMemory;

            if (MIN_FREE_PERCENTAGE > actualFreePercentage) {
                this.evict.set((long) ((EVICTION_PERCENTAGE / ONE_HUNDRED_PERCENT) * this.maxMemory));
            }

            if (logger.isFinestEnabled()) {
                logger.finest(format(
                        "Running node memory eviction - runtime.max=%s, runtime.used=%s, configuredFree%%=%.2f, actualFree%%=%.2f",
                        toPrettyString(maxMemory),
                        toPrettyString(totalMemory - freeMemory),
                        MIN_FREE_PERCENTAGE,
                        actualFreePercentage));
            }
        }
    }


    protected long getTotalMemory() {
        return memoryInfoAccessor.getTotalMemory();
    }

    protected long getFreeMemory() {
        return memoryInfoAccessor.getFreeMemory();
    }

    protected long getMaxMemory() {
        return memoryInfoAccessor.getMaxMemory();
    }
}
