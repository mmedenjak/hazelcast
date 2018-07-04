package com.hazelcast.spi;

import com.hazelcast.config.OOMEProtectionConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.MemoryInfoAccessor;
import com.hazelcast.util.RuntimeMemoryInfoAccessor;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.memory.MemorySize.toPrettyString;
import static com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl.getPartitionThreadId;
import static java.lang.String.format;
import static java.lang.System.getProperty;


public class MemoryChecker implements Runnable {
    // fields
    private static final double ONE_HUNDRED_PERCENT = 100D;
    private final MemoryInfoAccessor memoryInfoAccessor = getMemoryInfoAccessor();
    private final ILogger logger;
    private final int partitionThreadCount;
    private final int checkerPeriod;
    private final boolean enabled;
    private final double minFreePercentage;
    private final double evictPercentage;
    private long totalMemory;
    private long freeMemory;
    private long maxMemory;
    private AtomicLong evict = new AtomicLong();
    private volatile String serviceName;
    private volatile int partitionId;

    public MemoryChecker(NodeEngineImpl nodeEngine) {
        final OOMEProtectionConfig oomeProtectionConfig = nodeEngine.getConfig().getOomeProtectionConfig();
        checkerPeriod = oomeProtectionConfig.getCheckerPeriod();
        enabled = oomeProtectionConfig.isEnabled();
        minFreePercentage = oomeProtectionConfig.getMinFreePercentage();
        evictPercentage = oomeProtectionConfig.getEvictPercentage();
        this.logger = nodeEngine.getLogger(this.getClass());
        this.partitionThreadCount = nodeEngine.getOperationService().getPartitionThreadCount();
        if (enabled && checkerPeriod > 0) {
            nodeEngine.getExecutionService().schedule(this, checkerPeriod, TimeUnit.SECONDS);
        }
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

    public boolean needsEviction(String callerServiceName, int callerPartitionId) {
        if (!enabled) {
            return false;
        }

        if (checkerPeriod <= 0) {
            run();
            this.serviceName = callerServiceName;
            this.partitionId = callerPartitionId;
        }
        return evict.get() > 0
                && (this.serviceName == null || this.serviceName.equals(callerServiceName))
                && (callerPartitionId > -1 && samePartitionThread(callerPartitionId));
    }

    private boolean samePartitionThread(int partitionId) {
        final boolean evictAnyPartition = this.partitionId < 0;
        return evictAnyPartition || samePartitionThread(partitionId, this.partitionId);
    }

    public boolean samePartitionThread(int partitionId1, int partitionId2) {
        return getPartitionThreadId(partitionId1, partitionThreadCount)
                == getPartitionThreadId(partitionId2, partitionThreadCount);
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
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

            if (minFreePercentage > actualFreePercentage) {
                this.evict.set((long) ((evictPercentage / ONE_HUNDRED_PERCENT) * this.maxMemory));
                logger.warning(format(
                        "Running node memory eviction - runtime.max=%s, runtime.used=%s, configuredFree%%=%.2f, actualFree%%=%.2f",
                        toPrettyString(maxMemory),
                        toPrettyString(totalMemory - freeMemory),
                        minFreePercentage,
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
