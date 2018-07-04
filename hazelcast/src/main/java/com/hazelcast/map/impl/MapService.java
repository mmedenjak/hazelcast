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

package com.hazelcast.map.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryView;
import com.hazelcast.internal.cluster.ClusterStateListener;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.map.impl.event.MapEventPublishingService;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.LazyEntryViewFromRecord;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.impl.recordstore.Storage;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ClientAwareService;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EvictionSupportingService;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemoryChecker;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.NotifiableEventListener;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.impl.CountingMigrationAwareService;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionLostEvent;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.map.impl.eviction.Evictor.SAMPLE_COUNT;

/**
 * Defines map service behavior.
 *
 * @see MapManagedService
 * @see MapMigrationAwareService
 * @see MapTransactionalService
 * @see MapRemoteService
 * @see MapEventPublishingService
 * @see MapPostJoinAwareService
 * @see MapSplitBrainHandlerService
 * @see MapReplicationSupportingService
 * @see MapStatisticsAwareService
 * @see MapPartitionAwareService
 * @see MapQuorumAwareService
 * @see MapClientAwareService
 * @see MapServiceContext
 */
public class MapService implements ManagedService, FragmentedMigrationAwareService,
        TransactionalService, RemoteService, EventPublishingService<Object, ListenerAdapter>,
        PostJoinAwareService, SplitBrainHandlerService, ReplicationSupportingService, StatisticsAwareService<LocalMapStats>,
        PartitionAwareService, ClientAwareService, QuorumAwareService, NotifiableEventListener, ClusterStateListener,
        ClusterVersionListener, EvictionSupportingService {

    public static final String SERVICE_NAME = "hz:impl:mapService";

    protected ManagedService managedService;
    protected CountingMigrationAwareService migrationAwareService;
    protected TransactionalService transactionalService;
    protected RemoteService remoteService;
    protected EventPublishingService eventPublishingService;
    protected PostJoinAwareService postJoinAwareService;
    protected SplitBrainHandlerService splitBrainHandlerService;
    protected ReplicationSupportingService replicationSupportingService;
    protected StatisticsAwareService statisticsAwareService;
    protected PartitionAwareService partitionAwareService;
    protected ClientAwareService clientAwareService;
    protected MapQuorumAwareService quorumAwareService;
    protected MapServiceContext mapServiceContext;
    // RU_COMPAT_V3_9
    protected MapIndexSynchronizer mapIndexSynchronizer;

    public MapService() {
    }

    @Override
    public void dispatchEvent(Object event, ListenerAdapter listener) {
        eventPublishingService.dispatchEvent(event, listener);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        managedService.init(nodeEngine, properties);
    }

    @Override
    public void reset() {
        managedService.reset();
    }

    @Override
    public void shutdown(boolean terminate) {
        managedService.shutdown(terminate);
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        return migrationAwareService.getAllServiceNamespaces(event);
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return migrationAwareService.isKnownServiceNamespace(namespace);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return migrationAwareService.prepareReplicationOperation(event);
    }

    private static int removeKeys(Queue<Data> keysToRemove, RecordStore recordStore, boolean backup) {
        int removedEntryCount = 0;

        while (!keysToRemove.isEmpty()) {
            Data keyToEvict = keysToRemove.poll();
            recordStore.evict(keyToEvict, backup);
            removedEntryCount++;
        }

        return removedEntryCount;
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        migrationAwareService.beforeMigration(event);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        migrationAwareService.commitMigration(event);
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        migrationAwareService.rollbackMigration(event);
    }

    @Override
    public Operation getPostJoinOperation() {
        return postJoinAwareService.getPostJoinOperation();
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return remoteService.createDistributedObject(objectName);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        remoteService.destroyDistributedObject(objectName);
        quorumAwareService.onDestroy(objectName);
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent replicationEvent) {
        replicationSupportingService.onReplicationEvent(replicationEvent);
    }

    @Override
    public void onPartitionLost(IPartitionLostEvent partitionLostEvent) {
        partitionAwareService.onPartitionLost(partitionLostEvent);
    }

    @Override
    public Runnable prepareMergeRunnable() {
        return splitBrainHandlerService.prepareMergeRunnable();
    }

    @Override
    public <T extends TransactionalObject> T createTransactionalObject(String name, Transaction transaction) {
        return transactionalService.createTransactionalObject(name, transaction);
    }

    @Override
    public void rollbackTransaction(String transactionId) {
        transactionalService.rollbackTransaction(transactionId);
    }

    @Override
    public Map<String, LocalMapStats> getStats() {
        return statisticsAwareService.getStats();
    }

    @Override
    public String getQuorumName(String name) {
        return quorumAwareService.getQuorumName(name);
    }

    public MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    @Override
    public void clientDisconnected(String clientUuid) {
        clientAwareService.clientDisconnected(clientUuid);
    }

    @Override
    public void onRegister(Object service, String serviceName, String topic, EventRegistration registration) {
        EventFilter filter = registration.getFilter();
        if (!(filter instanceof EventListenerFilter) || !filter.eval(INVALIDATION.getType())) {
            return;
        }

        MapContainer mapContainer = mapServiceContext.getMapContainer(topic);
        mapContainer.increaseInvalidationListenerCount();
    }

    @Override
    public void onDeregister(Object service, String serviceName, String topic, EventRegistration registration) {
        EventFilter filter = registration.getFilter();
        if (!(filter instanceof EventListenerFilter) || !filter.eval(INVALIDATION.getType())) {
            return;
        }

        MapContainer mapContainer = mapServiceContext.getMapContainer(topic);
        mapContainer.decreaseInvalidationListenerCount();
    }

    public int getMigrationStamp() {
        return migrationAwareService.getMigrationStamp();
    }

    public boolean validateMigrationStamp(int stamp) {
        return migrationAwareService.validateMigrationStamp(stamp);
    }

    @Override
    public void onClusterStateChange(ClusterState newState) {
        mapServiceContext.onClusterStateChange(newState);
    }

    @Override
    // RU_COMPAT_V3_9
    // We wont need to sync the indexes in 3.10+ clusters.
    public void onClusterVersionChange(Version newVersion) {
        mapIndexSynchronizer.onClusterVersionChange(newVersion);
    }

    public static ObjectNamespace getObjectNamespace(String mapName) {
        return new DistributedObjectNamespace(SERVICE_NAME, mapName);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        return migrationAwareService.prepareReplicationOperation(event, namespaces);
    }

    @Override
    public void evict(MemoryChecker checker, int runningThreadPartitionId) {
        final int partitionIdToEvict = checker.getPartitionId();
        if (partitionIdToEvict > 0) {
            if (checker.samePartitionThread(partitionIdToEvict, runningThreadPartitionId)) {
                for (RecordStore recordStore : mapServiceContext.getPartitionContainer(partitionIdToEvict).getMaps().values()) {
                    forceEvict(recordStore, checker);
                }
            }
        } else {
            NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
            int partitionCount = nodeEngine.getPartitionService().getPartitionCount();

            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                if (checker.samePartitionThread(partitionId, runningThreadPartitionId)) {
                    ConcurrentMap<String, RecordStore> maps = mapServiceContext.getPartitionContainer(partitionId).getMaps();
                    for (RecordStore recordStore : maps.values()) {
                        forceEvict(recordStore, checker);
                    }

                }
            }
        }

        checker.setServiceName(null);
        checker.setPartitionId(-1);
    }

    public void forceEvict(RecordStore recordStore, MemoryChecker checker) {
        if (recordStore.size() == 0) {
            return;
        }
        boolean backup = isBackup(recordStore);

        Storage<Data, Record> storage = recordStore.getStorage();
        Queue<Data> keysToRemove = new LinkedList<Data>();
        Iterable<LazyEntryViewFromRecord> samples = storage.getRandomSamples(SAMPLE_COUNT);

        for (LazyEntryViewFromRecord candidate : samples) {
            final long remainingEvict = checker.evict(candidate.getCost());
            final Record record = candidate.getRecord();


            Data key = record.getKey();
            if (!recordStore.isLocked(key)) {
                if (!backup) {
                    recordStore.doPostEvictionOperations(record, backup);
                }
                keysToRemove.add(key);
            }


            if (remainingEvict < 0){
                break;
            }
        }
        int removedKeyCount = removeKeys(keysToRemove, recordStore, backup);

        recordStore.disposeDeferredBlocks();
    }

    protected boolean isBackup(RecordStore recordStore) {
        int partitionId = recordStore.getPartitionId();
        IPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
        IPartition partition = partitionService.getPartition(partitionId, false);
        return !partition.isLocal();
    }


}
