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

package com.hazelcast.map.impl.query;

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntriesSegment;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.query.PagingPredicateAccessor.getNearestAnchorEntry;
import static com.hazelcast.util.SortingUtil.compareAnchor;
import static com.hazelcast.util.SortingUtil.getSortedSubList;

/**
 * Responsible for running a full-partition scna for a single partition in the calling thread.
 */
public class PartitionScanRunner {

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected final InternalSerializationService serializationService;
    protected final IPartitionService partitionService;
    protected final OperationService operationService;
    protected final ClusterService clusterService;

    public PartitionScanRunner(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
        this.operationService = nodeEngine.getOperationService();
        this.clusterService = nodeEngine.getClusterService();
    }

    @SuppressWarnings("unchecked")
    public Collection<QueryableEntry> run(String mapName, Predicate predicate, int partitionId) {
        PagingPredicate pagingPredicate = predicate instanceof PagingPredicate ? (PagingPredicate) predicate : null;
        List<QueryableEntry> resultList = new LinkedList<QueryableEntry>();

        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        Iterator<Record> iterator = partitionContainer.getRecordStore(mapName).loadAwareIterator(getNow(), false);
        Map.Entry<Integer, Map.Entry> nearestAnchorEntry = getNearestAnchorEntry(pagingPredicate);
        boolean useCachedValues = isUseCachedDeserializedValuesEnabled(mapContainer, partitionId);
        Extractors extractors = mapServiceContext.getExtractors(mapName);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data key = (Data) toData(record.getKey());
            Object value = toData(
                    useCachedValues ? Records.getValueOrCachedValue(record, serializationService) : record.getValue());
            if (value == null) {
                continue;
            }
            //we want to always use CachedQueryEntry as these are short-living objects anyway
            QueryableEntry queryEntry = new LazyMapEntry(key, value, serializationService, extractors);

            if (predicate.apply(queryEntry) && compareAnchor(pagingPredicate, queryEntry, nearestAnchorEntry)) {
                resultList.add(queryEntry);
            }
        }
        return getSortedSubList(resultList, pagingPredicate, nearestAnchorEntry);
    }

    /**
     * Executes the predicate on a partition chunk. The offset in the partition
     * is defined by the {@code pointers} and the soft limit is defined by the
     * {@code fetchSize}. The method returns the matched entries and updated
     * pointers from which new entries can be fetched which allows for efficient
     * iteration of query results.
     * <p>
     * <b>NOTE</b>
     * The iteration may be done when the map is being mutated or when there are
     * membership changes. The iterator does not reflect the state when it has
     * been constructed - it may return some entries that were added after the
     * iteration has started and may not return some entries that were removed
     * after iteration has started.
     * The iterator will not, however, skip an entry if it has not been changed
     * and will not return an entry twice.
     *
     * @param mapName     the map name
     * @param predicate   the predicate which the entries must match
     * @param partitionId the partition which is queried
     * @param pointers    the pointers defining the state of iteration
     * @param fetchSize   the soft limit for the number of entries to fetch
     * @return entries matching the predicate and a table index from which new
     * entries can be fetched
     */
    public QueryableEntriesSegment run(String mapName, Predicate predicate, int partitionId,
                                       IterationPointer[] pointers, int fetchSize) {
        final List<QueryableEntry> resultList = new LinkedList<QueryableEntry>();
        final PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        final RecordStore recordStore = partitionContainer.getRecordStore(mapName);
        final Extractors extractors = mapServiceContext.getExtractors(mapName);

        while (resultList.size() < fetchSize && pointers[pointers.length - 1].getIndex() >= 0) {
            final MapEntriesWithCursor cursor = recordStore.fetchEntries(pointers, fetchSize - resultList.size());
            pointers = cursor.getIterationPointers();
            final Collection<? extends Entry<Data, Data>> entries = cursor.getBatch();
            if (entries.isEmpty()) {
                break;
            }
            for (Entry<Data, Data> entry : entries) {
                QueryableEntry queryEntry = new LazyMapEntry(entry.getKey(), entry.getValue(), serializationService, extractors);
                if (predicate.apply(queryEntry)) {
                    resultList.add(queryEntry);
                }
            }
        }
        return new QueryableEntriesSegment(resultList, pointers);
    }

    protected boolean isUseCachedDeserializedValuesEnabled(MapContainer mapContainer, int partitionId) {
        CacheDeserializedValues cacheDeserializedValues = mapContainer.getMapConfig().getCacheDeserializedValues();
        switch (cacheDeserializedValues) {
            case NEVER:
                return false;
            case ALWAYS:
                return true;
            default:
                //if index exists then cached value is already set -> let's use it
                return mapContainer.getIndexes(partitionId).hasIndex();
        }
    }

    protected <T> Object toData(T input) {
        return input;
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }
}
