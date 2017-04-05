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

package com.hazelcast.query;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.BinaryInterface;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.PredicateDataSerializerHook;
import com.hazelcast.util.IterationType;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.PREDICATE_DS_FACTORY_ID;

@BinaryInterface
public class PartitionIteratingPredicate<K, V> implements IndexAwarePredicate<K, V>, IdentifiedDataSerializable {

    private Predicate<K, V> predicate;
    private int pageSize;
    private int lastTableIndex;
    private IterationType iterationType;
    private int partitionId;

    public PartitionIteratingPredicate() {
    }

    public PartitionIteratingPredicate(Predicate<K, V> predicate, int pageSize, int partitionId) {
        setInnerPredicate(predicate);
        this.lastTableIndex = Integer.MAX_VALUE;
        this.pageSize = pageSize;
        this.partitionId = partitionId;
    }

    private void setInnerPredicate(Predicate<K, V> predicate) {
        if (predicate instanceof PartitionIteratingPredicate) {
            throw new IllegalArgumentException("Nested PagingPredicate is not supported!");
        }
        this.predicate = predicate;
    }

    @Override
    public Set<QueryableEntry<K, V>> filter(QueryContext queryContext) {
//        if (!(predicate instanceof IndexAwarePredicate)) {
//            return null;
//        }
//
//
//        Set<QueryableEntry<K, V>> set = ((IndexAwarePredicate<K, V>) predicate).filter(queryContext);
//        if (set == null || set.isEmpty()) {
//            return null;
//        }
//        List<QueryableEntry<K, V>> resultList = new ArrayList<QueryableEntry<K, V>>();
//        Map.Entry<Integer, Map.Entry> nearestAnchorEntry = getNearestAnchorEntry();
//        for (QueryableEntry<K, V> queryableEntry : set) {
//            if (SortingUtil.compareAnchor(this, queryableEntry, nearestAnchorEntry)) {
//                resultList.add(queryableEntry);
//            }
//        }
//
//        List<QueryableEntry<K, V>> sortedSubList =
//                (List) SortingUtil.getSortedSubList((List) resultList, this, nearestAnchorEntry);
//        return new LinkedHashSet<QueryableEntry<K, V>>(sortedSubList);
        return null;
    }


    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return predicate instanceof IndexAwarePredicate && ((IndexAwarePredicate) predicate).isIndexed(queryContext);
    }

    @Override
    public boolean apply(Map.Entry<K, V> mapEntry) {
        return predicate == null || predicate.apply(mapEntry);
    }

    public void reset() {
        iterationType = null;
        lastTableIndex = Integer.MAX_VALUE;
    }

    public IterationType getIterationType() {
        return iterationType;
    }

    public void setIterationType(IterationType iterationType) {
        this.iterationType = iterationType;
    }

    public int getPageSize() {
        return pageSize;
    }

    public Predicate<K, V> getPredicate() {
        return predicate;
    }

    public int getLastTableIndex() {
        return lastTableIndex;
    }

    public void setLastTableIndex(int lastTableIndex) {
        this.lastTableIndex = lastTableIndex;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(predicate);
        out.writeInt(pageSize);
        out.writeInt(lastTableIndex);
        out.writeUTF(iterationType.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        predicate = in.readObject();
        pageSize = in.readInt();
        lastTableIndex = in.readInt();
        iterationType = IterationType.valueOf(in.readUTF());
    }

    @Override
    public int getFactoryId() {
        return PREDICATE_DS_FACTORY_ID;
    }

    @Override
    public int getId() {
        return PredicateDataSerializerHook.ITERATING_PREDICATE;
    }

    public int getPartitionId() {
        return partitionId;
    }
}
