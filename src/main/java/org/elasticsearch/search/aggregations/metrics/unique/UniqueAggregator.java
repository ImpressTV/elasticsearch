/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.metrics.unique;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Set;
import gnu.trove.set.hash.THashSet;
import gnu.trove.map.hash.TLongObjectHashMap;

/**
 * Created by prixeus on 2015.08.25..
 */
public class UniqueAggregator extends NumericMetricsAggregator.SingleValue {
    private final ValuesSource.Bytes valuesSource;
    private SortedBinaryDocValues values;

    private TLongObjectHashMap<Set<BytesRef>> sets;
    private ValueFormatter formatter;

    public UniqueAggregator(String name, long estimatedBucketsCount, ValuesSource.Bytes valuesSource, @Nullable ValueFormatter formatter,
                         AggregationContext context, Aggregator parent) {
        super(name, estimatedBucketsCount,  context, parent);
        this.valuesSource = valuesSource;
        this.formatter = formatter;
        if (valuesSource != null) {
            sets = new TLongObjectHashMap<>();
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
         values = valuesSource.bytesValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        if (!sets.containsKey(owningBucketOrdinal)) {
            sets.put(owningBucketOrdinal, new THashSet<BytesRef>());
        }

        Set<BytesRef> set = sets.get(owningBucketOrdinal);
        values.setDocument(doc);
        for (int i = 0; i < values.count(); ++i) {
            set.add(values.valueAt(i).clone());
        }
    }

    @Override
    public double metric(long owningBucketOrd) {
        return valuesSource == null ? 0 : sets.get(owningBucketOrd).size();
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null) {
            return new InternalUnique(name, new THashSet<BytesRef>(), formatter);
        }
        assert owningBucketOrdinal < sets.size();
        return new InternalUnique(name, sets.get(owningBucketOrdinal), formatter);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalUnique(name, new THashSet<BytesRef>(), formatter);
    }

    public static class Factory extends ValuesSourceAggregatorFactory.LeafOnly<ValuesSource.Bytes> {

        public Factory(String name, ValuesSourceConfig<ValuesSource.Bytes> valuesSourceConfig) {
            super(name, InternalUnique.TYPE.name(), valuesSourceConfig);
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new UniqueAggregator(name, 0, null, config.formatter(), aggregationContext, parent);
        }

        @Override
        protected Aggregator create(ValuesSource.Bytes valuesSource, long expectedBucketsCount, AggregationContext aggregationContext,
                                    Aggregator parent) {
            return new UniqueAggregator(name, expectedBucketsCount, valuesSource, config.formatter(), aggregationContext, parent);
        }
    }

    @Override
    public void doClose() {
        //Releasables.close(sets);
    }
}
