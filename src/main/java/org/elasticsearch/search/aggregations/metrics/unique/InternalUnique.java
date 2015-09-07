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

import com.google.common.primitives.Bytes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.Set;
import gnu.trove.set.hash.THashSet;

/**
 * Created by prixeus on 2015.08.25..
 */
public class InternalUnique extends InternalNumericMetricsAggregation.SingleValue implements Unique {
    public final static Type TYPE = new Type("unique");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalUnique readResult(StreamInput in) throws IOException {
            InternalUnique result = new InternalUnique();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    //private double max;
    private Set<BytesRef> unique;

    InternalUnique() {} // for serialization

    public InternalUnique(String name, Set<BytesRef> unique, @Nullable ValueFormatter formatter) {
        super(name);
        this.valueFormatter = formatter;
        this.unique = unique;
    }

    @Override
    public double value() {
        return unique.size();
    }

    public double getValue() {
        return unique.size();
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalUnique reduce(ReduceContext reduceContext) {
        Set<BytesRef> all = new THashSet<>();
        for (InternalAggregation aggregation : reduceContext.aggregations()) {
            all.addAll(((InternalUnique) aggregation).unique);
        }
        return new InternalUnique(name, all, valueFormatter);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        int size = in.readInt();
        unique = new THashSet<>(size);
        for (int i = 0; i < size; ++i) {
            unique.add(in.readBytesRef());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeInt(unique.size());
        for (BytesRef e: unique) {
            out.writeBytesRef(e);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = !unique.isEmpty();
        builder.field(CommonFields.VALUE, hasValue ? unique.size() : null);
        if (hasValue && valueFormatter != null && !(valueFormatter instanceof ValueFormatter.Raw)) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(unique.size()));
        }
        return builder;
    }
}
