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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * Created by prixeus on 2015.08.25..
 */
public class UniqueParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalUnique.TYPE.name();
    }

    @Override
    public AggregatorFactory parse(String name, XContentParser parser, SearchContext context) throws IOException {
        ValuesSourceParser valuesSourceParser = ValuesSourceParser.bytes(name, InternalUnique.TYPE, context).formattable(true).build();

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (!valuesSourceParser.token(currentFieldName, token, parser)) {
                throw new SearchParseException(context, "Unexpected token " + token + " in [" + name + "].");
            }
        }

        return new ValuesSourceAggregatorFactory<ValuesSource.Bytes>(name, InternalUnique.TYPE.name(), valuesSourceParser.config()) {
            protected Aggregator create(ValuesSource.Bytes valueSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
                return new UniqueAggregator(name, expectedBucketsCount, valueSource, config.formatter(), aggregationContext, parent);
            }

            @Override
            protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
                return new UniqueAggregator(name, 0, null, config.formatter(), aggregationContext, parent);
            }
        };
    }
}
