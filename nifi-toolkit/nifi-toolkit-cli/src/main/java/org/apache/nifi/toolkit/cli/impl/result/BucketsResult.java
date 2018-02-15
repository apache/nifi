/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.toolkit.cli.impl.result;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.ReferenceResolver;
import org.apache.nifi.toolkit.cli.api.Referenceable;
import org.apache.nifi.toolkit.cli.api.ResolvedReference;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;

import java.io.PrintStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Result for a list of buckets.
 */
public class BucketsResult extends AbstractWritableResult<List<Bucket>> implements Referenceable {

    private final List<Bucket> buckets;

    public BucketsResult(final ResultType resultType, final List<Bucket> buckets) {
        super(resultType);
        this.buckets = buckets;
        Validate.notNull(buckets);

        // NOTE: it is important that the order the buckets are printed is the same order for the ReferenceResolver
        this.buckets.sort(Comparator.comparing(Bucket::getName));
    }

    @Override
    public List<Bucket> getResult() {
        return buckets;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        if (buckets.isEmpty()) {
            return;
        }

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 20, 36, true)
                .column("Id", 36, 36, false)
                .column("Description", 11, 40, true)
                .build();

        for (int i = 0; i < buckets.size(); ++i) {
            final Bucket bucket = buckets.get(i);
            table.addRow(String.valueOf(i + 1), bucket.getName(), bucket.getIdentifier(), bucket.getDescription());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public ReferenceResolver createReferenceResolver(final Context context) {
        final Map<Integer,Bucket> backRefs = new HashMap<>();
        final AtomicInteger position = new AtomicInteger(0);
        buckets.forEach(b -> backRefs.put(position.incrementAndGet(), b));

        return new ReferenceResolver() {
            @Override
            public ResolvedReference resolve(final CommandOption option, final Integer position) {
                final Bucket bucket = backRefs.get(position);
                if (bucket != null) {
                    return new ResolvedReference(option, position, bucket.getName(), bucket.getIdentifier());
                } else {
                    return null;
                }
            }

            @Override
            public boolean isEmpty() {
                return backRefs.isEmpty();
            }
        };
    }

}
