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
import org.apache.nifi.registry.flow.VersionedFlow;
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
 * Result for a list of VersionedFlows.
 */
public class VersionedFlowsResult extends AbstractWritableResult<List<VersionedFlow>> implements Referenceable {

    private final List<VersionedFlow> versionedFlows;

    public VersionedFlowsResult(final ResultType resultType, final List<VersionedFlow> flows) {
        super(resultType);
        this.versionedFlows = flows;
        Validate.notNull(this.versionedFlows);

        // NOTE: it is important that the order the flows are printed is the same order for the ReferenceResolver
        this.versionedFlows.sort(Comparator.comparing(VersionedFlow::getName));
    }

    @Override
    public List<VersionedFlow> getResult() {
        return versionedFlows;
    }

    @Override
    protected void writeSimpleResult(PrintStream output) {
        if (versionedFlows.isEmpty()) {
            return;
        }

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 20, 36, true)
                .column("Id", 36, 36, false)
                .column("Description", 11, 40, true)
                .build();

        for (int i = 0; i < versionedFlows.size(); ++i) {
            final VersionedFlow flow = versionedFlows.get(i);
            table.addRow(String.valueOf(i + 1), flow.getName(), flow.getIdentifier(), flow.getDescription());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public ReferenceResolver createReferenceResolver(final Context context) {
        final Map<Integer,VersionedFlow> backRefs = new HashMap<>();
        final AtomicInteger position = new AtomicInteger(0);
        versionedFlows.forEach(f -> backRefs.put(position.incrementAndGet(), f));

        return new ReferenceResolver() {
            @Override
            public ResolvedReference resolve(final CommandOption option, final Integer position) {
                final VersionedFlow versionedFlow = backRefs.get(position);
                if (versionedFlow != null) {
                    if (option != null && option == CommandOption.BUCKET_ID) {
                        return new ResolvedReference(option, position, versionedFlow.getBucketName(), versionedFlow.getBucketIdentifier());
                    } else {
                        return new ResolvedReference(option, position, versionedFlow.getName(), versionedFlow.getIdentifier());
                    }
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
