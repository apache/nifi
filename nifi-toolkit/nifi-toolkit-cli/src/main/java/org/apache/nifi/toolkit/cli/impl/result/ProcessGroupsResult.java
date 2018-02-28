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
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.ReferenceResolver;
import org.apache.nifi.toolkit.cli.api.Referenceable;
import org.apache.nifi.toolkit.cli.api.ResolvedReference;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;

import java.io.PrintStream;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Result for a list of ProcessGroupEntities.
 */
public class ProcessGroupsResult extends AbstractWritableResult<List<ProcessGroupDTO>> implements Referenceable {

    private final List<ProcessGroupDTO> processGroups;

    public ProcessGroupsResult(final ResultType resultType, final List<ProcessGroupDTO> processGroups) {
        super(resultType);
        this.processGroups = processGroups;
        Validate.notNull(this.processGroups);
        this.processGroups.sort(Comparator.comparing(ProcessGroupDTO::getName));
    }

    @Override
    public List<ProcessGroupDTO> getResult() {
        return processGroups;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {

        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Name", 20, 36, true)
                .column("Id", 36, 36, false)
                .column("Running", 7, 7, false)
                .column("Stopped", 7, 7, false)
                .column("Disabled", 8, 8, false)
                .column("Invalid", 7, 7, false)
                .build();

        for (int i=0; i < processGroups.size(); i++) {
            final ProcessGroupDTO dto = processGroups.get(i);
            table.addRow(
                    String.valueOf(i+1),
                    dto.getName(),
                    dto.getId(),
                    String.valueOf(dto.getRunningCount()),
                    String.valueOf(dto.getStoppedCount()),
                    String.valueOf(dto.getDisabledCount()),
                    String.valueOf(dto.getInvalidCount())
            );
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public ReferenceResolver createReferenceResolver(final Context context) {
        final Map<Integer,ProcessGroupDTO> backRefs = new HashMap<>();
        final AtomicInteger position = new AtomicInteger(0);
        processGroups.forEach(p -> backRefs.put(position.incrementAndGet(), p));

        return new ReferenceResolver() {
            @Override
            public ResolvedReference resolve(final CommandOption option, final Integer position) {
                final ProcessGroupDTO pg = backRefs.get(position);
                if (pg != null) {
                    return new ResolvedReference(option, position, pg.getName(), pg.getId());
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
