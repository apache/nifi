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
package org.apache.nifi.toolkit.cli.impl.result.nifi;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.toolkit.cli.api.Context;
import org.apache.nifi.toolkit.cli.api.ReferenceResolver;
import org.apache.nifi.toolkit.cli.api.Referenceable;
import org.apache.nifi.toolkit.cli.api.ResolvedReference;
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.command.CommandOption;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.toolkit.cli.impl.result.writer.DynamicTableWriter;
import org.apache.nifi.toolkit.cli.impl.result.writer.Table;
import org.apache.nifi.toolkit.cli.impl.result.writer.TableWriter;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.entity.ParameterContextsEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class ParamContextsResult extends AbstractWritableResult<ParameterContextsEntity> implements Referenceable {

    private final ParameterContextsEntity parameterContexts;
    private final List<ParameterContextDTO> results;

    public ParamContextsResult(final ResultType resultType, final ParameterContextsEntity parameterContexts) {
        super(resultType);
        this.parameterContexts = Validate.notNull(parameterContexts);
        this.results = new ArrayList<>();

        // If there is a param context that the user doesn't have permissions to then the entity will be returned with
        // a null component so we need to create a place holder DTO that has only the id populated
        Optional.ofNullable(parameterContexts.getParameterContexts()).orElse(Collections.emptySet())
                .forEach(pc -> {
                    if (pc.getComponent() == null) {
                        final ParameterContextDTO dto = new ParameterContextDTO();
                        dto.setId(pc.getId());
                        dto.setName(pc.getId());
                        results.add(dto);
                    } else {
                        results.add(pc.getComponent());
                    }
                });

        // NOTE: it is important that the order the contexts are printed is the same order for the ReferenceResolver
        Collections.sort(results, Comparator.comparing(ParameterContextDTO::getName));
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final Table table = new Table.Builder()
                .column("#", 3, 3, false)
                .column("Id", 36, 36, false)
                .column("Name", 20, 60, true)
                .column("Description", 40, 60, true)
                .build();

        for (int i = 0; i < results.size(); i++) {
            final ParameterContextDTO r = results.get(i);
            table.addRow("" + (i+1), r.getId(), r.getName(), r.getDescription());
        }

        final TableWriter tableWriter = new DynamicTableWriter();
        tableWriter.write(table, output);
    }

    @Override
    public ParameterContextsEntity getResult() {
        return parameterContexts;
    }

    @Override
    public ReferenceResolver createReferenceResolver(final Context context) {
        final Map<Integer, ParameterContextDTO> backRefs = new HashMap<>();
        final AtomicInteger position = new AtomicInteger(0);
        results.forEach(pc -> backRefs.put(position.incrementAndGet(), pc));

        return new ReferenceResolver() {
            @Override
            public ResolvedReference resolve(final CommandOption option, final Integer position) {
                final ParameterContextDTO parameterContext = backRefs.get(position);
                if (parameterContext != null) {
                    return new ResolvedReference(option, position, parameterContext.getName(), parameterContext.getId());
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
