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
import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;

import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Result for a list of ProcessGroupEntities.
 */
public class ProcessGroupsResult extends AbstractWritableResult<List<ProcessGroupEntity>> {

    private final List<ProcessGroupEntity> processGroupEntities;

    public ProcessGroupsResult(final ResultType resultType, final List<ProcessGroupEntity> processGroupEntities) {
        super(resultType);
        this.processGroupEntities = processGroupEntities;
        Validate.notNull(this.processGroupEntities);
    }

    @Override
    public List<ProcessGroupEntity> getResult() {
        return processGroupEntities;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        final List<ProcessGroupDTO> dtos = processGroupEntities.stream()
                .map(e -> e.getComponent()).collect(Collectors.toList());

        Collections.sort(dtos, Comparator.comparing(ProcessGroupDTO::getName));

        dtos.stream().forEach(dto -> output.println(dto.getName() + " - " + dto.getId()));
    }
}
