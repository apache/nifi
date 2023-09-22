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

import org.apache.nifi.toolkit.cli.api.ResultType;
import org.apache.nifi.toolkit.cli.impl.result.AbstractWritableResult;
import org.apache.nifi.web.api.dto.FlowRegistryClientDTO;

import java.io.PrintStream;
import java.util.Objects;

public class RegistryClientIDResult extends AbstractWritableResult<FlowRegistryClientDTO> {

    private final FlowRegistryClientDTO flowRegistryClientDTO;

    public RegistryClientIDResult(final ResultType resultType, final FlowRegistryClientDTO flowRegistryClientDTO) {
        super(resultType);
        this.flowRegistryClientDTO = Objects.requireNonNull(flowRegistryClientDTO);
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) {
        output.println(flowRegistryClientDTO.getId());
    }

    @Override
    public FlowRegistryClientDTO getResult() {
        return flowRegistryClientDTO;
    }
}
