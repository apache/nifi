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
import org.apache.nifi.web.api.entity.ProcessGroupEntity;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;

public class ProcessGroupResult extends AbstractWritableResult<ProcessGroupEntity> {

    private final ProcessGroupEntity entity;

    public ProcessGroupResult(final ResultType resultType, final ProcessGroupEntity entity) {
        super(resultType);
        this.entity = entity;
        Validate.notNull(entity);
    }

    @Override
    public ProcessGroupEntity getResult() {
        return entity;
    }

    @Override
    protected void writeSimpleResult(final PrintStream output) throws IOException {
        final ProcessGroupsResult result = new ProcessGroupsResult(
                ResultType.SIMPLE,
                Collections.singletonList(entity.getComponent())
        );
        result.writeSimpleResult(output);
    }
}
