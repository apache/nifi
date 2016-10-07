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

package org.apache.nifi.minifi.toolkit.configuration.dto;

import org.apache.nifi.minifi.commons.schema.common.WritableSchema;
import org.junit.Test;

import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public abstract class BaseSchemaTester<Schema extends WritableSchema, DTO> {
    protected final Function<DTO, Schema> dtoSchemaFunction;
    protected final Function<Map, Schema> mapSchemaFunction;
    protected DTO dto;
    protected Map<String, Object> map;

    protected BaseSchemaTester(Function<DTO, Schema> dtoSchemaFunction, Function<Map, Schema> mapSchemaFunction) {
        this.dtoSchemaFunction = dtoSchemaFunction;
        this.mapSchemaFunction = mapSchemaFunction;
    }

    protected void assertDtoAndMapConstructorAreSame(int validationErrors) {
        Schema dtoSchema = dtoSchemaFunction.apply(dto);
        Schema mapSchema = mapSchemaFunction.apply(map);
        assertSchemaEquals(dtoSchema, mapSchema);
        assertEquals(dtoSchema.getValidationIssues(), mapSchema.getValidationIssues());
        assertSchemaEquals(dtoSchema, mapSchemaFunction.apply(dtoSchema.toMap()));
        assertSchemaEquals(mapSchema, mapSchemaFunction.apply(mapSchema.toMap()));
        assertEquals(validationErrors, dtoSchema.getValidationIssues().size());
    }

    public abstract void assertSchemaEquals(Schema one, Schema two);

    @Test
    public void testFullyPopulatedSame() {
        assertDtoAndMapConstructorAreSame(0);
    }
}
