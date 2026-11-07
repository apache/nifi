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
package org.apache.nifi.swagger.integration;

import io.swagger.v3.oas.integration.api.ObjectMapperProcessor;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.cfg.EnumFeature;

/**
 * Standard Jackson Object Mapper Processor for customizing OpenAPI Specification generation
 */
public class StandardObjectMapperProcessor implements ObjectMapperProcessor {

    @Override
    public ObjectMapper processJsonObjectMapper(final ObjectMapper mapper) {
        // Write enums using name() instead of toString()
        return mapper.rebuild().disable(EnumFeature.WRITE_ENUMS_USING_TO_STRING).build();
    }
}
