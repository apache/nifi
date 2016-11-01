/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.minifi.toolkit.configuration.dto;

import org.apache.nifi.minifi.commons.schema.PortSchema;
import org.apache.nifi.web.api.dto.PortDTO;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ID_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.NAME_KEY;

public class PortSchemaFunction implements Function<PortDTO, PortSchema> {
    private final String wrapperName;

    public PortSchemaFunction(String wrapperName) {
        this.wrapperName = wrapperName;
    }

    @Override
    public PortSchema apply(PortDTO portDTO) {
        Map<String, Object> map = new HashMap<>();
        map.put(ID_KEY, portDTO.getId());
        map.put(NAME_KEY, portDTO.getName());
        return new PortSchema(map, wrapperName);
    }
}
