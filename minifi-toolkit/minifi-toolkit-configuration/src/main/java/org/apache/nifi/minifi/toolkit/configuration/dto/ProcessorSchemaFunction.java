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

import org.apache.nifi.minifi.commons.schema.ProcessorSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RelationshipDTO;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.nifi.minifi.commons.schema.common.CollectionUtil.nullToEmpty;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ID_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.NAME_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_PERIOD_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_STRATEGY_KEY;

public class ProcessorSchemaFunction implements Function<ProcessorDTO, ProcessorSchema> {
    @Override
    public ProcessorSchema apply(ProcessorDTO processorDTO) {
        ProcessorConfigDTO processorDTOConfig = processorDTO.getConfig();

        Map<String, Object> map = new HashMap<>();
        map.put(NAME_KEY, processorDTO.getName());
        map.put(ID_KEY, processorDTO.getId());
        map.put(ProcessorSchema.CLASS_KEY, processorDTO.getType());
        map.put(SCHEDULING_STRATEGY_KEY, processorDTOConfig.getSchedulingStrategy());
        map.put(SCHEDULING_PERIOD_KEY, processorDTOConfig.getSchedulingPeriod());

        map.put(CommonPropertyKeys.MAX_CONCURRENT_TASKS_KEY, processorDTOConfig.getConcurrentlySchedulableTaskCount());
        map.put(ProcessorSchema.PENALIZATION_PERIOD_KEY, processorDTOConfig.getPenaltyDuration());
        map.put(CommonPropertyKeys.YIELD_PERIOD_KEY, processorDTOConfig.getYieldDuration());
        Long runDurationMillis = processorDTOConfig.getRunDurationMillis();
        if (runDurationMillis != null) {
            map.put(ProcessorSchema.RUN_DURATION_NANOS_KEY, runDurationMillis * 1000);
        }
        map.put(ProcessorSchema.AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY, nullToEmpty(processorDTO.getRelationships()).stream()
                .filter(RelationshipDTO::isAutoTerminate)
                .map(RelationshipDTO::getName)
                .collect(Collectors.toList()));
        map.put(ProcessorSchema.PROCESSOR_PROPS_KEY, new HashMap<>(nullToEmpty(processorDTOConfig.getProperties())));

        String annotationData = processorDTOConfig.getAnnotationData();
        if(annotationData != null && !annotationData.isEmpty()) {
            map.put(ProcessorSchema.ANNOTATION_DATA_KEY, annotationData);
        }

        return new ProcessorSchema(map);
    }
}
