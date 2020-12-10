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
package org.apache.nifi.minifi.toolkit.configuration.registry;

import org.apache.nifi.minifi.commons.schema.ProcessorSchema;
import org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys;
import org.apache.nifi.registry.flow.VersionedProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.nifi.minifi.commons.schema.common.CollectionUtil.nullToEmpty;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ANNOTATION_DATA_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CLASS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ID_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.NAME_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROPERTIES_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_PERIOD_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_STRATEGY_KEY;

public class NiFiRegProcessorSchemaFunction implements Function<VersionedProcessor, ProcessorSchema> {
    @Override
    public ProcessorSchema apply(final VersionedProcessor versionedProcessor) {
        Map<String, Object> map = new HashMap<>();
        map.put(NAME_KEY, versionedProcessor.getName());
        map.put(ID_KEY, versionedProcessor.getIdentifier());
        map.put(CLASS_KEY, versionedProcessor.getType());
        map.put(SCHEDULING_STRATEGY_KEY, versionedProcessor.getSchedulingStrategy());
        map.put(SCHEDULING_PERIOD_KEY, versionedProcessor.getSchedulingPeriod());

        map.put(CommonPropertyKeys.MAX_CONCURRENT_TASKS_KEY, versionedProcessor.getConcurrentlySchedulableTaskCount());
        map.put(ProcessorSchema.PENALIZATION_PERIOD_KEY, versionedProcessor.getPenaltyDuration());
        map.put(CommonPropertyKeys.YIELD_PERIOD_KEY, versionedProcessor.getYieldDuration());
        Long runDurationMillis = versionedProcessor.getRunDurationMillis();
        if (runDurationMillis != null) {
            map.put(ProcessorSchema.RUN_DURATION_NANOS_KEY, runDurationMillis * 1000);
        }

        final List<String> autoTerminateRelationships = new ArrayList<>(nullToEmpty(versionedProcessor.getAutoTerminatedRelationships()));
        map.put(ProcessorSchema.AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY, autoTerminateRelationships);

        map.put(PROPERTIES_KEY, new HashMap<>(nullToEmpty(versionedProcessor.getProperties())));

        String annotationData = versionedProcessor.getAnnotationData();
        if(annotationData != null && !annotationData.isEmpty()) {
            map.put(ANNOTATION_DATA_KEY, annotationData);
        }

        return new ProcessorSchema(map);
    }
}
