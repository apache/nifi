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

package org.apache.nifi.minifi.toolkit.schema.v1;

import org.apache.nifi.minifi.toolkit.schema.ProcessorSchema;
import org.apache.nifi.minifi.toolkit.schema.common.BaseSchema;
import org.apache.nifi.minifi.toolkit.schema.common.ConvertableSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.minifi.toolkit.schema.ProcessorSchema.AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY;
import static org.apache.nifi.minifi.toolkit.schema.ProcessorSchema.DEFAULT_AUTO_TERMINATED_RELATIONSHIPS_LIST;
import static org.apache.nifi.minifi.toolkit.schema.ProcessorSchema.DEFAULT_MAX_CONCURRENT_TASKS;
import static org.apache.nifi.minifi.toolkit.schema.ProcessorSchema.DEFAULT_PENALIZATION_PERIOD;
import static org.apache.nifi.minifi.toolkit.schema.ProcessorSchema.DEFAULT_RUN_DURATION_NANOS;
import static org.apache.nifi.minifi.toolkit.schema.ProcessorSchema.DEFAULT_YIELD_DURATION;
import static org.apache.nifi.minifi.toolkit.schema.ProcessorSchema.IT_IS_NOT_A_VALID_SCHEDULING_STRATEGY;
import static org.apache.nifi.minifi.toolkit.schema.ProcessorSchema.PENALIZATION_PERIOD_KEY;
import static org.apache.nifi.minifi.toolkit.schema.ProcessorSchema.RUN_DURATION_NANOS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.ProcessorSchema.isSchedulingStrategy;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.CLASS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.DEFAULT_PROPERTIES;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.MAX_CONCURRENT_TASKS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.NAME_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.PROCESSORS_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.PROPERTIES_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.SCHEDULING_PERIOD_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.SCHEDULING_STRATEGY_KEY;
import static org.apache.nifi.minifi.toolkit.schema.common.CommonPropertyKeys.YIELD_PERIOD_KEY;

public class ProcessorSchemaV1 extends BaseSchema implements ConvertableSchema<ProcessorSchema> {
    private String name;
    private String processorClass;
    private String schedulingStrategy;
    private String schedulingPeriod;
    private Number maxConcurrentTasks = DEFAULT_MAX_CONCURRENT_TASKS;
    private String penalizationPeriod = DEFAULT_PENALIZATION_PERIOD;
    private String yieldPeriod = DEFAULT_YIELD_DURATION;
    private Number runDurationNanos = DEFAULT_RUN_DURATION_NANOS;
    private List<String> autoTerminatedRelationshipsList = DEFAULT_AUTO_TERMINATED_RELATIONSHIPS_LIST;
    private Map<String, Object> properties = DEFAULT_PROPERTIES;

    public ProcessorSchemaV1(Map map) {
        name = getRequiredKeyAsType(map, NAME_KEY, String.class, PROCESSORS_KEY);
        processorClass = getRequiredKeyAsType(map, CLASS_KEY, String.class, PROCESSORS_KEY);
        schedulingStrategy = getRequiredKeyAsType(map, SCHEDULING_STRATEGY_KEY, String.class, PROCESSORS_KEY);
        if (schedulingStrategy != null && !isSchedulingStrategy(schedulingStrategy)) {
            addValidationIssue(SCHEDULING_STRATEGY_KEY, PROCESSORS_KEY, IT_IS_NOT_A_VALID_SCHEDULING_STRATEGY);
        }
        schedulingPeriod = getRequiredKeyAsType(map, SCHEDULING_PERIOD_KEY, String.class, PROCESSORS_KEY);

        maxConcurrentTasks = getOptionalKeyAsType(map, MAX_CONCURRENT_TASKS_KEY, Number.class, PROCESSORS_KEY, DEFAULT_MAX_CONCURRENT_TASKS);
        penalizationPeriod = getOptionalKeyAsType(map, PENALIZATION_PERIOD_KEY, String.class, PROCESSORS_KEY, DEFAULT_PENALIZATION_PERIOD);
        yieldPeriod = getOptionalKeyAsType(map, YIELD_PERIOD_KEY, String.class, PROCESSORS_KEY, DEFAULT_YIELD_DURATION);
        runDurationNanos = getOptionalKeyAsType(map, RUN_DURATION_NANOS_KEY, Number.class, PROCESSORS_KEY, DEFAULT_RUN_DURATION_NANOS);
        autoTerminatedRelationshipsList = getOptionalKeyAsType(map, AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY, List.class, PROCESSORS_KEY, DEFAULT_AUTO_TERMINATED_RELATIONSHIPS_LIST);
        properties = getOptionalKeyAsType(map, PROPERTIES_KEY, Map.class, PROCESSORS_KEY, DEFAULT_PROPERTIES);
    }

    @Override
    public ProcessorSchema convert() {
        Map<String, Object> map = new HashMap<>();
        map.put(NAME_KEY, name);
        map.put(CLASS_KEY, processorClass);
        map.put(MAX_CONCURRENT_TASKS_KEY, maxConcurrentTasks);
        map.put(SCHEDULING_STRATEGY_KEY, schedulingStrategy);
        map.put(SCHEDULING_PERIOD_KEY, schedulingPeriod);
        map.put(PENALIZATION_PERIOD_KEY, penalizationPeriod);
        map.put(YIELD_PERIOD_KEY, yieldPeriod);
        map.put(RUN_DURATION_NANOS_KEY, runDurationNanos);
        map.put(AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY, autoTerminatedRelationshipsList);
        map.put(PROPERTIES_KEY, new HashMap<>(properties));
        return new ProcessorSchema(map);
    }

    public String getName() {
        return name;
    }

    @Override
    public int getVersion() {
        return ConfigSchemaV1.CONFIG_VERSION;
    }
}
