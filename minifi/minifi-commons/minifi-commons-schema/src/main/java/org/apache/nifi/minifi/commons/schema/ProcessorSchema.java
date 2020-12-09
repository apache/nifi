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

package org.apache.nifi.minifi.commons.schema;

import org.apache.nifi.minifi.commons.schema.common.BaseSchemaWithIdAndName;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ANNOTATION_DATA_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CLASS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.DEFAULT_PROPERTIES;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.MAX_CONCURRENT_TASKS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROPERTIES_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_PERIOD_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_STRATEGY_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.YIELD_PERIOD_KEY;

public class ProcessorSchema extends BaseSchemaWithIdAndName {
    public static final String PENALIZATION_PERIOD_KEY = "penalization period";
    public static final String RUN_DURATION_NANOS_KEY = "run duration nanos";
    public static final String AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY = "auto-terminated relationships list";

    public static final int DEFAULT_MAX_CONCURRENT_TASKS = 1;
    public static final String DEFAULT_PENALIZATION_PERIOD = "30 sec";
    public static final String DEFAULT_YIELD_DURATION = "1 sec";
    public static final long DEFAULT_RUN_DURATION_NANOS = 0;
    public static final List<String> DEFAULT_AUTO_TERMINATED_RELATIONSHIPS_LIST = Collections.emptyList();
    public static final String IT_IS_NOT_A_VALID_SCHEDULING_STRATEGY = "it is not a valid scheduling strategy";

    private String processorClass;
    private String schedulingStrategy;
    private String schedulingPeriod;
    private Number maxConcurrentTasks = DEFAULT_MAX_CONCURRENT_TASKS;
    private String penalizationPeriod = DEFAULT_PENALIZATION_PERIOD;
    private String yieldPeriod = DEFAULT_YIELD_DURATION;
    private Number runDurationNanos = DEFAULT_RUN_DURATION_NANOS;
    private List<String> autoTerminatedRelationshipsList = DEFAULT_AUTO_TERMINATED_RELATIONSHIPS_LIST;
    private Map<String, Object> properties = DEFAULT_PROPERTIES;
    private String annotationData = "";

    public ProcessorSchema(Map map) {
        super(map, "Processor(id: {id}, name: {name})");
        String wrapperName = getWrapperName();
        processorClass = getRequiredKeyAsType(map, CLASS_KEY, String.class, wrapperName);
        schedulingStrategy = getRequiredKeyAsType(map, SCHEDULING_STRATEGY_KEY, String.class, wrapperName);
        if (schedulingStrategy != null && !isSchedulingStrategy(schedulingStrategy)) {
            addValidationIssue(SCHEDULING_STRATEGY_KEY, wrapperName, IT_IS_NOT_A_VALID_SCHEDULING_STRATEGY);
        }
        schedulingPeriod = getRequiredKeyAsType(map, SCHEDULING_PERIOD_KEY, String.class, wrapperName);

        maxConcurrentTasks = getOptionalKeyAsType(map, MAX_CONCURRENT_TASKS_KEY, Number.class, wrapperName, DEFAULT_MAX_CONCURRENT_TASKS);
        penalizationPeriod = getOptionalKeyAsType(map, PENALIZATION_PERIOD_KEY, String.class, wrapperName, DEFAULT_PENALIZATION_PERIOD);
        yieldPeriod = getOptionalKeyAsType(map, YIELD_PERIOD_KEY, String.class, wrapperName, DEFAULT_YIELD_DURATION);
        runDurationNanos = getOptionalKeyAsType(map, RUN_DURATION_NANOS_KEY, Number.class, wrapperName, DEFAULT_RUN_DURATION_NANOS);
        autoTerminatedRelationshipsList = getOptionalKeyAsType(map, AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY, List.class, wrapperName, DEFAULT_AUTO_TERMINATED_RELATIONSHIPS_LIST);
        properties = getOptionalKeyAsType(map, PROPERTIES_KEY, Map.class, wrapperName, DEFAULT_PROPERTIES);

        annotationData = getOptionalKeyAsType(map, ANNOTATION_DATA_KEY, String.class, wrapperName, "");
    }

    public static boolean isSchedulingStrategy(String string) {
        try {
            SchedulingStrategy.valueOf(string);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = super.toMap();
        result.put(CLASS_KEY, processorClass);
        result.put(MAX_CONCURRENT_TASKS_KEY, maxConcurrentTasks);
        result.put(SCHEDULING_STRATEGY_KEY, schedulingStrategy);
        result.put(SCHEDULING_PERIOD_KEY, schedulingPeriod);
        result.put(PENALIZATION_PERIOD_KEY, penalizationPeriod);
        result.put(YIELD_PERIOD_KEY, yieldPeriod);
        result.put(RUN_DURATION_NANOS_KEY, runDurationNanos);
        result.put(AUTO_TERMINATED_RELATIONSHIPS_LIST_KEY, autoTerminatedRelationshipsList);
        result.put(PROPERTIES_KEY, new TreeMap<>(properties));

        if(annotationData != null && !annotationData.isEmpty()) {
            result.put(ANNOTATION_DATA_KEY, annotationData);
        }

        return result;
    }

    public String getProcessorClass() {
        return processorClass;
    }

    public Number getMaxConcurrentTasks() {
        return maxConcurrentTasks;
    }

    public String getSchedulingStrategy() {
        return schedulingStrategy;
    }

    public String getSchedulingPeriod() {
        return schedulingPeriod;
    }

    public String getPenalizationPeriod() {
        return penalizationPeriod;
    }

    public String getYieldPeriod() {
        return yieldPeriod;
    }

    public Number getRunDurationNanos() {
        return runDurationNanos;
    }

    public List<String> getAutoTerminatedRelationshipsList() {
        return autoTerminatedRelationshipsList;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public String getAnnotationData() {
        return annotationData;
    }

}
