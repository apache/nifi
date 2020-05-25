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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.CLASS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.COMMENT_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.DEFAULT_PROPERTIES;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROPERTIES_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_PERIOD_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_STRATEGY_KEY;
import static org.apache.nifi.minifi.commons.schema.ProcessorSchema.IT_IS_NOT_A_VALID_SCHEDULING_STRATEGY;

public class ReportingSchema extends BaseSchemaWithIdAndName {
    private String schedulingStrategy;
    private String schedulingPeriod;
    private String comment;

    private String reportingClass;
    private Map<String, Object> properties = DEFAULT_PROPERTIES;

    public ReportingSchema(Map map) {
        super(map, "Reporting(id: {id}, name: {name})");
        if (this.getId().equals("")) {
            // MiNiFi will throw an error if it can not find `id` of BaseSchemaWithIdAndName for YML config version 3
            this.setId(UUID.randomUUID().toString());
        }
        String wrapperName = getWrapperName();
        reportingClass = getRequiredKeyAsType(map, CLASS_KEY, String.class, wrapperName);
        schedulingStrategy = getRequiredKeyAsType(map, SCHEDULING_STRATEGY_KEY, String.class, wrapperName);
        if (schedulingStrategy != null && !isSchedulingStrategy(schedulingStrategy)) {
            addValidationIssue(SCHEDULING_STRATEGY_KEY, wrapperName, IT_IS_NOT_A_VALID_SCHEDULING_STRATEGY);
        }
        schedulingPeriod = getRequiredKeyAsType(map, SCHEDULING_PERIOD_KEY, String.class, wrapperName);
        comment = getOptionalKeyAsType(map, COMMENT_KEY, String.class, wrapperName, "");
        properties = getOptionalKeyAsType(map, PROPERTIES_KEY, Map.class, wrapperName, DEFAULT_PROPERTIES);
    }

    public static boolean isSchedulingStrategy(String string) {
        try {
            SchedulingStrategy.valueOf(string);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> result = super.toMap();
        result.put(CLASS_KEY, reportingClass);
        result.put(COMMENT_KEY, comment);
        result.put(SCHEDULING_STRATEGY_KEY, schedulingStrategy);
        result.put(SCHEDULING_PERIOD_KEY, schedulingPeriod);
        result.put(PROPERTIES_KEY, new HashMap<>(properties));
        return result;
    }

    public String getComment() {
        return comment;
    }

    public String getSchedulingStrategy() {
        return schedulingStrategy;
    }

    public String getSchedulingPeriod() {
        return schedulingPeriod;
    }

    public String getReportingClass() {
        return reportingClass;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }
}
