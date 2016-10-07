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

import org.apache.nifi.minifi.commons.schema.common.BaseSchema;
import org.apache.nifi.minifi.commons.schema.common.WritableSchema;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.util.Map;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.COMMENT_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.PROVENANCE_REPORTING_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_PERIOD_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_STRATEGY_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.USE_COMPRESSION_KEY;
import static org.apache.nifi.minifi.commons.schema.RemoteProcessingGroupSchema.TIMEOUT_KEY;

/**
 *
 */
public class ProvenanceReportingSchema extends BaseSchema implements WritableSchema {
    public static final String DESTINATION_URL_KEY = "destination url";
    public static final String PORT_NAME_KEY = "port name";
    public static final String ORIGINATING_URL_KEY = "originating url";
    public static final String BATCH_SIZE_KEY = "batch size";

    public static final String DEFAULT_ORGINATING_URL = "http://${hostname(true)}:8080/nifi";
    public static final String DEFAULT_TIMEOUT = "30 secs";
    public static final int DEFAULT_BATCH_SIZE = 1000;
    public static final boolean DEFAULT_USE_COMPRESSION = true;

    private String schedulingStrategy;
    private String schedulingPeriod;
    private String destinationUrl;
    private String portName;

    private String comment;
    private String originatingUrl = DEFAULT_ORGINATING_URL;
    private Boolean useCompression = DEFAULT_USE_COMPRESSION;
    private String timeout = DEFAULT_TIMEOUT;
    private Number batchSize = DEFAULT_BATCH_SIZE;

    public ProvenanceReportingSchema(Map map) {
        schedulingStrategy = getRequiredKeyAsType(map, SCHEDULING_STRATEGY_KEY, String.class, PROVENANCE_REPORTING_KEY);
        if (schedulingStrategy != null) {
            try {
                SchedulingStrategy.valueOf(schedulingStrategy);
            } catch (IllegalArgumentException e) {
                addValidationIssue(SCHEDULING_STRATEGY_KEY, PROVENANCE_REPORTING_KEY, "it is not a valid scheduling strategy");
            }
        }

        schedulingPeriod = getRequiredKeyAsType(map, SCHEDULING_PERIOD_KEY, String.class, PROVENANCE_REPORTING_KEY);
        destinationUrl = getRequiredKeyAsType(map, DESTINATION_URL_KEY, String.class, PROVENANCE_REPORTING_KEY);
        portName = getRequiredKeyAsType(map, PORT_NAME_KEY, String.class, PROVENANCE_REPORTING_KEY);

        comment = getOptionalKeyAsType(map, COMMENT_KEY, String.class, PROVENANCE_REPORTING_KEY, "");
        originatingUrl = getOptionalKeyAsType(map, ORIGINATING_URL_KEY, String.class, PROVENANCE_REPORTING_KEY, DEFAULT_ORGINATING_URL);
        useCompression = getOptionalKeyAsType(map, USE_COMPRESSION_KEY, Boolean.class, PROVENANCE_REPORTING_KEY, DEFAULT_USE_COMPRESSION);
        timeout = getOptionalKeyAsType(map, TIMEOUT_KEY, String.class, PROVENANCE_REPORTING_KEY, DEFAULT_TIMEOUT);
        batchSize = getOptionalKeyAsType(map, BATCH_SIZE_KEY, Number.class, PROVENANCE_REPORTING_KEY, DEFAULT_BATCH_SIZE);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = mapSupplier.get();
        result.put(COMMENT_KEY, comment);
        result.put(SCHEDULING_STRATEGY_KEY, schedulingStrategy);
        result.put(SCHEDULING_PERIOD_KEY, schedulingPeriod);
        result.put(DESTINATION_URL_KEY, destinationUrl);
        result.put(PORT_NAME_KEY, portName);
        result.put(ORIGINATING_URL_KEY, originatingUrl);
        result.put(USE_COMPRESSION_KEY, useCompression);
        result.put(TIMEOUT_KEY, timeout);
        result.put(BATCH_SIZE_KEY, batchSize);
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

    public String getDestinationUrl() {
        return destinationUrl;
    }

    public String getPortName() {
        return portName;
    }

    public String getOriginatingUrl() {
        return originatingUrl;
    }

    public boolean getUseCompression() {
        return useCompression;
    }

    public String getTimeout() {
        return timeout;
    }

    public Number getBatchSize() {
        return batchSize;
    }
}
