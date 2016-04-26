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
package org.apache.nifi.minifi.bootstrap.util.schema;

import org.apache.nifi.minifi.bootstrap.util.schema.common.BaseSchema;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.util.Map;

import static org.apache.nifi.minifi.bootstrap.util.schema.RemoteProcessingGroupSchema.TIMEOUT_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.COMMENT_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.PROVENANCE_REPORTING_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.SCHEDULING_PERIOD_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.SCHEDULING_STRATEGY_KEY;
import static org.apache.nifi.minifi.bootstrap.util.schema.common.CommonPropertyKeys.USE_COMPRESSION_KEY;

/**
 *
 */
public class ProvenanceReportingSchema extends BaseSchema {
    public static final String DESTINATION_URL_KEY = "destination url";
    public static final String PORT_NAME_KEY = "port name";
    public static final String ORIGINATING_URL_KEY = "originating url";
    public static final String BATCH_SIZE_KEY = "batch size";

    private String comment;
    private String schedulingStrategy;
    private String schedulingPeriod;
    private String destinationUrl;
    private String portName;
    private String originatingUrl = "http://${hostname(true)}:8080/nifi";
    private Boolean useCompression = true;
    private String timeout = "30 secs";
    private Number batchSize = 1000;

    public ProvenanceReportingSchema() {
    }

    public ProvenanceReportingSchema(Map map) {
        comment = getOptionalKeyAsType(map, COMMENT_KEY, String.class, PROVENANCE_REPORTING_KEY, null);

        schedulingStrategy = getRequiredKeyAsType(map, SCHEDULING_STRATEGY_KEY, String.class, PROVENANCE_REPORTING_KEY);
        try {
            SchedulingStrategy.valueOf(schedulingStrategy);
        } catch (IllegalArgumentException e) {
            addValidationIssue(SCHEDULING_STRATEGY_KEY, PROVENANCE_REPORTING_KEY, "it is not a valid scheduling strategy");
        }

        schedulingPeriod = getRequiredKeyAsType(map, SCHEDULING_PERIOD_KEY, String.class, PROVENANCE_REPORTING_KEY);

        destinationUrl = getRequiredKeyAsType(map, DESTINATION_URL_KEY, String.class, PROVENANCE_REPORTING_KEY);
        portName = getRequiredKeyAsType(map, PORT_NAME_KEY, String.class, PROVENANCE_REPORTING_KEY);

        originatingUrl = getOptionalKeyAsType(map, ORIGINATING_URL_KEY, String.class, PROVENANCE_REPORTING_KEY, "http://${hostname(true)}:8080/nifi");

        useCompression = getOptionalKeyAsType(map, USE_COMPRESSION_KEY, Boolean.class, PROVENANCE_REPORTING_KEY, true);

        timeout = getOptionalKeyAsType(map, TIMEOUT_KEY, String.class, PROVENANCE_REPORTING_KEY, "30 secs");

        batchSize = getOptionalKeyAsType(map, BATCH_SIZE_KEY, Number.class, PROVENANCE_REPORTING_KEY, 1000);
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
