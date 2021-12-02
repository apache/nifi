/*
 * Apache NiFi
 * Copyright 2014-2018 The Apache Software Foundation
 *
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

package org.apache.nifi.c2.protocol.api;

import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public enum FlowFormat {

    YAML_V2_TYPE(Values.YAML_V2),

    FLOW_SNAPSHOT_JSON_V1_TYPE(Values.FLOW_SNAPSHOT_JSON_V1);

    public final String headerValue;

    FlowFormat(final String headerValue) {
        this.headerValue = headerValue;
        Objects.requireNonNull(this.headerValue);
    }

    public String getHeaderValue() {
        return headerValue;
    }

    public static FlowFormat fromHeaderValue(final String headerValue) {
        if (StringUtils.isBlank(headerValue)) {
            throw new IllegalArgumentException("Header value cannot be null or blank");
        }

        for (FlowFormat flowFormat : values()) {
            if (flowFormat.getHeaderValue().equals(headerValue.toLowerCase())) {
                return flowFormat;
            }
        }

        throw new IllegalArgumentException("Unknown header value: " + headerValue);
    }

    /**
     * The string values of the header for reference from REST resource methods that need a string.
     */
    public static class Values {

        public static final String YAML_V2 = "application/vnd.minifi-c2+yaml;version=2";

        public static final String FLOW_SNAPSHOT_JSON_V1 = "application/vnd.minifi-c2+json;version=1";

        /**
         * NOTE - THIS LIST BE UPDATED WHEN ADDING VALUES ABOVE
         */
        public static final String ALL =
            YAML_V2 + ", " +
                FLOW_SNAPSHOT_JSON_V1;

    }
}
