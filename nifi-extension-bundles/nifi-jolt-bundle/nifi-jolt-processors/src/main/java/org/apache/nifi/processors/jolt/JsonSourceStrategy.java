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

package org.apache.nifi.processors.jolt;

import org.apache.nifi.components.DescribedValue;

public enum JsonSourceStrategy implements DescribedValue {
    FLOW_FILE("application/json", "Transformation applied to FlowFile content containing JSON"),
    ATTRIBUTE("application/json", "Transformation applied to FlowFile attribute containing JSON"),
    JSON_LINES("application/jsonl", "Transformation applied to FlowFile content containing JSON Lines or NDJSON");

    private final String contentType;
    private final String description;

    JsonSourceStrategy(
            final String contentType,
            final String description
    ) {
        this.contentType = contentType;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return name();
    }

    @Override
    public String getDescription() {
        return description;
    }

    public String getContentType() {
        return contentType;
    }
}
