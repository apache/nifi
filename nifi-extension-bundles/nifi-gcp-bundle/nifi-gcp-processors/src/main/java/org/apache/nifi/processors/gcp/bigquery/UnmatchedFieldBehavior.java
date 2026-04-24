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

package org.apache.nifi.processors.gcp.bigquery;

import org.apache.nifi.components.DescribedValue;

/**
 * Strategy applied when an incoming record contains a field that does not exist in the
 * target BigQuery table schema. The BigQuery Storage Write API does not surface this condition
 * because NiFi drops unknown fields client-side before encoding to Protobuf; this enum allows
 * operators to opt in to logging or failure semantics.
 */
public enum UnmatchedFieldBehavior implements DescribedValue {

    IGNORE("Ignore Unmatched Fields", "Ignore Unmatched Fields",
            "Any record field that does not map to a BigQuery table column is silently ignored."),
    WARN("Warn on Unmatched Fields", "Warn on Unmatched Fields",
            "Any record field that does not map to a BigQuery table column is ignored, but a warning is logged per affected record."),
    FAIL("Fail on Unmatched Fields", "Fail on Unmatched Fields", """
            If a record contains a field that does not map to a BigQuery table column, the FlowFile is routed to failure; \
            or the affected record is dropped when Skip Invalid Rows is true.""");

    private final String value;
    private final String displayName;
    private final String description;

    UnmatchedFieldBehavior(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
