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

package org.apache.nifi.processors.zendesk;

import org.apache.nifi.components.DescribedValue;

import java.util.stream.Stream;

public enum ZendeskExportMethod implements DescribedValue {
    CURSOR("cursor", "Cursor Based", "%s/cursor.json",
        "start_time", "cursor", "after_cursor",
        "In cursor-based incremental exports, each page of results includes an \"after\" cursor pointer to use as the starting cursor for the next page of results."),
    TIME("time", "Time Based", "%s.json",
        "start_time", "start_time", "end_time",
        "In time-based incremental exports, each page of results includes an end time to use as the start time for the next page of results.");

    private final String value;
    private final String displayName;
    private final String exportApiPathTemplate;
    private final String initialCursorQueryParameterName;
    private final String cursorQueryParameterName;
    private final String cursorJsonFieldName;
    private final String description;

    ZendeskExportMethod(String value, String displayName, String exportApiPathTemplate, String initialCursorQueryParameterName,
                        String cursorQueryParameterName, String cursorJsonFieldName, String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
        this.exportApiPathTemplate = exportApiPathTemplate;
        this.initialCursorQueryParameterName = initialCursorQueryParameterName;
        this.cursorQueryParameterName = cursorQueryParameterName;
        this.cursorJsonFieldName = cursorJsonFieldName;
    }

    public static ZendeskExportMethod forName(String methodName) {
        return Stream.of(values()).filter(m -> m.getValue().equalsIgnoreCase(methodName)).findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Invalid Zendesk incremental export method: " + methodName));
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

    public String getExportApiPathTemplate() {
        return exportApiPathTemplate;
    }

    public String getInitialCursorQueryParameterName() {
        return initialCursorQueryParameterName;
    }

    public String getCursorQueryParameterName() {
        return cursorQueryParameterName;
    }

    public String getCursorJsonFieldName() {
        return cursorJsonFieldName;
    }
}
