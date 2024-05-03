/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.iceberg;

import org.apache.nifi.components.DescribedValue;

public enum UnmatchedColumnBehavior implements DescribedValue {
    IGNORE_UNMATCHED_COLUMN("Ignore Unmatched Columns",
            "Any column in the database that does not have a field in the document will be assumed to not be required.  No notification will be logged"),

    WARNING_UNMATCHED_COLUMN("Warn on Unmatched Columns",
            "Any column in the database that does not have a field in the document will be assumed to not be required.  A warning will be logged"),

    FAIL_UNMATCHED_COLUMN("Fail on Unmatched Columns",
            "A flow will fail if any column in the database that does not have a field in the document.  An error will be logged");


    private final String displayName;
    private final String description;

    UnmatchedColumnBehavior(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
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
