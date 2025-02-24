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

package org.apache.nifi.processors.aws.s3.api;

import org.apache.nifi.components.DescribedValue;

public enum TagsTarget implements DescribedValue {
    ATTRIBUTES("Attributes", """
            When selected, the tags will be written to FlowFile attributes with the prefix "s3.tag." following the convention used in other processors. For example:
            the S3 tag GuardDutyMalwareScanStatusType will be written as s3.tag.GuardDutyMalwareScanStatus when using the default value
            """),
    FLOWFILE_BODY("FlowFile Body", "Write the tags to FlowFile content as JSON data.");

    private final String displayName;
    private final String description;

    TagsTarget(String displayName, String description) {
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
