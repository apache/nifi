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
package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.components.DescribedValue;

/**
 * Controls how the Identifier Field, Index Field, and Timestamp Field property values are
 * interpreted when locating a field within a document.
 */
public enum FieldReferenceMode implements DescribedValue {
    LITERAL("Literal Field Name", "Literal Field Name",
            "The configured value is the exact name of a top-level field in the document; \"/\" and \"\\\" "
                    + "are treated as literal characters. This is the default and matches the behavior of earlier releases."),
    NESTED_PATH("Nested Field Path", "Nested Field Path",
            "The configured value is a \"/\"-delimited path into nested objects (e.g. \"@metadata/id\"). "
                    + "To reference a field name that contains a literal \"/\", escape it as \"\\/\"; a literal backslash is \"\\\\\".");

    private final String value;
    private final String displayName;
    private final String description;

    FieldReferenceMode(final String value, final String displayName, final String description) {
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
