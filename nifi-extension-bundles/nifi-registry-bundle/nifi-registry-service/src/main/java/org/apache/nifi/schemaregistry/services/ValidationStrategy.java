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
package org.apache.nifi.schemaregistry.services;

import org.apache.nifi.components.DescribedValue;

public enum ValidationStrategy implements DescribedValue {
    VALIDATE("Validate Schemas", """
            Validate incoming Avro schemas. Validation includes field name,
            namespace and default value validation. All field names must be valid Avro names
            that match the following regular expression [A-Za-z_][A-Za-z0-9_]* (i.e. a name that starts with a letter or an underscore, followed by zero or more alphanumeric,
            characters or underscores), all namespaces must be one or more valid Avro names each matching the aforementioned regular expression and separated by periods
            and any default value specified must match the field type (e.g. a field with type int must specify a number such as 9 or null)"""),
    NONE("Do Not Validate Schemas", "Do not validate field names, namespaces and default values of incoming schemas.");

    private final String displayName;
    private final String description;

    ValidationStrategy(String displayName, String description) {
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
