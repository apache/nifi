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
package org.apache.nifi.json.schema;

import org.apache.nifi.components.DescribedValue;

public enum SchemaVersion implements DescribedValue {
    DRAFT_4("Draft Version 4", "Draft 4", "http://json-schema.org/draft-04/schema#"),
    DRAFT_6("Draft Version 6", "Draft 6", "http://json-schema.org/draft-06/schema#"),
    DRAFT_7("Draft Version 7", "Draft 7", "http://json-schema.org/draft-07/schema#"),
    DRAFT_2019_09("Draft Version 2019-09", "Draft 2019-09", "https://json-schema.org/draft/2019-09/schema"),
    DRAFT_2020_12("Draft Version 2020-12", "Draft 2020-12", "https://json-schema.org/draft/2020-12/schema");

    private final String description;
    private final String displayName;
    private final String uri;

    SchemaVersion(String description, String displayName, String uri) {
        this.description = description;
        this.displayName = displayName;
        this.uri = uri;
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

    public String getUri() {
        return uri;
    }
}