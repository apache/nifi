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

package org.apache.nifi.processors.snowflake.util;

import org.apache.nifi.components.DescribedValue;

import java.util.Objects;
import java.util.Optional;

public enum SnowflakeInternalStageType implements DescribedValue {
    USER("user", "User", "Use the user's internal stage") {
        @Override
        public String getStage(final SnowflakeInternalStageTypeParameters parameters) {
            return "@~";
        }
    },
    TABLE("table", "Table", "Use a table's internal stage") {
        @Override
        public String getStage(final SnowflakeInternalStageTypeParameters parameters) {
            final StringBuilder stringBuilder = new StringBuilder("@");
            Optional.ofNullable(parameters.database())
                    .ifPresent(database -> stringBuilder.append(database).append("."));
            Optional.ofNullable(parameters.schema())
                    .ifPresent(schema -> stringBuilder.append(schema).append("."));

            stringBuilder.append("%").append(Objects.requireNonNull(parameters.table()));
            return stringBuilder.toString();
        }
    },
    NAMED("named", "Named", "Use a named internal stage. This stage must be created beforehand in Snowflake") {
        @Override
        public String getStage(final SnowflakeInternalStageTypeParameters parameters) {
            final StringBuilder stringBuilder = new StringBuilder("@");
            Optional.ofNullable(parameters.database())
                    .ifPresent(database -> stringBuilder.append(database).append("."));
            Optional.ofNullable(parameters.schema())
                    .ifPresent(schema -> stringBuilder.append(schema).append("."));
            stringBuilder.append(Objects.requireNonNull(parameters.stageName()));
            return stringBuilder.toString();
        }
    };

    private final String value;
    private final String displayName;
    private final String description;

    SnowflakeInternalStageType(final String value, final String displayName, final String description) {
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

    public abstract String getStage(final SnowflakeInternalStageTypeParameters parameters);

}
