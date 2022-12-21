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
package org.apache.nifi.kafka.shared.login;

/**
 * Helper class to build JAAS configuration
 */
public class LoginConfigBuilder {

    private static final String SPACE = " ";

    private static final String EQUALS = "=";

    private static final String DOUBLE_QUOTE = "\"";

    private static final String SEMI_COLON = ";";

    private static final String REQUIRE_MODULE_CLASS_FORMAT = "%s required";

    private final StringBuilder builder;

    public LoginConfigBuilder(String moduleClassName) {
        this.builder = new StringBuilder(String.format(REQUIRE_MODULE_CLASS_FORMAT, moduleClassName));
    }

    public LoginConfigBuilder append(String configEntry) {
        builder.append(SPACE).append(configEntry);
        return this;
    }

    public LoginConfigBuilder append(String key, Object value) {
        builder.append(SPACE);

        builder.append(key);
        builder.append(EQUALS);
        if (value instanceof String) {
            builder.append(DOUBLE_QUOTE);
            builder.append(value);
            builder.append(DOUBLE_QUOTE);
        } else {
            builder.append(value);
        }

        return this;
    }

    public String build() {
        builder.append(SEMI_COLON);
        return builder.toString();
    }

    @Override
    public String toString() {
        return build();
    }
}
