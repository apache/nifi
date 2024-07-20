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
package org.apache.nifi.parameter;

import java.util.Objects;

/**
 * Filter values and return null for Parameter values with sensitive descriptors
 */
public class FilterSensitiveParameterValueMapper implements ParameterValueMapper {
    /**
     * Get mapped Parameter value based on properties
     *
     * @param parameter Parameter with descriptor of attributes for mapping
     * @param value Parameter value to be mapped
     * @return Mapped Parameter value
     */
    @Override
    public String getMapped(final Parameter parameter, final String value) {
        Objects.requireNonNull(parameter, "Parameter required");

        final ParameterDescriptor descriptor = parameter.getDescriptor();
        final String mapped;

        if (descriptor.isSensitive()) {
            mapped = null;
        } else {
            mapped = value;
        }

        return mapped;
    }
}
