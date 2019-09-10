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

public class Parameter {
    private final ParameterDescriptor descriptor;
    private final String value;

    public Parameter(final ParameterDescriptor descriptor, final String value) {
        this.descriptor = descriptor;
        this.value = value;
    }

    public ParameterDescriptor getDescriptor() {
        return descriptor;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Parameter parameter = (Parameter) o;
        return Objects.equals(descriptor, parameter.descriptor) && Objects.equals(value, parameter.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(descriptor, value);
    }
}
