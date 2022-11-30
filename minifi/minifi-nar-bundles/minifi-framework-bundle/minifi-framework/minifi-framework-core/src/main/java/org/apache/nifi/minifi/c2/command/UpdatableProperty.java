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

package org.apache.nifi.minifi.c2.command;

import java.io.Serializable;
import java.util.Objects;

public class UpdatableProperty implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String propertyName;
    private final String propertyValue;
    private final String validator;

    public UpdatableProperty(String propertyName, String propertyValue, String validator) {
        this.propertyName = propertyName;
        this.propertyValue = propertyValue;
        this.validator = validator;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getPropertyValue() {
        return propertyValue;
    }

    public String getValidator() {
        return validator;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UpdatableProperty that = (UpdatableProperty) o;
        return Objects.equals(propertyName, that.propertyName) && Objects.equals(propertyValue, that.propertyValue) && Objects.equals(validator,
            that.validator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(propertyName, propertyValue, validator);
    }

    @Override
    public String toString() {
        return "UpdatableProperty{" +
            "propertyName='" + propertyName + '\'' +
            ", propertyValue='" + propertyValue + '\'' +
            ", validator='" + validator + '\'' +
            '}';
    }

}
