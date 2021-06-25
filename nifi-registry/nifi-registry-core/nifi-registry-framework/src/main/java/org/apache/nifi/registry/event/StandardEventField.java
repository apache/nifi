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
package org.apache.nifi.registry.event;

import org.apache.commons.lang3.Validate;
import org.apache.nifi.registry.hook.EventField;
import org.apache.nifi.registry.hook.EventFieldName;

import java.util.Objects;

/**
 * Standard implementation of EventField.
 */
public class StandardEventField implements EventField {

    private final EventFieldName name;

    private final String value;

    public StandardEventField(final EventFieldName name, final String value) {
        this.name = name;
        this.value = value;
        Validate.notNull(this.name);
        Validate.notNull(this.value);
    }

    @Override
    public EventFieldName getName() {
        return name;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StandardEventField that = (StandardEventField) o;
        return name == that.name && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return "StandardEventField{name=" + name + ", value='" + value + '\'' + '}';
    }
}
