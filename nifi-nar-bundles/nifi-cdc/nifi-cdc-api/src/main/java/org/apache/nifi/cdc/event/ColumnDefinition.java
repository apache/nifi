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
package org.apache.nifi.cdc.event;

import org.apache.commons.lang3.builder.EqualsBuilder;

/**
 * A class that specifies a definition for a relational table column, including type, name, etc.
 */
public class ColumnDefinition {

    private int type;
    private String name = "";

    public ColumnDefinition(int type) {
        this.type = type;
    }

    public ColumnDefinition(int type, String name) {
        this(type);
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnDefinition that = (ColumnDefinition) o;

        return new EqualsBuilder()
                .append(type, that.type)
                .append(name, that.name)
                .isEquals();
    }

    @Override
    public int hashCode() {
        int result = type;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}
