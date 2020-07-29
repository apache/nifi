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
package org.wali;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DummyRecord {

    private final String id;
    private final Map<String, String> props;
    private final UpdateType updateType;
    private String swapLocation;

    public DummyRecord(final String id, final UpdateType updateType) {
        this.id = id;
        this.props = new HashMap<>();
        this.updateType = updateType;
    }

    public String getId() {
        return id;
    }

    public UpdateType getUpdateType() {
        return updateType;
    }

    public DummyRecord setProperties(final Map<String, String> props) {
        this.props.clear();
        this.props.putAll(props);
        return this;
    }

    public DummyRecord setProperty(final String name, final String value) {
        this.props.put(name, value);
        return this;
    }

    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(this.props);
    }

    public String getProperty(final String name) {
        return props.get(name);
    }

    public String getSwapLocation() {
        return swapLocation;
    }

    public void setSwapLocation(String swapLocation) {
        this.swapLocation = swapLocation;
    }

    @Override
    public String toString() {
        return "DummyRecord [id=" + id + ", props=" + props + ", updateType=" + updateType + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id, this.props, this.updateType, this.swapLocation);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof DummyRecord)) {
            return false;
        }
        final DummyRecord other = (DummyRecord) obj;
        return Objects.equals(id, other.id) && Objects.equals(props, other.props) && Objects.equals(updateType, other.updateType) && Objects.equals(swapLocation, other.swapLocation);
    }
}
