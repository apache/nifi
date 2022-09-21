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

package org.apache.nifi.c2.protocol.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

@ApiModel
public class SupportedOperation implements Serializable {
    private static final long serialVersionUID = 1;

    @ApiModelProperty("The type of the operation supported by the agent")
    private OperationType type;

    @ApiModelProperty("Operand specific properties defined by the agent")
    private Map<OperandType, Map<String, Object>> properties;

    public OperationType getType() {
        return type;
    }

    public void setType(OperationType type) {
        this.type = type;
    }

    public Map<OperandType, Map<String, Object>> getProperties() {
        return properties;
    }

    public void setProperties(Map<OperandType, Map<String, Object>> properties) {
        this.properties = properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SupportedOperation that = (SupportedOperation) o;
        return type == that.type && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, properties);
    }

    @Override
    public String toString() {
        return "SupportedOperation{" +
            "type=" + type +
            ", properties=" + properties +
            '}';
    }
}
