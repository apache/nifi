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
package org.apache.nifi.web.api.dto;

import com.wordnik.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

/**
 * Counter value for a specific component in a specific context. A counter is a value that a component can adjust during processing.
 */
@XmlType(name = "counter")
public class CounterDTO implements Cloneable {

    private String id;
    private String context;
    private String name;
    private Long valueCount;
    private String value;

    /**
     * @return context of the counter
     */
    @ApiModelProperty(
            value = "The context of the counter."
    )
    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    /**
     * @return id of the counter
     */
    @ApiModelProperty(
            value = "The id of the counter."
    )
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return name of the counter
     */
    @ApiModelProperty(
            value = "The name of the counter."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return value for the counter
     */
    @ApiModelProperty(
            value = "The value of the counter."
    )
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @ApiModelProperty(
            value = "The value count."
    )
    public Long getValueCount() {
        return valueCount;
    }

    public void setValueCount(Long valueCount) {
        this.valueCount = valueCount;
    }

    @Override
    public CounterDTO clone() {
        final CounterDTO other = new CounterDTO();
        other.setId(getId());
        other.setName(getName());
        other.setContext(getContext());
        other.setValue(getValue());
        other.setValueCount(getValueCount());
        return other;
    }
}
