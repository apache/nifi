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
package org.apache.nifi.web.api.dto.action.details;

import com.wordnik.swagger.annotations.ApiModelProperty;
import javax.xml.bind.annotation.XmlType;

/**
 * Configuration details for an Action.
 */
@XmlType(name = "configureDetails")
public class ConfigureDetailsDTO extends ActionDetailsDTO {

    private String name;
    private String previousValue;
    private String value;

    /**
     * @return name of the property that was modified
     */
    @ApiModelProperty(
            value = "The name of the property that was modified."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return previous value
     */
    @ApiModelProperty(
            value = "The previous value."
    )
    public String getPreviousValue() {
        return previousValue;
    }

    public void setPreviousValue(String previousValue) {
        this.previousValue = previousValue;
    }

    /**
     * @return new value
     */
    @ApiModelProperty(
            value = "The new value."
    )
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
