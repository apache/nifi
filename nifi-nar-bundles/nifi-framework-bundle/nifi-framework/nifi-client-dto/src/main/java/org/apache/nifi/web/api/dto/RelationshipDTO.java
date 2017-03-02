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
 * Details of a relationship.
 */
@XmlType(name = "relationship")
public class RelationshipDTO {

    private String name;
    private String description;
    private Boolean autoTerminate;

    /**
     * @return the relationship name
     */
    @ApiModelProperty(
            value = "The relationship name."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the relationship description
     */
    @ApiModelProperty(
            value = "The relationship description."
    )
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * @return true if relationship is auto terminated;false otherwise
     */
    @ApiModelProperty(
            value = "Whether or not flowfiles sent to this relationship should auto terminate."
    )
    public Boolean isAutoTerminate() {
        return autoTerminate;
    }

    public void setAutoTerminate(Boolean autoTerminate) {
        this.autoTerminate = autoTerminate;
    }
}
