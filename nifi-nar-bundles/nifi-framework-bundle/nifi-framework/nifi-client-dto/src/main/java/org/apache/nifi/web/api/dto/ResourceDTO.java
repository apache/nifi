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
 * Resource that supports access/authorization policies.
 */
@XmlType(name = "resource")
public class ResourceDTO {

    private String identifier;
    private String name;

    /* getters / setters */
    /**
     * The name of the resource.
     *
     * @return The name of the resource
     */
    @ApiModelProperty(
            value = "The name of the resource."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * The identifier of the resource.
     *
     * @return The identifier of the resource
     */
    @ApiModelProperty(
            value = "The identifier of the resource."
    )
    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

}
