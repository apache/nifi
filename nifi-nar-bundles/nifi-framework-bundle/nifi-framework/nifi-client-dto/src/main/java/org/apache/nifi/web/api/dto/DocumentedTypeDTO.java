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
import java.util.Set;

/**
 * Class used for providing documentation of a specified type.
 */
@XmlType(name = "documentedType")
public class DocumentedTypeDTO {

    private String type;
    private String description;
    private String usageRestriction;
    private Set<String> tags;

    /**
     * @return An optional description of the corresponding type
     */
    @ApiModelProperty(
            value = "The description of the type."
    )
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * @return An optional description of why the usage of this component is restricted
     */
    @ApiModelProperty(
            value = "The description of why the usage of this component is restricted."
    )
    public String getUsageRestriction() {
        return usageRestriction;
    }

    public void setUsageRestriction(String usageRestriction) {
        this.usageRestriction = usageRestriction;
    }

    /**
     * @return The type is the fully-qualified name of a Java class
     */
    @ApiModelProperty(
            value = "The fully qualified name of the type."
    )
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * @return The tags associated with this type
     */
    @ApiModelProperty(
            value = "The tags associated with this type."
    )
    public Set<String> getTags() {
        return tags;
    }

    public void setTags(final Set<String> tags) {
        this.tags = tags;
    }

}
