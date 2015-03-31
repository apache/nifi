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

import java.util.Set;
import javax.xml.bind.annotation.XmlType;

/**
 * Class used for providing documentation of a specified type.
 */
@XmlType(name = "documentedType")
public class DocumentedTypeDTO {

    private String type;
    private Set<DocumentedTypeDTO> childTypes;
    private String description;
    private Set<String> tags;

    /**
     * An optional description of the corresponding type.
     *
     * @return
     */
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * The type is the fully-qualified name of a Java class.
     *
     * @return
     */
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    /**
     * The tags associated with this type.
     *
     * @return
     */
    public Set<String> getTags() {
        return tags;
    }

    public void setTags(final Set<String> tags) {
        this.tags = tags;
    }

    /**
     * Child types for this type.
     * 
     * @return 
     */
    public Set<DocumentedTypeDTO> getChildTypes() {
        return childTypes;
    }

    public void setChildTypes(Set<DocumentedTypeDTO> childTypes) {
        this.childTypes = childTypes;
    }
    
}
