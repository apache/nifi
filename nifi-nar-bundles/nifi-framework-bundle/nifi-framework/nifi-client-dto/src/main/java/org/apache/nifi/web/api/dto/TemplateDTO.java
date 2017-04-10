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
import org.apache.nifi.web.api.dto.util.DateTimeAdapter;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;

/**
 * Defines a template.
 */
@XmlRootElement(name = "template")
public class TemplateDTO {
    public static final String MAX_ENCODING_VERSION = "1.1";

    private String uri;

    private String id;
    private String groupId;
    private String name;
    private String description;
    private Date timestamp;
    private String encodingVersion;

    private FlowSnippetDTO snippet;

    /**
     * @return id for this template
     */
    @ApiModelProperty("The id of the template.")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @ApiModelProperty("The id of the Process Group that the template belongs to.")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }


    /**
     * @return uri for this template
     */
    @ApiModelProperty(
            value = "The URI for the template."
    )
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * @return name of this template
     */
    @ApiModelProperty(
            value = "The name of the template."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return description of this template
     */
    @ApiModelProperty(
            value = "The description of the template."
    )
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * @return timestamp when this template was created
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    @ApiModelProperty(
            value = "The timestamp when this template was created.",
            dataType = "string"
    )
    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * encodingVersion needs to be updated if the {@link TemplateDTO} changes.
     * @return encoding version of this template.
     */
    @XmlAttribute(name= "encoding-version")
    @ApiModelProperty(
            value = "The encoding version of this template."
    )
    public String getEncodingVersion() {
        return encodingVersion;
    }

    public void setEncodingVersion(String encodingVersion) {
        this.encodingVersion = encodingVersion;
    }

    /**
     * @return snippet in this template
     */
    @ApiModelProperty(
            value = "The contents of the template."
    )
    public FlowSnippetDTO getSnippet() {
        return snippet;
    }

    public void setSnippet(FlowSnippetDTO snippet) {
        this.snippet = snippet;
    }
}
