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
package org.apache.nifi.web.api.dto.flow;

import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.util.TimeAdapter;
import org.apache.nifi.web.api.entity.FlowBreadcrumbEntity;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;

/**
 * The NiFi flow starting at a given Process Group.
 */
@XmlType(name = "processGroupFlow")
public class ProcessGroupFlowDTO {

    private String id;
    private String uri;
    private String parentGroupId;
    private FlowBreadcrumbEntity breadcrumb;
    private FlowDTO flow;
    private Date lastRefreshed;

    /**
     * @return contents of this process group. This field will be populated if the request is marked verbose
     */
    @ApiModelProperty(
        value = "The flow structure starting at this Process Group."
    )
    public FlowDTO getFlow() {
        return flow;
    }

    public void setFlow(FlowDTO flow) {
        this.flow = flow;
    }

    /**
     * The id for this component.
     *
     * @return The id
     */
    @ApiModelProperty(
        value = "The id of the component."
    )
    public String getId() {
        return this.id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    /**
     * The breadcrumb for this ProcessGroup flow.
     *
     * @return The breadcrumb for this ProcessGroup flow
     */
    @ApiModelProperty(
        value = "The breadcrumb of the process group."
    )
    public FlowBreadcrumbEntity getBreadcrumb() {
        return breadcrumb;
    }

    public void setBreadcrumb(FlowBreadcrumbEntity breadcrumb) {
        this.breadcrumb = breadcrumb;
    }

    /**
     * @return id for the parent group of this component if applicable, null otherwise
     */
    @ApiModelProperty(
        value = "The id of parent process group of this component if applicable."
    )
    public String getParentGroupId() {
        return parentGroupId;
    }

    public void setParentGroupId(String parentGroupId) {
        this.parentGroupId = parentGroupId;
    }

    /**
     * The uri for linking to this component in this NiFi.
     *
     * @return The uri
     */
    @ApiModelProperty(
        value = "The URI for futures requests to the component."
    )
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty(
        value = "The time the flow for the process group was last refreshed.",
        dataType = "string"
    )
    public Date getLastRefreshed() {
        return lastRefreshed;
    }

    public void setLastRefreshed(Date lastRefreshed) {
        this.lastRefreshed = lastRefreshed;
    }
}
