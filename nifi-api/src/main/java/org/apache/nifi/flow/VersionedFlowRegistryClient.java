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

package org.apache.nifi.flow;

import io.swagger.annotations.ApiModelProperty;

public class VersionedFlowRegistryClient extends VersionedConfigurableExtension {
    @Deprecated
    private String id;
    @Deprecated
    private String url;
    private String description;
    private String annotationData;

    @Override
    public ComponentType getComponentType() {
        return ComponentType.FLOW_REGISTRY_CLIENT;
    }

    /**
     * @deprecated use {@link #getIdentifier()} instead.
     */
    @Deprecated
    @ApiModelProperty("The ID of the Registry. This method is deprecated. Use #getIdentifier instead.")
    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    @Deprecated
    @ApiModelProperty("The URL for interacting with the registry")
    public String getUrl() {
        return url;
    }

    @Deprecated
    public void setUrl(final String url) {
        this.url = url;
    }

    @ApiModelProperty("The description of the registry")
    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    @ApiModelProperty(value = "The annotation for the reporting task. This is how the custom UI relays configuration to the reporting task.")
    public String getAnnotationData() {
        return annotationData;
    }

    public void setAnnotationData(String annotationData) {
        this.annotationData = annotationData;
    }
}
