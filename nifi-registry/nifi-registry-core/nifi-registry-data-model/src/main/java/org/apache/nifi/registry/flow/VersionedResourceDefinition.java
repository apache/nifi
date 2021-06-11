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

package org.apache.nifi.registry.flow;

import io.swagger.annotations.ApiModelProperty;

import java.util.Set;

public class VersionedResourceDefinition {
    private VersionedResourceCardinality cardinality;
    private Set<VersionedResourceType> resourceTypes;

    @ApiModelProperty("The cardinality of the resource")
    public VersionedResourceCardinality getCardinality() {
        return cardinality;
    }

    public void setCardinality(final VersionedResourceCardinality cardinality) {
        this.cardinality = cardinality;
    }

    @ApiModelProperty("The types of resource that the Property Descriptor is allowed to reference")
    public Set<VersionedResourceType> getResourceTypes() {
        return resourceTypes;
    }

    public void setResourceTypes(final Set<VersionedResourceType> resourceTypes) {
        this.resourceTypes = resourceTypes;
    }
}
