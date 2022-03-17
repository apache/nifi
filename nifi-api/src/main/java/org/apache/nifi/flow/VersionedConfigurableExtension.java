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

import java.util.Map;

public abstract class VersionedConfigurableExtension extends VersionedComponent
        implements VersionedExtensionComponent, VersionedConfigurableComponent {

    private String type;
    private Bundle bundle;

    private Map<String, String> properties;
    private Map<String, VersionedPropertyDescriptor> propertyDescriptors;

    @Override
    @ApiModelProperty("The type of the extension component")
    public String getType() {
        return type;
    }

    @Override
    public void setType(final String type) {
        this.type = type;
    }

    @Override
    @ApiModelProperty("Information about the bundle from which the component came")
    public Bundle getBundle() {
        return bundle;
    }

    @Override
    public void setBundle(Bundle bundle) {
        this.bundle = bundle;
    }


    @Override
    @ApiModelProperty("The properties for the component. Properties whose value is not set will only contain the property name.")
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    @ApiModelProperty("The property descriptors for the component.")
    public Map<String, VersionedPropertyDescriptor> getPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public void setPropertyDescriptors(Map<String, VersionedPropertyDescriptor> propertyDescriptors) {
        this.propertyDescriptors = propertyDescriptors;
    }

}
