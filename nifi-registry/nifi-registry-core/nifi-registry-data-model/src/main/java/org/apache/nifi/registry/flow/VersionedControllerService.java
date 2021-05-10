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

import java.util.List;
import java.util.Map;

import io.swagger.annotations.ApiModelProperty;

public class VersionedControllerService extends VersionedComponent
        implements VersionedConfigurableComponent, VersionedExtensionComponent {

    private String type;
    private Bundle bundle;
    private List<ControllerServiceAPI> controllerServiceApis;

    private Map<String, String> properties;
    private Map<String, VersionedPropertyDescriptor> propertyDescriptors;
    private String annotationData;


    @Override
    @ApiModelProperty(value = "The type of the controller service.")
    public String getType() {
        return type;
    }

    @Override
    public void setType(String type) {
        this.type = type;
    }

    @Override
    @ApiModelProperty(value = "The details of the artifact that bundled this processor type.")
    public Bundle getBundle() {
        return bundle;
    }

    @Override
    public void setBundle(Bundle bundle) {
        this.bundle = bundle;
    }

    @ApiModelProperty(value = "Lists the APIs this Controller Service implements.")
    public List<ControllerServiceAPI> getControllerServiceApis() {
        return controllerServiceApis;
    }

    public void setControllerServiceApis(List<ControllerServiceAPI> controllerServiceApis) {
        this.controllerServiceApis = controllerServiceApis;
    }

    @Override
    @ApiModelProperty(value = "The properties of the controller service.")
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    @ApiModelProperty("The property descriptors for the processor.")
    public Map<String, VersionedPropertyDescriptor> getPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public void setPropertyDescriptors(Map<String, VersionedPropertyDescriptor> propertyDescriptors) {
        this.propertyDescriptors = propertyDescriptors;
    }

    @ApiModelProperty(value = "The annotation for the controller service. This is how the custom UI relays configuration to the controller service.")
    public String getAnnotationData() {
        return annotationData;
    }

    public void setAnnotationData(String annotationData) {
        this.annotationData = annotationData;
    }

    @Override
    public ComponentType getComponentType() {
        return ComponentType.CONTROLLER_SERVICE;
    }
}
