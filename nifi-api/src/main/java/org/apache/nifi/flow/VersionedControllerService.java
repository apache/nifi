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

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

public class VersionedControllerService extends VersionedConfigurableExtension {

    private List<ControllerServiceAPI> controllerServiceApis;

    private String annotationData;
    private ScheduledState scheduledState;
    private String bulletinLevel;

    @Schema(description = "Lists the APIs this Controller Service implements.")
    public List<ControllerServiceAPI> getControllerServiceApis() {
        return controllerServiceApis;
    }

    public void setControllerServiceApis(List<ControllerServiceAPI> controllerServiceApis) {
        this.controllerServiceApis = controllerServiceApis;
    }

    @Schema(description = "The annotation for the controller service. This is how the custom UI relays configuration to the controller service.")
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

    @Schema(description = "The ScheduledState denoting whether the Controller Service is ENABLED or DISABLED")
    public ScheduledState getScheduledState() {
        return scheduledState;
    }

    public void setScheduledState(final ScheduledState scheduledState) {
        this.scheduledState = scheduledState;
    }

    @Schema(description = "The level at which the controller service will report bulletins.")
    public String getBulletinLevel() {
        return bulletinLevel;
    }

    public void setBulletinLevel(String bulletinLevel) {
        this.bulletinLevel = bulletinLevel;
    }
}
