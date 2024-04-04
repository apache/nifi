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
package org.apache.nifi.web.api.entity;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API. This particular entity holds a reference to a ControllerConfigurationDTO.
 */
@XmlRootElement(name = "controllerConfigurationEntity")
public class ControllerBulletinsEntity extends Entity {

    private List<BulletinEntity> bulletins;
    private List<BulletinEntity> controllerServiceBulletins;
    private List<BulletinEntity> reportingTaskBulletins;
    private List<BulletinEntity> flowAnalysisRuleBulletins;
    private List<BulletinEntity> parameterProviderBulletins;
    private List<BulletinEntity> flowRegistryClientBulletins;

    /**
     * @return System bulletins to be reported to the user
     */
    @Schema(description = "System level bulletins to be reported to the user.")
    public List<BulletinEntity> getBulletins() {
        return bulletins;
    }

    public void setBulletins(List<BulletinEntity> bulletins) {
        this.bulletins = bulletins;
    }

    /**
     * @return Controller service bulletins to be reported to the user
     */
    @Schema(description = "Controller service bulletins to be reported to the user.")
    public List<BulletinEntity> getControllerServiceBulletins() {
        return controllerServiceBulletins;
    }

    public void setControllerServiceBulletins(List<BulletinEntity> controllerServiceBulletins) {
        this.controllerServiceBulletins = controllerServiceBulletins;
    }

    /**
     * @return Reporting task bulletins to be reported to the user
     */
    @Schema(description = "Reporting task bulletins to be reported to the user.")
    public List<BulletinEntity> getReportingTaskBulletins() {
        return reportingTaskBulletins;
    }

    public void setReportingTaskBulletins(List<BulletinEntity> reportingTaskBulletins) {
        this.reportingTaskBulletins = reportingTaskBulletins;
    }

    /**
     * @return Flow Analysis Rule bulletins to be reported to the user
     */
    @Schema(description = "Flow Analysis Rule bulletins to be reported to the user.")
    public List<BulletinEntity> getFlowAnalysisRuleBulletins() {
        return flowAnalysisRuleBulletins;
    }

    public void setFlowAnalysisRuleBulletins(List<BulletinEntity> flowAnalysisRuleBulletins) {
        this.flowAnalysisRuleBulletins = flowAnalysisRuleBulletins;
    }

    /**
     * @return Parameter provider bulletins to be reported to the user
     */
    @Schema(description = "Parameter provider bulletins to be reported to the user.")
    public List<BulletinEntity> getParameterProviderBulletins() {
        return parameterProviderBulletins;
    }

    public void setParameterProviderBulletins(List<BulletinEntity> parameterProviderBulletins) {
        this.parameterProviderBulletins = parameterProviderBulletins;
    }

    /**
     * @return Flow registry client bulletins to be reported to the user
     */
    @Schema(description = "Flow registry client bulletins to be reported to the user.")
    public List<BulletinEntity> getFlowRegistryClientBulletins() {
        return flowRegistryClientBulletins;
    }

    public void setFlowRegistryClientBulletins(List<BulletinEntity> flowRegistryClientBulletins) {
        this.flowRegistryClientBulletins = flowRegistryClientBulletins;
    }

    @Override
    public ControllerBulletinsEntity clone() {
        final ControllerBulletinsEntity other = new ControllerBulletinsEntity();
        other.setBulletins(getBulletins() == null ? null : new ArrayList<>(getBulletins()));
        other.setControllerServiceBulletins(getControllerServiceBulletins() == null ? null : new ArrayList<>(getControllerServiceBulletins()));
        other.setReportingTaskBulletins(getReportingTaskBulletins() == null ? null : new ArrayList<>(getReportingTaskBulletins()));
        other.setFlowAnalysisRuleBulletins(getFlowAnalysisRuleBulletins() == null ? null : new ArrayList<>(getFlowAnalysisRuleBulletins()));
        other.setParameterProviderBulletins(getParameterProviderBulletins() == null ? null : new ArrayList<>(getParameterProviderBulletins()));
        other.setFlowRegistryClientBulletins(getFlowRegistryClientBulletins() == null ? null : new ArrayList<>(getFlowRegistryClientBulletins()));
        return other;
    }
}
