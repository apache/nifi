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

package org.apache.nifi.web.api.dto.diagnostics;

import javax.xml.bind.annotation.XmlType;

import io.swagger.annotations.ApiModelProperty;

@XmlType(name = "jvmDiagnosticsSnapshot")
public class JVMDiagnosticsSnapshotDTO implements Cloneable {
    private JVMSystemDiagnosticsSnapshotDTO systemDiagnosticsDto;
    private JVMFlowDiagnosticsSnapshotDTO flowDiagnosticsDto;
    private JVMControllerDiagnosticsSnapshotDTO controllerDiagnosticsDto;

    @ApiModelProperty("System-related diagnostics information")
    public JVMSystemDiagnosticsSnapshotDTO getSystemDiagnosticsDto() {
        return systemDiagnosticsDto;
    }

    public void setSystemDiagnosticsDto(JVMSystemDiagnosticsSnapshotDTO systemDiagnosticsDto) {
        this.systemDiagnosticsDto = systemDiagnosticsDto;
    }

    @ApiModelProperty("Flow-related diagnostics information")
    public JVMFlowDiagnosticsSnapshotDTO getFlowDiagnosticsDto() {
        return flowDiagnosticsDto;
    }

    public void setFlowDiagnosticsDto(JVMFlowDiagnosticsSnapshotDTO flowDiagnosticsDto) {
        this.flowDiagnosticsDto = flowDiagnosticsDto;
    }

    @ApiModelProperty("Controller-related diagnostics information")
    public JVMControllerDiagnosticsSnapshotDTO getControllerDiagnostics() {
        return controllerDiagnosticsDto;
    }

    public void setControllerDiagnostics(JVMControllerDiagnosticsSnapshotDTO controllerDiagnostics) {
        this.controllerDiagnosticsDto = controllerDiagnostics;
    }


    @Override
    public JVMDiagnosticsSnapshotDTO clone() {
        final JVMDiagnosticsSnapshotDTO clone = new JVMDiagnosticsSnapshotDTO();
        clone.systemDiagnosticsDto = systemDiagnosticsDto == null ? null : systemDiagnosticsDto.clone();
        clone.flowDiagnosticsDto = flowDiagnosticsDto == null ? null : flowDiagnosticsDto.clone();
        clone.controllerDiagnosticsDto = controllerDiagnosticsDto == null ? null : controllerDiagnosticsDto.clone();
        return clone;
    }




    @XmlType(name = "versionInfo")
    public static class VersionInfoDTO implements Cloneable {

        private String niFiVersion;
        private String javaVendor;
        private String javaVersion;
        private String javaVmVendor;
        private String osName;
        private String osVersion;
        private String osArchitecture;

        @ApiModelProperty("The version of this NiFi.")
        public String getNiFiVersion() {
            return niFiVersion;
        }

        public void setNiFiVersion(String niFiVersion) {
            this.niFiVersion = niFiVersion;
        }

        @ApiModelProperty("Java vendor")
        public String getJavaVendor() {
            return javaVendor;
        }

        public void setJavaVendor(String javaVendor) {
            this.javaVendor = javaVendor;
        }

        @ApiModelProperty("Java VM Vendor")
        public String getJavaVmVendor() {
            return javaVmVendor;
        }

        public void setJavaVmVendor(String javaVmVendor) {
            this.javaVmVendor = javaVmVendor;
        }

        @ApiModelProperty("Java version")
        public String getJavaVersion() {
            return javaVersion;
        }

        public void setJavaVersion(String javaVersion) {
            this.javaVersion = javaVersion;
        }

        @ApiModelProperty("Host operating system name")
        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        @ApiModelProperty("Host operating system version")
        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @ApiModelProperty("Host operating system architecture")
        public String getOsArchitecture() {
            return osArchitecture;
        }

        public void setOsArchitecture(String osArchitecture) {
            this.osArchitecture = osArchitecture;
        }


        @Override
        public VersionInfoDTO clone() {
            final VersionInfoDTO other = new VersionInfoDTO();
            other.setNiFiVersion(getNiFiVersion());
            other.setJavaVendor(getJavaVendor());
            other.setJavaVersion(getJavaVersion());
            other.setOsName(getOsName());
            other.setOsVersion(getOsVersion());
            other.setOsArchitecture(getOsArchitecture());
            return other;
        }
    }
}
