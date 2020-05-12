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

package org.apache.nifi.minifi.commons.status;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.minifi.commons.status.connection.ConnectionStatusBean;
import org.apache.nifi.minifi.commons.status.controllerservice.ControllerServiceStatus;
import org.apache.nifi.minifi.commons.status.instance.InstanceStatus;
import org.apache.nifi.minifi.commons.status.processor.ProcessorStatusBean;
import org.apache.nifi.minifi.commons.status.reportingTask.ReportingTaskStatus;
import org.apache.nifi.minifi.commons.status.rpg.RemoteProcessGroupStatusBean;
import org.apache.nifi.minifi.commons.status.system.SystemDiagnosticsStatus;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

public class FlowStatusReport implements java.io.Serializable {
    private List<ControllerServiceStatus> controllerServiceStatusList;
    private List<ProcessorStatusBean> processorStatusList;
    private List<ConnectionStatusBean> connectionStatusList;
    private List<RemoteProcessGroupStatusBean> remoteProcessGroupStatusList;
    private InstanceStatus instanceStatus;
    private SystemDiagnosticsStatus systemDiagnosticsStatus;
    private List<ReportingTaskStatus> reportingTaskStatusList;
    private List<String> errorsGeneratingReport;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public FlowStatusReport() {
    }

    public List<ControllerServiceStatus> getControllerServiceStatusList() {
        return controllerServiceStatusList;
    }

    public void setControllerServiceStatusList(List<ControllerServiceStatus> controllerServiceStatusList) {
        this.controllerServiceStatusList = controllerServiceStatusList;
    }

    public List<ProcessorStatusBean> getProcessorStatusList() {
        return processorStatusList;
    }

    public void setProcessorStatusList(List<ProcessorStatusBean> processorStatusList) {
        this.processorStatusList = processorStatusList;
    }

    public List<ConnectionStatusBean> getConnectionStatusList() {
        return connectionStatusList;
    }

    public void setConnectionStatusList(List<ConnectionStatusBean> connectionStatusList) {
        this.connectionStatusList = connectionStatusList;
    }

    public InstanceStatus getInstanceStatus() {
        return instanceStatus;
    }

    public void setInstanceStatus(InstanceStatus instanceStatus) {
        this.instanceStatus = instanceStatus;
    }

    public SystemDiagnosticsStatus getSystemDiagnosticsStatus() {
        return systemDiagnosticsStatus;
    }

    public void setSystemDiagnosticsStatus(SystemDiagnosticsStatus systemDiagnosticsStatus) {
        this.systemDiagnosticsStatus = systemDiagnosticsStatus;
    }

    public List<RemoteProcessGroupStatusBean> getRemoteProcessGroupStatusList() {
        return remoteProcessGroupStatusList;
    }

    public void setRemoteProcessGroupStatusList(List<RemoteProcessGroupStatusBean> remoteProcessGroupStatusList) {
        this.remoteProcessGroupStatusList = remoteProcessGroupStatusList;
    }

    public List<ReportingTaskStatus> getReportingTaskStatusList() {
        return reportingTaskStatusList;
    }

    public void setReportingTaskStatusList(List<ReportingTaskStatus> reportingTaskStatusList) {
        this.reportingTaskStatusList = reportingTaskStatusList;
    }

    public List<String> getErrorsGeneratingReport() {
        return errorsGeneratingReport;
    }

    public void setErrorsGeneratingReport(List<String> errorsGeneratingReport) {
        this.errorsGeneratingReport = errorsGeneratingReport;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FlowStatusReport that = (FlowStatusReport) o;

        if (getControllerServiceStatusList() != null ? !getControllerServiceStatusList().equals(that.getControllerServiceStatusList()) : that.getControllerServiceStatusList() != null) return false;
        if (getProcessorStatusList() != null ? !getProcessorStatusList().equals(that.getProcessorStatusList()) : that.getProcessorStatusList() != null) return false;
        if (getConnectionStatusList() != null ? !getConnectionStatusList().equals(that.getConnectionStatusList()) : that.getConnectionStatusList() != null) return false;
        if (getRemoteProcessGroupStatusList() != null ? !getRemoteProcessGroupStatusList().equals(that.getRemoteProcessGroupStatusList()) : that.getRemoteProcessGroupStatusList() != null)
            return false;
        if (getInstanceStatus() != null ? !getInstanceStatus().equals(that.getInstanceStatus()) : that.getInstanceStatus() != null) return false;
        if (getSystemDiagnosticsStatus() != null ? !getSystemDiagnosticsStatus().equals(that.getSystemDiagnosticsStatus()) : that.getSystemDiagnosticsStatus() != null) return false;
        if (getReportingTaskStatusList() != null ? !getReportingTaskStatusList().equals(that.getReportingTaskStatusList()) : that.getReportingTaskStatusList() != null) return false;
        return getErrorsGeneratingReport() != null ? getErrorsGeneratingReport().equals(that.getErrorsGeneratingReport()) : that.getErrorsGeneratingReport() == null;

    }

    @Override
    public int hashCode() {
        int result = getControllerServiceStatusList() != null ? getControllerServiceStatusList().hashCode() : 0;
        result = 31 * result + (getProcessorStatusList() != null ? getProcessorStatusList().hashCode() : 0);
        result = 31 * result + (getConnectionStatusList() != null ? getConnectionStatusList().hashCode() : 0);
        result = 31 * result + (getRemoteProcessGroupStatusList() != null ? getRemoteProcessGroupStatusList().hashCode() : 0);
        result = 31 * result + (getInstanceStatus() != null ? getInstanceStatus().hashCode() : 0);
        result = 31 * result + (getSystemDiagnosticsStatus() != null ? getSystemDiagnosticsStatus().hashCode() : 0);
        result = 31 * result + (getReportingTaskStatusList() != null ? getReportingTaskStatusList().hashCode() : 0);
        result = 31 * result + (getErrorsGeneratingReport() != null ? getErrorsGeneratingReport().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {

        StringWriter jsonString = new StringWriter();
        try(JsonGenerator generator = objectMapper.getFactory().createGenerator(jsonString)){
            generator.writeStartObject();
            generator.writeObjectField("controllerServiceStatusList", controllerServiceStatusList);
            generator.writeObjectField("processorStatusList", processorStatusList);
            generator.writeObjectField("connectionStatusList", connectionStatusList);
            generator.writeObjectField("remoteProcessGroupStatusList", remoteProcessGroupStatusList);
            generator.writeObjectField("instanceStatus", instanceStatus);
            generator.writeObjectField("systemDiagnosticsStatus", systemDiagnosticsStatus);
            generator.writeObjectField("reportingTaskStatusList", reportingTaskStatusList);
            generator.writeObjectField("errorsGeneratingReport", errorsGeneratingReport);
            generator.writeEndObject();
            generator.close();
        } catch (IOException e) {
            //this should not occur since we are using a StringWriter, however, in the event it does. Generate
            //the old style report
            return "FlowStatusReport{" +
                "controllerServiceStatusList=" + controllerServiceStatusList +
                ", processorStatusList=" + processorStatusList +
                ", connectionStatusList=" + connectionStatusList +
                ", remoteProcessGroupStatusList=" + remoteProcessGroupStatusList +
                ", instanceStatus=" + instanceStatus +
                ", systemDiagnosticsStatus=" + systemDiagnosticsStatus +
                ", reportingTaskStatusList=" + reportingTaskStatusList +
                ", errorsGeneratingReport=" + errorsGeneratingReport +
                '}';
        }
        return jsonString.toString();
    }
}
