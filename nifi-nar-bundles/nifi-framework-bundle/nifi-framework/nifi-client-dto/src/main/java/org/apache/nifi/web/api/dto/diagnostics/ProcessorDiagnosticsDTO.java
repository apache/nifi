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

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;

import javax.xml.bind.annotation.XmlType;
import java.util.List;
import java.util.Set;

@XmlType(name = "processorDiagnostics")
public class ProcessorDiagnosticsDTO {
    private ProcessorDTO processor;
    private ProcessorStatusDTO processorStatus;
    private Set<ControllerServiceDiagnosticsDTO> referencedControllerServices;
    private Set<ConnectionDiagnosticsDTO> incomingConnections;
    private Set<ConnectionDiagnosticsDTO> outgoingConnections;
    private JVMDiagnosticsDTO jvmDiagnostics;
    private List<ThreadDumpDTO> threadDumps;
    private ClassLoaderDiagnosticsDTO classLoaderDiagnostics;


    @ApiModelProperty("Information about the Processor for which the Diagnostic Report is generated")
    public ProcessorDTO getProcessor() {
        return processor;
    }

    public void setProcessor(ProcessorDTO processor) {
        this.processor = processor;
    }

    @ApiModelProperty("The Status for the Processor for which the Diagnostic Report is generated")
    public ProcessorStatusDTO getProcessorStatus() {
        return processorStatus;
    }

    public void setProcessorStatus(ProcessorStatusDTO processorStatus) {
        this.processorStatus = processorStatus;
    }

    @ApiModelProperty("Diagnostic Information about all Controller Services that the Processor is referencing")
    public Set<ControllerServiceDiagnosticsDTO> getReferencedControllerServices() {
        return referencedControllerServices;
    }

    public void setReferencedControllerServices(Set<ControllerServiceDiagnosticsDTO> referencedControllerServices) {
        this.referencedControllerServices = referencedControllerServices;
    }

    @ApiModelProperty("Diagnostic Information about all incoming Connections")
    public Set<ConnectionDiagnosticsDTO> getIncomingConnections() {
        return incomingConnections;
    }

    public void setIncomingConnections(Set<ConnectionDiagnosticsDTO> incomingConnections) {
        this.incomingConnections = incomingConnections;
    }

    @ApiModelProperty("Diagnostic Information about all outgoing Connections")
    public Set<ConnectionDiagnosticsDTO> getOutgoingConnections() {
        return outgoingConnections;
    }

    public void setOutgoingConnections(Set<ConnectionDiagnosticsDTO> outgoingConnections) {
        this.outgoingConnections = outgoingConnections;
    }

    @ApiModelProperty("Diagnostic Information about the JVM and system-level diagnostics")
    public JVMDiagnosticsDTO getJvmDiagnostics() {
        return jvmDiagnostics;
    }

    public void setJvmDiagnostics(JVMDiagnosticsDTO jvmDiagnostics) {
        this.jvmDiagnostics = jvmDiagnostics;
    }

    @ApiModelProperty("Thread Dumps that were taken of the threads that are active in the Processor")
    public List<ThreadDumpDTO> getThreadDumps() {
        return threadDumps;
    }

    public void setThreadDumps(List<ThreadDumpDTO> threadDumps) {
        this.threadDumps = threadDumps;
    }

    @ApiModelProperty("Information about the Controller Service's Class Loader")
    public ClassLoaderDiagnosticsDTO getClassLoaderDiagnostics() {
        return classLoaderDiagnostics;
    }

    public void setClassLoaderDiagnostics(ClassLoaderDiagnosticsDTO classLoaderDiagnostics) {
        this.classLoaderDiagnostics = classLoaderDiagnostics;
    }
}
