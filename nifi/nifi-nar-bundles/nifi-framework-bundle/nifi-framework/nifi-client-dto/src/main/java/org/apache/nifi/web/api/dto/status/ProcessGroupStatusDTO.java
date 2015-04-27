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
package org.apache.nifi.web.api.dto.status;

import java.util.Collection;
import java.util.Date;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

/**
 * The status for a process group in this NiFi.
 */
@XmlType(name = "processGroupStatus")
public class ProcessGroupStatusDTO extends StatusDTO {

    private String id;
    private String name;
    private Collection<ConnectionStatusDTO> connectionStatus;
    private Collection<ProcessorStatusDTO> processorStatus;
    private Collection<ProcessGroupStatusDTO> processGroupStatus;
    private Collection<RemoteProcessGroupStatusDTO> remoteProcessGroupStatus;
    private Collection<PortStatusDTO> inputPortStatus;
    private Collection<PortStatusDTO> outputPortStatus;

    private String input;
    private String queuedCount;
    private String queuedSize;
    private String queued;
    private String read;
    private String written;
    private String output;
    private String transferred;
    private String received;
    private String sent;
    private Integer activeThreadCount;
    private Date statsLastRefreshed;

    /**
     * The id for the process group.
     *
     * @return The id for the process group
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return name of this process group
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return active thread count for this process group
     */
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
    }

    /**
     * The status of all connections in this process group.
     *
     * @return The status of all connections
     */
    public Collection<ConnectionStatusDTO> getConnectionStatus() {
        return connectionStatus;
    }

    public void setConnectionStatus(Collection<ConnectionStatusDTO> connectionStatus) {
        this.connectionStatus = connectionStatus;
    }

    /**
     * The status of all process groups in this process group.
     *
     * @return The status of all process groups
     */
    public Collection<ProcessGroupStatusDTO> getProcessGroupStatus() {
        return processGroupStatus;
    }

    public void setProcessGroupStatus(Collection<ProcessGroupStatusDTO> processGroupStatus) {
        this.processGroupStatus = processGroupStatus;
    }

    /**
     * The status of all remote process groups in this process group.
     *
     * @return The status of all remote process groups
     */
    public Collection<RemoteProcessGroupStatusDTO> getRemoteProcessGroupStatus() {
        return remoteProcessGroupStatus;
    }

    public void setRemoteProcessGroupStatus(final Collection<RemoteProcessGroupStatusDTO> remoteProcessGroupStatus) {
        this.remoteProcessGroupStatus = remoteProcessGroupStatus;
    }

    /**
     * The status of all processors in this process group.
     *
     * @return The status of all processors
     */
    public Collection<ProcessorStatusDTO> getProcessorStatus() {
        return processorStatus;
    }

    public void setProcessorStatus(Collection<ProcessorStatusDTO> processorStatus) {
        this.processorStatus = processorStatus;
    }

    /**
     * The status of all input ports in this process group.
     *
     * @return The status of all input ports
     */
    public Collection<PortStatusDTO> getInputPortStatus() {
        return inputPortStatus;
    }

    public void setInputPortStatus(Collection<PortStatusDTO> inputPortStatus) {
        this.inputPortStatus = inputPortStatus;
    }

    /**
     * The status of all output ports in this process group.
     *
     * @return The status of all output ports
     */
    public Collection<PortStatusDTO> getOutputPortStatus() {
        return outputPortStatus;
    }

    public void setOutputPortStatus(Collection<PortStatusDTO> outputPortStatus) {
        this.outputPortStatus = outputPortStatus;
    }

    /**
     * The output stats for this process group.
     *
     * @return The output stats
     */
    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    /**
     * The transferred stats for this process group. This represents the
     * count/size of flowfiles transferred to/from queues.
     *
     * @return The transferred status for this process group
     */
    public String getTransferred() {
        return transferred;
    }

    public void setTransferred(String transferred) {
        this.transferred = transferred;
    }

    /**
     * The received stats for this process group. This represents the count/size
     * of flowfiles received.
     *
     * @return The received stats for this process group
     */
    public String getReceived() {
        return received;
    }

    public void setReceived(String received) {
        this.received = received;
    }

    /**
     * The sent stats for this process group. This represents the count/size of
     * flowfiles sent.
     *
     * @return The sent stats for this process group
     */
    public String getSent() {
        return sent;
    }

    public void setSent(String sent) {
        this.sent = sent;
    }

    /**
     * The queued count for this process group.
     *
     * @return The queued count for this process group
     */
    public String getQueuedCount() {
        return queuedCount;
    }

    public void setQueuedCount(String queuedCount) {
        this.queuedCount = queuedCount;
    }

    /**
     * The queued size for this process group.
     *
     * @return The queued size for this process group
     */
    public String getQueuedSize() {
        return queuedSize;
    }

    public void setQueuedSize(String queuedSize) {
        this.queuedSize = queuedSize;
    }

    /**
     * The queued stats for this process group.
     *
     * @return The queued stats
     */
    public String getQueued() {
        return queued;
    }

    public void setQueued(String queued) {
        this.queued = queued;
    }

    /**
     * The read stats for this process group.
     *
     * @return The read stats
     */
    public String getRead() {
        return read;
    }

    public void setRead(String read) {
        this.read = read;
    }

    /**
     * The written stats for this process group.
     *
     * @return The written stats
     */
    public String getWritten() {
        return written;
    }

    public void setWritten(String written) {
        this.written = written;
    }

    /**
     * The input stats for this process group.
     *
     * @return The input stats
     */
    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    /**
     * When the status for this process group was calculated.
     *
     * @return The the status was calculated
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    public Date getStatsLastRefreshed() {
        return statsLastRefreshed;
    }

    public void setStatsLastRefreshed(Date statsLastRefreshed) {
        this.statsLastRefreshed = statsLastRefreshed;
    }

}
