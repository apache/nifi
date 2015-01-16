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
package org.apache.nifi.controller.status.history;

import java.util.Date;
import java.util.List;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;

/**
 * A repository for storing and retrieving components' historical status
 * information
 */
public interface ComponentStatusRepository {

    /**
     * Captures the status information provided in the given report
     *
     * @param rootGroupStatus
     */
    void capture(ProcessGroupStatus rootGroupStatus);

    /**
     * Captures the status information provided in the given report, providing a
     * timestamp that indicates the time at which the status report was
     * generated. This can be used to replay historical values.
     *
     * @param rootGroupStatus
     * @param timestamp
     */
    void capture(ProcessGroupStatus rootGroupStatus, Date timestamp);

    /**
     * Returns the Date at which the latest capture was performed
     *
     * @return
     */
    Date getLastCaptureDate();

    /**
     * Returns a {@link StatusHistory} that provides the status information
     * about the Connection with the given ID during the given time period.
     *
     * @param connectionId the ID of the Connection for which the Status is
     * desired
     * @param start the earliest date for which status information should be
     * returned; if <code>null</code>, the start date should be assumed to be
     * the beginning of time
     * @param end the latest date for which status information should be
     * returned; if <code>null</code>, the end date should be assumed to be the
     * current time
     * @param preferredDataPoints the preferred number of data points to return.
     * If the date range is large, the total number of data points could be far
     * too many to process. Therefore, this parameter allows the requestor to
     * indicate how many samples to return.
     * @return
     */
    StatusHistory getConnectionStatusHistory(String connectionId, Date start, Date end, int preferredDataPoints);

    /**
     * Returns a {@link StatusHistory} that provides the status information
     * about the Process Group with the given ID during the given time period.
     *
     * @param processGroupId
     * @param start the earliest date for which status information should be
     * returned; if <code>null</code>, the start date should be assumed to be
     * the beginning of time
     * @param end the latest date for which status information should be
     * returned; if <code>null</code>, the end date should be assumed to be the
     * current time
     * @param preferredDataPoints the preferred number of data points to return.
     * If the date range is large, the total number of data points could be far
     * too many to process. Therefore, this parameter allows the requestor to
     * indicate how many samples to return.
     * @return
     */
    StatusHistory getProcessGroupStatusHistory(String processGroupId, Date start, Date end, int preferredDataPoints);

    /**
     * Returns a {@link StatusHistory} that provides the status information
     * about the Processor with the given ID during the given time period.
     *
     * @param processorId
     * @param start the earliest date for which status information should be
     * returned; if <code>null</code>, the start date should be assumed to be
     * the beginning of time
     * @param end the latest date for which status information should be
     * returned; if <code>null</code>, the end date should be assumed to be the
     * current time
     * @param preferredDataPoints the preferred number of data points to return.
     * If the date range is large, the total number of data points could be far
     * too many to process. Therefore, this parameter allows the requestor to
     * indicate how many samples to return.
     * @return
     */
    StatusHistory getProcessorStatusHistory(String processorId, Date start, Date end, int preferredDataPoints);

    /**
     * Returns a {@link StatusHistory} that provides the status information
     * about the Remote Process Group with the given ID during the given time
     * period.
     *
     * @param remoteGroupId
     * @param start the earliest date for which status information should be
     * returned; if <code>null</code>, the start date should be assumed to be
     * the beginning of time
     * @param end the latest date for which status information should be
     * returned; if <code>null</code>, the end date should be assumed to be the
     * current time
     * @param preferredDataPoints the preferred number of data points to return.
     * If the date range is large, the total number of data points could be far
     * too many to process. Therefore, this parameter allows the requestor to
     * indicate how many samples to return.
     * @return
     */
    StatusHistory getRemoteProcessGroupStatusHistory(String remoteGroupId, Date start, Date end, int preferredDataPoints);

    /**
     * Returns a List of all {@link MetricDescriptor}s that are applicable to
     * Process Groups
     *
     * @return
     */
    List<MetricDescriptor<ProcessGroupStatus>> getProcessGroupMetricDescriptors();

    /**
     * Returns a List of all {@link MetricDescriptor}s that are applicable to
     * Processors
     *
     * @return
     */
    List<MetricDescriptor<ProcessorStatus>> getProcessorMetricDescriptors();

    /**
     * Returns a List of all {@link MetricDescriptor}s that are applicable to
     * Remote Process Groups
     *
     * @return
     */
    List<MetricDescriptor<RemoteProcessGroupStatus>> getRemoteProcessGroupMetricDescriptors();

    /**
     * Returns a List of all {@link MetricDescriptor}s that are applicable to
     * Connections
     *
     * @return
     */
    List<MetricDescriptor<ConnectionStatus>> getConnectionMetricDescriptors();

}
