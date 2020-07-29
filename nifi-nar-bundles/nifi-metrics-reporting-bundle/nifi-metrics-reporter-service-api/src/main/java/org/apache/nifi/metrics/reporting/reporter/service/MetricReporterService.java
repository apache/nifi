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
package org.apache.nifi.metrics.reporting.reporter.service;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * An interface for controller services used by MetricsReportingTask. In order to report to a new
 * client, implement this interface and make sure to return the desired implementation of {@link ScheduledReporter}.
 *
 * @author Omer Hadari
 */
public interface MetricReporterService extends ControllerService {

    /**
     * Create a reporter to a metric client (i.e. graphite).
     *
     * @param metricRegistry registry with the metrics to report.
     * @return an instance of the reporter.
     * @throws ProcessException if there was an error creating the reporter.
     */
    ScheduledReporter createReporter(MetricRegistry metricRegistry) throws ProcessException;
}
