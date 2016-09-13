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
package org.apache.nifi.reporting.datadog;

import com.codahale.metrics.MetricRegistry;
import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.transport.HttpTransport;
import org.coursera.metrics.datadog.transport.Transport;
import org.coursera.metrics.datadog.transport.UdpTransport;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

/**
 * Class configures MetricRegistry (passed outside or created from scratch) with Datadog support
 */
public class DDMetricRegistryBuilder {


    private MetricRegistry metricRegistry = null;
    private List<String> tags = Arrays.asList();
    private DatadogReporter datadogReporter;
    private String apiKey = "";
    private Transport transport;

    public DDMetricRegistryBuilder setMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        return this;
    }

    public DDMetricRegistryBuilder setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public DatadogReporter getDatadogReporter() {
        return datadogReporter;
    }

    public MetricRegistry build(String apiKey) throws IOException {
        if (metricRegistry == null)
            metricRegistry = new MetricRegistry();

        if (createTransport(apiKey)) {
            datadogReporter = createDatadogReporter(this.metricRegistry);
        }
        return this.metricRegistry;
    }

    private boolean createTransport(String apiKey) {
        //if api key was not changed
        if (this.apiKey.equals(apiKey)) {
            return false;
        } else if (apiKey.equals("agent")) {
            this.apiKey = "agent";
            transport = new UdpTransport.Builder().build();
            return true;
        } else {
            this.apiKey = apiKey;
            transport = new HttpTransport.Builder().withApiKey(apiKey).build();
            return true;
        }
    }

    /*
     * create DataDog reporter
     */
    private DatadogReporter createDatadogReporter(MetricRegistry metricRegistry) throws IOException {
        DatadogReporter reporter =
                DatadogReporter.forRegistry(metricRegistry)
                        .withHost(InetAddress.getLocalHost().getHostName())
                        .withTransport(transport)
                        .withTags(tags)
                        .build();
        return reporter;
    }
}