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
package org.apache.nifi.controller.metrics;

import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

/**
 * Standard implementation of Configuration Context properties for Component Metric Reporter
 */
public class StandardComponentMetricReporterConfigurationContext implements ComponentMetricReporterConfigurationContext {
    private final SSLContext sslContext;
    private final X509TrustManager trustManager;

    public StandardComponentMetricReporterConfigurationContext(final SSLContext sslContext, final X509TrustManager trustManager) {
        this.sslContext = sslContext;
        this.trustManager = trustManager;
    }

    @Override
    public Optional<SSLContext> getSSLContext() {
        return Optional.ofNullable(sslContext);
    }

    @Override
    public Optional<X509TrustManager> getTrustManager() {
        return Optional.ofNullable(trustManager);
    }
}
