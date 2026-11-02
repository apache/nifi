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
package org.apache.nifi.framework.configuration;

import org.apache.nifi.action.FlowActionReporter;
import org.apache.nifi.action.FlowActionReporterConfigurationContext;
import org.apache.nifi.action.StandardFlowActionReporterConfigurationContext;
import org.apache.nifi.controller.metrics.ComponentMetricReporter;
import org.apache.nifi.controller.metrics.ComponentMetricReporterConfigurationContext;
import org.apache.nifi.controller.metrics.DefaultComponentMetricReporter;
import org.apache.nifi.controller.metrics.StandardComponentMetricReporterConfigurationContext;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;

/**
 * Framework Extension Reporter Configuration independent of Flow Controller Configuration to avoid dependency resolution issues
 */
@Configuration
public class ReporterConfiguration {

    private static final String FLOW_ACTION_REPORTER_IMPLEMENTATION = "nifi.flow.action.reporter.implementation";

    private static final String COMPONENT_METRIC_REPORTER_IMPLEMENTATION = "nifi.component.metric.reporter.implementation";

    private NiFiProperties properties;

    private ExtensionDiscoveringManager extensionManager;

    private SSLContext sslContext;

    private X509TrustManager trustManager;

    @Autowired
    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    @Autowired
    public void setExtensionManager(final ExtensionDiscoveringManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    @Autowired(required = false)
    public void setSslContext(final SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    @Autowired(required = false)
    public void setTrustManager(final X509TrustManager trustManager) {
        this.trustManager = trustManager;
    }

    /**
     * Flow Action Reporter configured from NiFi Application Properties
     *
     * @return Flow Action Reporter
     */
    @Bean
    public FlowActionReporter flowActionReporter() {
        final String configuredClassName = properties.getProperty(FLOW_ACTION_REPORTER_IMPLEMENTATION);
        if (configuredClassName == null || configuredClassName.isBlank()) {
            return null;
        }

        try {
            final FlowActionReporter flowActionReporter = NarThreadContextClassLoader.createInstance(extensionManager, configuredClassName, FlowActionReporter.class, properties);
            final FlowActionReporterConfigurationContext configurationContext = new StandardFlowActionReporterConfigurationContext(sslContext, trustManager);
            flowActionReporter.onConfigured(configurationContext);
            return flowActionReporter;
        } catch (final Exception e) {
            throw new IllegalStateException("Failed to create FlowActionReporter with class [%s]".formatted(configuredClassName), e);
        }
    }

    /**
     * Component Metric Reporter configured from NiFi Application Properties
     *
     * @return Component Metric Reporter
     */
    @Bean
    public ComponentMetricReporter componentMetricReporter() {
        final ComponentMetricReporter componentMetricReporter;

        final String configuredClassName = properties.getProperty(COMPONENT_METRIC_REPORTER_IMPLEMENTATION);
        if (configuredClassName == null || configuredClassName.isBlank()) {
            componentMetricReporter = new DefaultComponentMetricReporter();
        } else {
            try {
                componentMetricReporter = NarThreadContextClassLoader.createInstance(extensionManager, configuredClassName, ComponentMetricReporter.class, properties);
                final ComponentMetricReporterConfigurationContext configurationContext = new StandardComponentMetricReporterConfigurationContext(sslContext, trustManager);
                componentMetricReporter.onConfigured(configurationContext);
            } catch (final Exception e) {
                throw new IllegalStateException("Failed to create ComponentMetricReporter with class [%s]".formatted(configuredClassName), e);
            }
        }

        return componentMetricReporter;
    }
}
