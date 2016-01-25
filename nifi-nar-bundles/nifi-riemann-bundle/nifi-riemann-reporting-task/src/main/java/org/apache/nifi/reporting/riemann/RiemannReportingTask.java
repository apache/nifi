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
package org.apache.nifi.reporting.riemann;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.riemann.metrics.MetricsService;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.client.IPromise;
import com.aphyr.riemann.client.RiemannClient;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.core.VirtualMachineMetrics;

@Tags({ "reporting", "riemann", "metrics" })
@DynamicProperty(name = "Attribute Name", value = "Attribute Value", supportsExpressionLanguage = false,
        description = "Additional attributes may be attached to the event by adding dynamic properties")
@CapabilityDescription("Publish NiFi metrics to Riemann. These metrics include " + "JVM, Processor, and General Data Flow metrics. In addition, you may also forward bulletin " + "board messages.")
public class RiemannReportingTask extends AbstractReportingTask {
    public static final PropertyDescriptor RIEMANN_HOST = new PropertyDescriptor.Builder().name("Riemann Address").description("Hostname of Riemann server").required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor RIEMANN_PORT = new PropertyDescriptor.Builder().name("Riemann Port").description("Port that Riemann is listening on").required(true).defaultValue("5555")
            .addValidator(StandardValidators.PORT_VALIDATOR).build();
    public static final PropertyDescriptor TRANSPORT_PROTOCOL = new PropertyDescriptor.Builder().name("Transport Protocol").description("Transport protocol to speak to Riemann in").required(true)
            .allowableValues(new Transport[] { Transport.TCP, Transport.UDP }).defaultValue("TCP").build();
    public static final PropertyDescriptor SERVICE_PREFIX = new PropertyDescriptor.Builder().name("Prefix for Service Name").description("Prefix to use when reporting to Riemann").defaultValue("nifi")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    public static final PropertyDescriptor WRITE_TIMEOUT = new PropertyDescriptor.Builder().name("Timeout").description("Timeout in milliseconds when writing events to Riemann").required(true)
            .defaultValue("500ms").addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();
    static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder().name("Hostname").description("The Hostname of this NiFi instance to report to Riemann").required(true)
            .expressionLanguageSupported(true).defaultValue("${hostname(true)}").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    static final PropertyDescriptor TAGS = new PropertyDescriptor.Builder().name("Tags").description("Comma separated list of tags to include ").required(true).expressionLanguageSupported(true)
            .defaultValue("nifi,metrics").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
    static final PropertyDescriptor SEND_JVM_METRICS = new PropertyDescriptor.Builder().name("JVM Metrics").description("Forwards NiFi JVM metrics to Riemann").allowableValues("true", "false")
            .required(true).defaultValue("true").addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();
    static final PropertyDescriptor SEND_NIFI_METRICS = new PropertyDescriptor.Builder().name("NiFi Metrics").description("Forwards aggregated data flow metrics to Riemann")
            .allowableValues("true", "false").required(true).defaultValue("true").addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();
    static final PropertyDescriptor SEND_PROCESSOR_METRICS = new PropertyDescriptor.Builder().name("Processor Metrics").description("Forwards metrics for individual processor to Riemann")
            .allowableValues("true", "false").required(true).defaultValue("true").addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();
    static final PropertyDescriptor SEND_BULLETIN_MESSAGES = new PropertyDescriptor.Builder().name("Bulletin Messages").description("Forwards messages from the Bulletin board to Riemann")
            .allowableValues("true", "false").required(true).defaultValue("true").addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();
    static final PropertyDescriptor MIN_BULLETIN_LEVEL = new PropertyDescriptor.Builder().name("Minimum Bulletin Level")
            .description("Only forward bulletin messages at this level and above to Riemann").allowableValues(LogLevel.values()).required(true).defaultValue("WARNING").build();
    protected volatile Transport transport;
    private volatile long lastObservedBulletinId = 0;
    private volatile RiemannClient riemannClient = null;
    private volatile MetricsService metricsService;
    private volatile VirtualMachineMetrics virtualMachineMetrics = VirtualMachineMetrics.getInstance();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = Lists.newArrayList();
        properties.add(RIEMANN_HOST);
        properties.add(RIEMANN_PORT);
        properties.add(TRANSPORT_PROTOCOL);
        properties.add(SERVICE_PREFIX);
        properties.add(HOSTNAME);
        properties.add(TAGS);
        properties.add(SEND_JVM_METRICS);
        properties.add(SEND_NIFI_METRICS);
        properties.add(SEND_PROCESSOR_METRICS);
        properties.add(SEND_BULLETIN_MESSAGES);
        properties.add(MIN_BULLETIN_LEVEL);
        properties.add(WRITE_TIMEOUT);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder().name(propertyDescriptorName).required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(false).dynamic(true)
                .build();
    }

    @OnStopped
    public final void cleanUpClient() {
        if (riemannClient != null) {
            this.riemannClient.close();
        }
        this.riemannClient = null;
    }

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) throws ProcessException {
        if (riemannClient == null || !riemannClient.isConnected()) {
            transport = Transport.valueOf(context.getProperty(TRANSPORT_PROTOCOL).getValue());
            String host = context.getProperty(RIEMANN_HOST).evaluateAttributeExpressions().getValue();
            int port = context.getProperty(RIEMANN_PORT).asInteger();
            RiemannClient client = null;
            try {
                switch (transport) {
                case TCP:
                    client = RiemannClient.tcp(host, port);
                    break;
                case UDP:
                    client = RiemannClient.udp(host, port);
                    break;
                }
                client.connect();
                riemannClient = client;
            } catch (IOException e) {
                if (client != null) {
                    client.close();
                }
                throw new ProcessException(String.format("Unable to connect to Riemann [%s:%d] (%s)\n%s", host, port, transport, e.getMessage()));
            }
        }
        metricsService = new MetricsService(context.getProperty(SERVICE_PREFIX).getValue(), context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue(),
                context.getProperty(TAGS).getValue().split(","));

        // Add dynamic properties as attributes to messages
        Map<String, String> additionalProperties = Maps.newHashMapWithExpectedSize(context.getProperties().size());
        for (PropertyDescriptor propertyDescriptor : context.getProperties().keySet()) {
            if (propertyDescriptor.isDynamic()) {
                additionalProperties.put(propertyDescriptor.getName(), context.getProperty(propertyDescriptor).getValue());
            }
        }
        metricsService.setAdditionalAttributes(additionalProperties);
    }

    /**
     * Retrieves all new bulletins in the `BulletinRepository` since the last time this method was called.
     *
     * @param context
     *          Reporting context
     * @return new bulletins
     */
    private List<Bulletin> getNewBulletins(ReportingContext context) {
        BulletinQuery.Builder bulletinQueryBuilder = new BulletinQuery.Builder().messageMatches(".*").after(lastObservedBulletinId);
        List<Bulletin> bulletins = context.getBulletinRepository().findBulletins(bulletinQueryBuilder.build());
        List<Bulletin> filteredBulletins = Lists.newArrayListWithCapacity(bulletins.size());
        int logLevelValue = LogLevel.valueOf(context.getProperty(MIN_BULLETIN_LEVEL).getValue()).getValue();
        if (bulletins.size() > 0) {
            for (Bulletin bulletin : bulletins) {
                if (LogLevel.valueOf(bulletin.getLevel()).getValue() >= logLevelValue) {
                    filteredBulletins.add(bulletin);
                }
            }
            for (Bulletin bulletin : bulletins) {
                lastObservedBulletinId = Math.max(lastObservedBulletinId, bulletin.getId());
            }
        }
        return filteredBulletins;
    }

    @Override
    public void onTrigger(ReportingContext context) {
        try {
            final long start = System.currentTimeMillis();

            List<IPromise<Proto.Msg>> promises = Lists.newArrayListWithCapacity(4);

            // Overall NiFi metrics
            if (context.getProperty(SEND_NIFI_METRICS).asBoolean()) {
                promises.add(riemannClient.sendEvents(metricsService.createProcessGroupEvents(context.getEventAccess().getControllerStatus())));
            }

            // Individual processor metrics
            if (context.getProperty(SEND_PROCESSOR_METRICS).asBoolean()) {
                promises.add(riemannClient.sendEvents(metricsService.createProcessorEvents(context.getEventAccess().getControllerStatus().getProcessorStatus())));
            }

            // Bulletin repository messages
            if (context.getProperty(SEND_BULLETIN_MESSAGES).asBoolean()) {
                List<Bulletin> bulletins = getNewBulletins(context);
                if (bulletins.size() > 0) {
                    getLogger().info("Forwarding " + bulletins.size() + " bulletin messages to Riemann...");
                    promises.add(riemannClient.sendEvents(metricsService.createBulletinEvents(bulletins)));
                }
            }

            // JVM metrics
            if (context.getProperty(SEND_JVM_METRICS).asBoolean()) {
                promises.add(riemannClient.sendEvents(metricsService.createJVMEvents(virtualMachineMetrics)));
            }

            // Block until all messages are sent - TCP only
            if (transport == Transport.TCP) {
                for (IPromise<Proto.Msg> promise : promises) {
                    Proto.Msg returnMessage = promise.deref(context.getProperty(WRITE_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
                    if (returnMessage == null) {
                        throw new ProcessException("Timed out writing to Riemann!");
                    }
                }
            }

            // Flush client
            riemannClient.flush();

            final long completedMillis = TimeUnit.NANOSECONDS.toMillis(System.currentTimeMillis() - start);
            getLogger().info("Successfully sent metrics to Riemann in {} ms", new Object[] { completedMillis });
        } catch (IOException e) {
            getLogger().error("Error sending events to Riemann", e);
        }
    }

    protected enum Transport {
        TCP, UDP
    }

    protected enum LogLevel {
        INFO(1), WARNING(2), ERROR(3), DEBUG(4), TRACE(5), FATAL(6);

        private final int value;

        LogLevel(final int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }
}
