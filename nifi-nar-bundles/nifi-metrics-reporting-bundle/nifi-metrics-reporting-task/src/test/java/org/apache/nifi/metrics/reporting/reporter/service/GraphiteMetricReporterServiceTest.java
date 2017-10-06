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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.GraphiteSender;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class for {@link GraphiteMetricReporterService}.
 *
 * @author Omer Hadari
 */
@RunWith(MockitoJUnitRunner.class)
public class GraphiteMetricReporterServiceTest {

    /**
     * Service identifier for registerting the tested service to the tests runner.
     */
    private static final String SERVICE_IDENTIFIER = "graphite-metric-reporter-service";

    /**
     * Sample host name for the {@link GraphiteMetricReporterService#HOST} property.
     */
    private static final String TEST_HOST = "some-host";

    /**
     * Sample port for the {@link GraphiteMetricReporterService#PORT} property.
     */
    private static final int TEST_PORT = 12345;

    /**
     * Sample charset for the {@link GraphiteMetricReporterService#CHARSET} property.
     */
    private static final Charset TEST_CHARSET = StandardCharsets.UTF_16LE;

    /**
     * Sample prefix for metric names.
     */
    private static final String METRIC_NAMES_PREFIX = "test-metric-name-prefix";

    /**
     * Sample metric for verifying that a graphite sender with the correct configuration is used.
     */
    private static final String TEST_METRIC_NAME = "test-metric";

    /**
     * The fixed value of {@link #TEST_METRIC_NAME}.
     */
    private static final int TEST_METRIC_VALUE = 2;

    /**
     * Dummy processor for creating {@link #runner}.
     */
    @Mock
    private Processor processorDummy;

    /**
     * Mock sender for verifying creation with the correct configuration.
     */
    @Mock
    private GraphiteSender graphiteSenderMock;

    /**
     * Stub metric registry, that contains the test metrics.
     */
    private MetricRegistry metricRegistryStub;

    /**
     * Test runner for activating and configuring the service.
     */
    private TestRunner runner;

    /**
     * The test subject.
     */
    private GraphiteMetricReporterService testedService;

    /**
     * Instantiate the runner and mocks between tests. Register metrics to the {@link #metricRegistryStub}.
     */
    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(processorDummy);
        testedService = new GraphiteMetricReporterService();

        metricRegistryStub = new MetricRegistry();
        metricRegistryStub.register(TEST_METRIC_NAME, ((Gauge<Integer>) () -> TEST_METRIC_VALUE));

    }


    /**
     * Make sure that a correctly configured service can be activated.
     */
    @Test
    public void testGraphiteMetricReporterSanityConfiguration() throws Exception {
        runner.addControllerService(SERVICE_IDENTIFIER, testedService);
        setServiceProperties(TEST_HOST, TEST_PORT, TEST_CHARSET, METRIC_NAMES_PREFIX);
        runner.enableControllerService(testedService);

        runner.assertValid(testedService);
    }


    /**
     * Make sure that a correctly configured service provides a reporter for the matching configuration, and
     * actually reports to the correct address.
     */
    @Test
    public void testCreateReporterUsesCorrectSender() throws Exception {
        testedService = new TestableGraphiteMetricReporterService();
        runner.addControllerService(SERVICE_IDENTIFIER, testedService);
        setServiceProperties(TEST_HOST, TEST_PORT, TEST_CHARSET, METRIC_NAMES_PREFIX);
        when(graphiteSenderMock.isConnected()).thenReturn(false);
        runner.enableControllerService(testedService);

        ScheduledReporter createdReporter = testedService.createReporter(metricRegistryStub);
        createdReporter.report();

        String expectedMetricName = MetricRegistry.name(METRIC_NAMES_PREFIX, TEST_METRIC_NAME);
        verify(graphiteSenderMock).send(eq(expectedMetricName), eq(String.valueOf(TEST_METRIC_VALUE)), anyLong());
    }

    /**
     * Make sure that {@link GraphiteMetricReporterService#shutdown()} closes the connection to graphite.
     */
    @Test
    public void testShutdownClosesSender() throws Exception {
        testedService = new TestableGraphiteMetricReporterService();
        runner.addControllerService(SERVICE_IDENTIFIER, testedService);
        setServiceProperties(TEST_HOST, TEST_PORT, TEST_CHARSET, METRIC_NAMES_PREFIX);
        runner.enableControllerService(testedService);
        runner.disableControllerService(testedService);

        verify(graphiteSenderMock).close();
    }

    /**
     * Set the test subject's properties.
     *
     * @param host              populates {@link GraphiteMetricReporterService#HOST}.
     * @param port              populates {@link GraphiteMetricReporterService#PORT}.
     * @param charset           populates {@link GraphiteMetricReporterService#CHARSET}.
     * @param metricNamesPrefix populates {@link GraphiteMetricReporterService#METRIC_NAME_PREFIX}.
     */
    private void setServiceProperties(String host, int port, Charset charset, String metricNamesPrefix) {
        runner.setProperty(testedService, GraphiteMetricReporterService.HOST, host);
        runner.setProperty(testedService, GraphiteMetricReporterService.PORT, String.valueOf(port));
        runner.setProperty(testedService, GraphiteMetricReporterService.CHARSET, charset.name());
        runner.setProperty(testedService, GraphiteMetricReporterService.METRIC_NAME_PREFIX, metricNamesPrefix);
    }

    /**
     * This class is a patch. It overrides {@link GraphiteMetricReporterService#createSender(String, int, Charset)}
     * so that it is possible to verify a correct creation of graphite senders according to property values.
     */
    private class TestableGraphiteMetricReporterService extends GraphiteMetricReporterService {

        /**
         * Overrides the actual methods in order to inject the mock {@link #graphiteSenderMock}.
         * <p>
         * If this method is called with the test property values, it returns the mock. Otherwise operate
         * regularly.
         *
         * @param host    the provided hostname.
         * @param port    the provided port.
         * @param charset the provided graphite server charset.
         * @return {@link #graphiteSenderMock} if all params were the constant test params, regular result otherwise.
         */
        @Override
        protected GraphiteSender createSender(String host, int port, Charset charset) {
            if (TEST_HOST.equals(host) && TEST_PORT == port && TEST_CHARSET.equals(charset)) {
                return graphiteSenderMock;

            }
            return super.createSender(host, port, charset);
        }
    }
}
