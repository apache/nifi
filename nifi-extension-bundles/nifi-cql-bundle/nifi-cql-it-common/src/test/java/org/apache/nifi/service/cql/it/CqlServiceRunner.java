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
package org.apache.nifi.service.cql.it;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.util.List;
import java.util.Map;

/**
 * Wires a {@link CQLExecutionService} onto a {@link TestRunner} for an integration test.
 *
 * <p>Every IT in this suite needs the same four lines - a runner over {@link MockCqlProcessor}, the service
 * added to it, and contact points/datacenter/keyspace set from a running container - before it can do
 * anything interesting, and then diverges: some enable the service and use it, others only call
 * {@code verify()} against a deliberately broken configuration. This collects the common part and leaves the
 * divergence to {@link #enable()} versus {@link #verify()}.
 *
 * <p>Properties are set in call order and later calls overwrite earlier ones, so a test wanting all the
 * usual connection settings except one writes {@code withConnection(info).withProperty(DATACENTER, "bogus")}
 * rather than restating the other two.
 */
public final class CqlServiceRunner {

    private static final String SERVICE_ID = "cql-session-provider";

    private final TestRunner runner = TestRunners.newTestRunner(new MockCqlProcessor());

    private final CQLExecutionService service;

    private CqlServiceRunner(final CQLExecutionService service) throws InitializationException {
        this.service = service;
        runner.addControllerService(SERVICE_ID, service);
    }

    public static CqlServiceRunner forService(final CQLExecutionService service) throws InitializationException {
        return new CqlServiceRunner(service);
    }

    /**
     * Sets contact points, datacenter and keyspace from a running container.
     */
    public CqlServiceRunner withConnection(final CqlConnectionInfo connectionInfo) {
        return withConnection(connectionInfo.contactPoint(), connectionInfo.datacenter(), connectionInfo.keyspace());
    }

    /**
     * As {@link #withConnection(CqlConnectionInfo)}, for a test holding the coordinates loose rather than as
     * a {@link CqlConnectionInfo} - the SSL and authentication suites provision their own containers and
     * never build one.
     */
    public CqlServiceRunner withConnection(final String contactPoint, final String datacenter, final String keyspace) {
        return withProperty(CQLExecutionService.CONTACT_POINTS, contactPoint)
                .withProperty(CQLExecutionService.DATACENTER, datacenter)
                .withProperty(CQLExecutionService.KEYSPACE, keyspace);
    }

    public CqlServiceRunner withProperty(final PropertyDescriptor descriptor, final String value) {
        runner.setProperty(service, descriptor, value);
        return this;
    }

    /**
     * The underlying runner, for the occasional test that has to wire a second controller service the CQL
     * service refers to (an SSL context, say) with setup too conditional to express here.
     */
    public TestRunner runner() {
        return runner;
    }

    /**
     * Enables the service and hands it back, ready to use.
     */
    public CQLExecutionService enable() {
        runner.enableControllerService(service);
        return service;
    }

    /**
     * Runs {@code verify()} against the configuration built so far, without enabling the service - the point
     * being that the configuration may be one that cannot work.
     */
    public List<ConfigVerificationResult> verify() {
        return runner.verify(service, Map.of());
    }
}
