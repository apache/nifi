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
package org.apache.nifi.accumulo.controllerservices;

import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestAccumuloService {

    private static final String INSTANCE = "instance";
    private static final String ZOOKEEPER = "zookeeper";
    private static final String PASSWORD = "PASSWORD";
    private static final String USER = "USER";
    private static final String KERBEROS = "KERBEROS";
    private static final String PRINCIPAL = "principal";
    private static final String KERBEROS_PASSWORD = "kerberos_password";
    private static final String NONE = "NONE";

    private TestRunner runner;
    private AccumuloService accumuloService;

    private final KerberosCredentialsService credentialService = mock(KerberosCredentialsService.class);
    private final KerberosUserService kerberosUserService = mock(KerberosUserService.class);
    private final Processor dummyProcessor = mock(Processor.class);

    @BeforeEach
    public void init() {
        runner = TestRunners.newTestRunner(dummyProcessor);
        accumuloService = new AccumuloService();

        when(credentialService.getIdentifier()).thenReturn("1");
        when(kerberosUserService.getIdentifier()).thenReturn("kerberosUserService1");
    }

    @Test
    public void testServiceValidWithAuthTypePasswordAndInstanceZookeeperUserPasswordAreSet() throws InitializationException {
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, PASSWORD);
        runner.setProperty(accumuloService, AccumuloService.ACCUMULO_USER, USER);
        runner.setProperty(accumuloService, AccumuloService.ACCUMULO_PASSWORD, PASSWORD);

        runner.assertValid(accumuloService);
    }

    @Test
    public void testServiceNotValidWithInstanceMissing() throws InitializationException {
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);

        assertServiceIsInvalidWithErrorMessage("Instance name must be supplied");
    }

    @Test
    public void testServiceNotValidWithZookeeperMissing() throws InitializationException {
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);

        assertServiceIsInvalidWithErrorMessage("Zookeepers must be supplied");
    }

    @Test
    public void testServiceNotValidWithAuthTypeNone() throws InitializationException {
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, NONE);

        assertServiceIsInvalidWithErrorMessage("Non supported Authentication type");
    }

    @Test
    public void testServiceNotValidWithAuthTypePasswordAndUserMissing() throws InitializationException {
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, PASSWORD);
        runner.setProperty(accumuloService, AccumuloService.ACCUMULO_PASSWORD, PASSWORD);

        assertServiceIsInvalidWithErrorMessage("Accumulo user must be supplied");
    }

    @Test
    public void testServiceNotValidWithAuthTypePasswordAndPasswordMissing() throws InitializationException {
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, PASSWORD);
        runner.setProperty(accumuloService, AccumuloService.ACCUMULO_USER, USER);

        assertServiceIsInvalidWithErrorMessage("Password must be supplied");
    }

    @Test
    public void testServiceNotValidWithAuthTypeKerberosAndKerberosPasswordAndCredentialServiceMissing() throws InitializationException {
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, KERBEROS);

        assertServiceIsInvalidWithErrorMessage("Either Kerberos Password, Kerberos Credential Service, or Kerberos User Service must be set");
    }

    @Test
    public void testServiceNotValidWithAuthTypeKerberosAndKerberosPrincipalMissing() throws InitializationException {
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, KERBEROS);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_PASSWORD, KERBEROS_PASSWORD);

        assertServiceIsInvalidWithErrorMessage("Kerberos Principal must be supplied");
    }

    @Test
    public void testServiceNotValidWithAuthTypeKerberosAndKerberosPasswordAndCredentialServiceSet() throws InitializationException {
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, KERBEROS);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_PASSWORD, KERBEROS_PASSWORD);
        runner.addControllerService("kerberos-credentials-service", credentialService);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_CREDENTIALS_SERVICE, credentialService.getIdentifier());

        assertServiceIsInvalidWithErrorMessage("should not be filled out at the same time");
    }

    @Test
    public void testServiceNotValidWithAuthTypeKerberosAndPrincipalAndCredentialServiceSet() throws InitializationException {
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, KERBEROS);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_PRINCIPAL, PRINCIPAL);
        runner.addControllerService("kerberos-credentials-service", credentialService);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_CREDENTIALS_SERVICE, credentialService.getIdentifier());

        assertServiceIsInvalidWithErrorMessage("Kerberos Principal (for password) should not be filled out");
    }

    @Test
    public void testServiceNotValidWithAuthTypeKerberosAndKerberosPasswordAndUserServiceSet() throws InitializationException {
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, KERBEROS);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_PRINCIPAL, PRINCIPAL);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_PASSWORD, KERBEROS_PASSWORD);
        runner.addControllerService("kerberos-user-service", kerberosUserService);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_USER_SERVICE, kerberosUserService.getIdentifier());

        assertServiceIsInvalidWithErrorMessage("should not be filled out at the same time");
    }

    @Test
    public void testServiceNotValidWithAuthTypeKerberosAndCredentialServiceAndUserServiceSet() throws InitializationException {
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, KERBEROS);

        runner.addControllerService("kerberos-credentials-service", credentialService);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_CREDENTIALS_SERVICE, credentialService.getIdentifier());

        runner.addControllerService("kerberos-user-service", kerberosUserService);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_USER_SERVICE, kerberosUserService.getIdentifier());

        assertServiceIsInvalidWithErrorMessage("Kerberos User Service cannot be specified while also specifying a Kerberos Credential Service");
    }

    @Test
    public void testServiceIsValidWithAuthTypeKerberosAndKerberosUserServiceSet() throws InitializationException {
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, KERBEROS);
        runner.addControllerService("kerberos-user-service", kerberosUserService);
        runner.enableControllerService(kerberosUserService);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_USER_SERVICE, kerberosUserService.getIdentifier());
        runner.assertValid(accumuloService);
    }

    private void assertServiceIsInvalidWithErrorMessage(String errorMessage) {
        Exception exception = assertThrows(IllegalStateException.class, () -> runner.enableControllerService(accumuloService));
        assertTrue(exception.getMessage().contains(errorMessage));
    }
}