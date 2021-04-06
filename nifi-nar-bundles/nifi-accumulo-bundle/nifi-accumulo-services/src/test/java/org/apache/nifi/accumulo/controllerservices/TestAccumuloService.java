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
import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.junit.Before;
import org.junit.Test;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
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

    @Mock
    private KerberosCredentialsService credentialService;
    @Mock
    private Processor dummyProcessor;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);

        runner = TestRunners.newTestRunner(dummyProcessor);
        accumuloService = new AccumuloService();

        when(credentialService.getIdentifier()).thenReturn("1");
    }

    @Test
    public void testServiceValidWithAuthTypePasswordAndInstanceZookeeperUserPasswordAreSet() throws InitializationException {
        //given
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, PASSWORD);
        runner.setProperty(accumuloService, AccumuloService.ACCUMULO_USER, USER);
        runner.setProperty(accumuloService, AccumuloService.ACCUMULO_PASSWORD, PASSWORD);
        //when
        //then
        runner.assertValid(accumuloService);
    }

    @Test
    public void testServiceNotValidWithInstanceMissing() throws InitializationException {
        //given
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        //when
        //then
        assertServiceIsInvalidWithErrorMessage("Instance name must be supplied");
    }

    @Test
    public void testServiceNotValidWithZookeeperMissing() throws InitializationException {
        //given
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        //when
        //then
        assertServiceIsInvalidWithErrorMessage("Zookeepers must be supplied");
    }

    @Test
    public void testServiceNotValidWithAuthTypeNone() throws InitializationException {
        //given
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, NONE);
        //when
        //then
        assertServiceIsInvalidWithErrorMessage("Non supported Authentication type");
    }

    @Test
    public void testServiceNotValidWithAuthTypePasswordAndUserMissing() throws InitializationException {
        //given
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, PASSWORD);
        runner.setProperty(accumuloService, AccumuloService.ACCUMULO_PASSWORD, PASSWORD);
        //when
        //then
        assertServiceIsInvalidWithErrorMessage("Accumulo user must be supplied");
    }

    @Test
    public void testServiceNotValidWithAuthTypePasswordAndPasswordMissing() throws InitializationException {
        //given
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, PASSWORD);
        runner.setProperty(accumuloService, AccumuloService.ACCUMULO_USER, USER);
        //when
        //then
        assertServiceIsInvalidWithErrorMessage("Password must be supplied");
    }

    @Test
    public void testServiceNotValidWithAuthTypeKerberosAndKerberosPasswordAndCredentialServiceMissing() throws InitializationException {
        //given
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, KERBEROS);
        //when
        //then
        assertServiceIsInvalidWithErrorMessage("Either Kerberos Password or Kerberos Credential Service must be set");
    }

    @Test
    public void testServiceNotValidWithAuthTypeKerberosAndKerberosPrincipalMissing() throws InitializationException {
        //given
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, KERBEROS);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_PASSWORD, KERBEROS_PASSWORD);
        //when
        //then
        assertServiceIsInvalidWithErrorMessage("Kerberos Principal must be supplied");
    }

    @Test
    public void testServiceNotValidWithAuthTypeKerberosAndKerberosPasswordAndCredentialServiceSet() throws InitializationException {
        //given
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, KERBEROS);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_PASSWORD, KERBEROS_PASSWORD);
        runner.addControllerService("kerberos-credentials-service", credentialService);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_CREDENTIALS_SERVICE, credentialService.getIdentifier());
        //when
        //then
        assertServiceIsInvalidWithErrorMessage("should not be filled out at the same time");
    }

    @Test
    public void testServiceNotValidWithAuthTypeKerberosAndPrincipalAndCredentialServiceSet() throws InitializationException {
        //given
        runner.addControllerService("accumulo-connector-service", accumuloService);
        runner.setProperty(accumuloService, AccumuloService.INSTANCE_NAME, INSTANCE);
        runner.setProperty(accumuloService, AccumuloService.ZOOKEEPER_QUORUM, ZOOKEEPER);
        runner.setProperty(accumuloService, AccumuloService.AUTHENTICATION_TYPE, KERBEROS);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_PRINCIPAL, PRINCIPAL);
        runner.addControllerService("kerberos-credentials-service", credentialService);
        runner.setProperty(accumuloService, AccumuloService.KERBEROS_CREDENTIALS_SERVICE, credentialService.getIdentifier());
        //when
        //then
        assertServiceIsInvalidWithErrorMessage("Kerberos Principal (for password) should not be filled out");
    }

    private void assertServiceIsInvalidWithErrorMessage(String errorMessage) {
        Exception exception = assertThrows(IllegalStateException.class, () -> runner.enableControllerService(accumuloService));
        assertThat(exception.getMessage(), containsString(errorMessage));
    }
}