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
package org.apache.nifi.jms.cf;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;

import static org.mockito.Mockito.mock;

/**
 *
 */
public class JMSConnectionFactoryProviderTest {

    private static Logger logger = LoggerFactory.getLogger(JMSConnectionFactoryProviderTest.class);

    @Test
    public void validateNotValidForNonExistingLibPath() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));
        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService("cfProvider", cfProvider);
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.BROKER_URI, "myhost:1234");

        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CLIENT_LIB_DIR_PATH, "foo");
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CONNECTION_FACTORY_IMPL,
                "org.apache.nifi.jms.testcflib.TestConnectionFactory");
        runner.assertNotValid(cfProvider);
    }

    @Test
    public void validateELExpression() throws InitializationException, URISyntaxException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));
        runner.setValidateExpressionUsage(true);
        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        String clientLib = this.getClass().getResource("/dummy-lib.jar").toURI().toString();
        runner.addControllerService("cfProvider", cfProvider);

        runner.setVariable("broker.uri", "tcp://0.0.0.0:616161");
        runner.setVariable("client.lib", clientLib);

        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.BROKER_URI, "${broker.uri}");
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CLIENT_LIB_DIR_PATH, "${client.lib}");
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CONNECTION_FACTORY_IMPL,
                "org.apache.nifi.jms.testcflib.TestConnectionFactory");
        runner.assertValid(cfProvider);
    }

    @Test
    public void testClientLibResourcesLoaded() throws InitializationException, URISyntaxException {
        TestRunner runner = TestRunners.newTestRunner(mock(Processor.class));
        runner.setValidateExpressionUsage(true);

        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();

        String clientLib = this.getClass().getResource("/dummy-lib.jar").toURI().toString() + "," +
                           this.getClass().getResource("/dummy-lib-2.jar").toURI().toString() + "," +
                           this.getClass().getResource("/dummy.conf").toURI().toString() + ",";

        runner.addControllerService("cfProvider", cfProvider);

        runner.setVariable("broker.uri", "tcp://0.0.0.0:616161");
        runner.setVariable("client.lib", clientLib);

        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.BROKER_URI, "${broker.uri}");
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CLIENT_LIB_DIR_PATH, "${client.lib}");
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CONNECTION_FACTORY_IMPL,
                "org.apache.nifi.jms.testcflib.TestConnectionFactory");

        runner.assertValid(cfProvider);

        ClassLoader loader = runner.getClass().getClassLoader();
        Assert.assertTrue(loader.getResource("dummy.conf") != null);
        Assert.assertTrue(loader.getResource("dummy-lib.jar") != null);
        Assert.assertTrue(loader.getResource("dummy-lib-2.jar") != null);
    }

    @Test(expected = IllegalStateException.class)
    public void validateGetConnectionFactoryFailureIfServiceNotConfigured() throws Exception {
        new JMSConnectionFactoryProvider().getConnectionFactory();
    }

}
