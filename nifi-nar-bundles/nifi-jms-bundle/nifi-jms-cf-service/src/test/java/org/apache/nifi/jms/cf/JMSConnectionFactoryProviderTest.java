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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.ServiceLoader;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class JMSConnectionFactoryProviderTest {

    private static Logger logger = LoggerFactory.getLogger(JMSConnectionFactoryProviderTest.class);

    @Test
    public void validateFullConfigWithUserLib() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(Mockito.mock(Processor.class));
        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService("cfProvider", cfProvider);
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.BROKER_URI, "myhost:1234");

        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CLIENT_LIB_DIR_PATH,
                new File("test-lib").getAbsolutePath()); // see README in 'test-lib' dir for more info
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CONNECTION_FACTORY_IMPL,
                "org.apache.nifi.jms.testcflib.TestConnectionFactory");
        runner.setProperty(cfProvider, "Foo", "foo");
        runner.setProperty(cfProvider, "Bar", "3");

        runner.enableControllerService(cfProvider);
        runner.assertValid(cfProvider);
        ConnectionFactory cf = cfProvider.getConnectionFactory();
        assertNotNull(cf);
        assertEquals("org.apache.nifi.jms.testcflib.TestConnectionFactory", cf.getClass().getName());
        assertEquals("myhost", this.get("getHost", cf));
        assertEquals(1234, this.get("getPort", cf));
        assertEquals("foo", this.get("getFoo", cf));
        assertEquals(3, this.get("getBar", cf));
    }

    @Test(expected = AssertionError.class)
    public void validateOnConfigureFailsIfCNFonConnectionFactory() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(Mockito.mock(Processor.class));
        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService("cfProvider", cfProvider);
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.BROKER_URI, "myhost:1234");

        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CLIENT_LIB_DIR_PATH, "test-lib");
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CONNECTION_FACTORY_IMPL,
                "foo.bar.NonExistingConnectionFactory");
        runner.enableControllerService(cfProvider);
    }

    @Test
    public void validateNotValidForNonExistingLibPath() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(Mockito.mock(Processor.class));
        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService("cfProvider", cfProvider);
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.BROKER_URI, "myhost:1234");

        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CLIENT_LIB_DIR_PATH, "foo");
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CONNECTION_FACTORY_IMPL,
                "org.apache.nifi.jms.testcflib.TestConnectionFactory");
        runner.assertNotValid(cfProvider);
    }

    @Test(expected = AssertionError.class)
    public void validateFailsIfURINotHostPortAndNotActiveMQ() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(Mockito.mock(Processor.class));
        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService("cfProvider", cfProvider);
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.BROKER_URI, "myhost");

        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CLIENT_LIB_DIR_PATH, "test-lib");
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CONNECTION_FACTORY_IMPL,
                "org.apache.nifi.jms.testcflib.TestConnectionFactory");
        runner.enableControllerService(cfProvider);
        runner.assertNotValid(cfProvider);
    }

    @Test
    public void validateNotValidForNonDirectoryPath() throws Exception {
        TestRunner runner = TestRunners.newTestRunner(Mockito.mock(Processor.class));
        JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
        runner.addControllerService("cfProvider", cfProvider);
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.BROKER_URI, "myhost:1234");

        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CLIENT_LIB_DIR_PATH, "pom.xml");
        runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CONNECTION_FACTORY_IMPL,
                "org.apache.nifi.jms.testcflib.TestConnectionFactory");
        runner.assertNotValid(cfProvider);
    }

    @Test(expected = IllegalStateException.class)
    public void validateGetConnectionFactoryFailureIfServiceNotConfigured() throws Exception {
        new JMSConnectionFactoryProvider().getConnectionFactory();
    }

    /**
     * This test simply validates that {@link ConnectionFactory} can be setup by
     * pointing to the location of the client libraries at runtime. It uses
     * ActiveMQ which is not present at the POM but instead pulled from Maven
     * repo using {@link TestUtils#setupActiveMqLibForTesting(boolean)}, which
     * implies that for this test to run the computer must be connected to the
     * Internet. If computer is not connected to the Internet, this test will
     * quietly fail logging a message.
     */
    @Test
    public void validateFactoryCreationWithActiveMQLibraries() throws Exception {
        try {
            String libPath = TestUtils.setupActiveMqLibForTesting(true);

            TestRunner runner = TestRunners.newTestRunner(Mockito.mock(Processor.class));
            JMSConnectionFactoryProvider cfProvider = new JMSConnectionFactoryProvider();
            runner.addControllerService("cfProvider", cfProvider);
            runner.setProperty(cfProvider, JMSConnectionFactoryProvider.BROKER_URI,
                    "vm://localhost?broker.persistent=false");
            runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CLIENT_LIB_DIR_PATH, libPath);
            runner.setProperty(cfProvider, JMSConnectionFactoryProvider.CONNECTION_FACTORY_IMPL,
                    "org.apache.activemq.ActiveMQConnectionFactory");
            runner.enableControllerService(cfProvider);
            runner.assertValid(cfProvider);

            Connection connection = cfProvider.getConnectionFactory().createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination queue = session.createQueue("myqueue");
            MessageProducer producer = session.createProducer(queue);
            MessageConsumer consumer = session.createConsumer(queue);

            TextMessage message = session.createTextMessage("Hello");
            producer.send(message);
            assertEquals("Hello", ((TextMessage) consumer.receive()).getText());
            connection.stop();
            connection.close();
        } catch (Exception e) {
            logger.error("'validateFactoryCreationWithActiveMQLibraries' failed due to ", e);
        }
    }

    @Test
    public void validateServiceIsLocatableViaServiceLoader() {
        ServiceLoader<ControllerService> loader = ServiceLoader.<ControllerService> load(ControllerService.class);
        Iterator<ControllerService> iter = loader.iterator();
        boolean present = false;
        while (iter.hasNext()) {
            ControllerService cs = iter.next();
            assertTrue(cs instanceof JMSConnectionFactoryProviderDefinition);
            present = true;
        }
        assertTrue(present);
    }

    @SuppressWarnings("unchecked")
    private <T> T get(String methodName, ConnectionFactory cf) throws Exception {
        Method m = Utils.findMethod(methodName, cf.getClass());
        return (T) m.invoke(cf);
    }
}
