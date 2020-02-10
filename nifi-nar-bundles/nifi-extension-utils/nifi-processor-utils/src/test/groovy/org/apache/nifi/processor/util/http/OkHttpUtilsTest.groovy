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
package org.apache.nifi.processor.util.http

import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.components.PropertyValue
import org.apache.nifi.expression.ExpressionLanguageScope
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class OkHttpUtilsTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(OkHttpUtils.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {

    }

    @After
    void tearDown() throws Exception {

    }

    /**
     * Exercise the property value retrieval
     */
    @Test
    void testShouldGetPropertyValue() {
        // Arrange
        PDProcessor processor = new PDProcessor()

        final String HOST_VALUE = "hostValue"
        final String PORT_VALUE = "1234"
        final String USERNAME_VALUE = "nifi_user"
        final String PASSWORD_VALUE = "thisIsABadPassword"

        TestRunner runner = TestRunners.newTestRunner(processor)
        runner.setProperty(PDProcessor.HOST_NAME, HOST_VALUE)
        runner.setProperty(PDProcessor.PORT_NAME, PORT_VALUE)
        runner.setProperty(PDProcessor.USERNAME_NAME, USERNAME_VALUE)
        runner.setProperty(PDProcessor.PASSWORD_NAME, PASSWORD_VALUE)

        // Get a reference to the context directly
        ProcessContext context = runner.processContext

        def pdMap = [
                (OkHttpUtils.PROXY_HOST_NAME)    : PDProcessor.HOST_NAME,
                (OkHttpUtils.PROXY_PORT_NAME)    : PDProcessor.PORT_NAME,
                (OkHttpUtils.PROXY_USERNAME_NAME): PDProcessor.USERNAME_NAME,
                (OkHttpUtils.PROXY_PASSWORD_NAME): PDProcessor.PASSWORD_NAME,
        ]

        // Act
        PropertyValue proxyHost = OkHttpUtils.getPropertyValue(context, OkHttpUtils.PROXY_HOST_NAME, pdMap)
        PropertyValue proxyPort = OkHttpUtils.getPropertyValue(context, OkHttpUtils.PROXY_PORT_NAME, pdMap)
        PropertyValue proxyUsername = OkHttpUtils.getPropertyValue(context, OkHttpUtils.PROXY_USERNAME_NAME, pdMap)
        PropertyValue proxyPassword = OkHttpUtils.getPropertyValue(context, OkHttpUtils.PROXY_PASSWORD_NAME, pdMap)

        // Assert
        assert proxyHost.getValue() == HOST_VALUE
        assert proxyPort.getValue() == PORT_VALUE
        assert proxyUsername.getValue() == USERNAME_VALUE
        assert proxyPassword.getValue() == PASSWORD_VALUE
    }

    @Test
    void testGetPropertyValueShouldHandleNullValues() {
        // Arrange
        PDProcessor processor = new PDProcessor()

        TestRunner runner = TestRunners.newTestRunner(processor)
        runner.setProperty(PDProcessor.HOST_NAME, "")
        runner.setProperty(PDProcessor.PORT_NAME, "")
        // Rather than explicitly setting a null value for these properties, don't set them at all
//        runner.setProperty(PDProcessor.USERNAME_NAME, USERNAME_VALUE)
//        runner.setProperty(PDProcessor.PASSWORD_NAME, PASSWORD_VALUE)

        // Get a reference to the context directly
        ProcessContext context = runner.processContext

        def pdMap = [
                (OkHttpUtils.PROXY_HOST_NAME)    : PDProcessor.HOST_NAME,
                (OkHttpUtils.PROXY_PORT_NAME)    : PDProcessor.PORT_NAME,
                (OkHttpUtils.PROXY_USERNAME_NAME): PDProcessor.USERNAME_NAME,
                (OkHttpUtils.PROXY_PASSWORD_NAME): PDProcessor.PASSWORD_NAME,
        ]

        // Act
        PropertyValue proxyHost = OkHttpUtils.getPropertyValue(context, OkHttpUtils.PROXY_HOST_NAME, pdMap)
        PropertyValue proxyPort = OkHttpUtils.getPropertyValue(context, OkHttpUtils.PROXY_PORT_NAME, pdMap)
        PropertyValue proxyUsername = OkHttpUtils.getPropertyValue(context, OkHttpUtils.PROXY_USERNAME_NAME, pdMap)
        PropertyValue proxyPassword = OkHttpUtils.getPropertyValue(context, OkHttpUtils.PROXY_PASSWORD_NAME, pdMap)

        // Assert
        assert proxyHost.getValue() == ""
        assert proxyPort.getValue() == ""
        assert !proxyUsername.getValue()
        assert !proxyPassword.getValue()
    }

    @Test
    void testShouldGetPropertyValueWithExpressionLanguage() {
        // Arrange
        PDProcessor processor = new PDProcessor()

        final String HOST_VALUE = "\${literal(\"hostValue\"):repeat(2)}"

        TestRunner runner = TestRunners.newTestRunner(processor)
        runner.setProperty(PDProcessor.HOST_NAME, HOST_VALUE)

        // Get a reference to the context directly
        ProcessContext context = runner.processContext

        def pdMap = [
                (OkHttpUtils.PROXY_HOST_NAME): PDProcessor.HOST_NAME,
        ]

        // Act
        PropertyValue proxyHost = OkHttpUtils.getPropertyValue(context, OkHttpUtils.PROXY_HOST_NAME, pdMap)

        // Assert
        assert proxyHost.getValue() == "hostValuehostValue"
    }
}

class PDProcessor extends AbstractProcessor {
    private static final Logger logger = LoggerFactory.getLogger(PDProcessor.class)

    public static final String HOST_NAME = "proxy-host"
    public static final String PORT_NAME = "proxy-port"
    public static final String USERNAME_NAME = "proxy-username"
    public static final String PASSWORD_NAME = "proxy-password"

    PropertyDescriptor proxyHost = new PropertyDescriptor.Builder()
            .name(HOST_NAME)
            .displayName("Proxy Host")
            .required(false)
            .sensitive(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build()

    PropertyDescriptor proxyPort = new PropertyDescriptor.Builder()
            .name(PORT_NAME)
            .displayName("Proxy Port")
            .required(false)
            .sensitive(false)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .build()

    PropertyDescriptor proxyUsername = new PropertyDescriptor.Builder()
            .name(USERNAME_NAME)
            .displayName("Proxy Username")
            .required(false)
            .sensitive(false)
            .build()

    PropertyDescriptor proxyPassword = new PropertyDescriptor.Builder()
            .name(PASSWORD_NAME)
            .displayName("Proxy Password")
            .required(false)
            .sensitive(true)
            .build()


    List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        [proxyHost, proxyPort, proxyUsername, proxyPassword]
    }

    @Override
    void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        logger.info("Inside PDProcessor#onTrigger()")
    }
}