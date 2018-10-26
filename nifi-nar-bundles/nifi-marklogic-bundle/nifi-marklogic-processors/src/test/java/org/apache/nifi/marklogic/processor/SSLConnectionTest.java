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
package org.apache.nifi.marklogic.processor;

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.marklogic.controller.DefaultMarkLogicDatabaseClientService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.StandardSSLContextService;
import org.junit.Before;
import org.junit.Test;

public class SSLConnectionTest extends AbstractMarkLogicProcessorTest {

    @Before
    public void setUp() throws InitializationException {
        initialize(new MockAbstractMarkLogicProcessor());
    }

    @Test
    public void testCreateClientWithSSL() throws Exception {
        final Map<String, String> sslProperties = new HashMap<String, String>();
        sslProperties.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/keystore.jks");
        sslProperties.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "passwordpassword");
        sslProperties.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        sslProperties.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/truststore.jks");
        sslProperties.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "passwordpassword");
        sslProperties.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");

        final StandardSSLContextService sslService = new StandardSSLContextService();
        runner.addControllerService("ssl-context-ml", sslService, sslProperties);

        runner.enableControllerService(sslService);
        runner.assertValid(sslService);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.DATABASE, database);
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.SSL_CONTEXT_SERVICE, "ssl-context-ml");
        runner.setProperty(service, DefaultMarkLogicDatabaseClientService.CLIENT_AUTH, "WANT");
        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    public static class MockAbstractMarkLogicProcessor extends AbstractMarkLogicProcessor {
        @Override
        public void onTrigger(ProcessContext arg0, ProcessSessionFactory arg1) throws ProcessException {

        }
    }
}
