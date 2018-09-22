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
package org.apache.nifi.processors.standard;

import org.apache.nifi.processors.standard.util.TestPutTCPCommon;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.StandardSSLContextService;

import java.util.HashMap;
import java.util.Map;

public class TestPutTcpSSL extends TestPutTCPCommon {
    private static Map<String, String> sslProperties;

    public TestPutTcpSSL() {
        super();
        ssl = true;

        sslProperties = createSslProperties();
    }

    @Override
    public void configureProperties(String host, int port, String outgoingMessageDelimiter, boolean connectionPerFlowFile, boolean expectValid) throws InitializationException {
        runner.setProperty(PutTCP.HOSTNAME, host);
        runner.setProperty(PutTCP.PORT, Integer.toString(port));

        final StandardSSLContextService sslService = new StandardSSLContextService();
        runner.addControllerService("ssl-context", sslService, sslProperties);
        runner.enableControllerService(sslService);
        runner.setProperty(PutTCP.SSL_CONTEXT_SERVICE, "ssl-context");

        if (outgoingMessageDelimiter != null) {
            runner.setProperty(PutTCP.OUTGOING_MESSAGE_DELIMITER, outgoingMessageDelimiter);
        }
        runner.setProperty(PutTCP.CONNECTION_PER_FLOWFILE, String.valueOf(connectionPerFlowFile));

        if (expectValid) {
            runner.assertValid();
        } else {
            runner.assertNotValid();
        }
    }

    private static Map<String, String> createSslProperties() {
        final Map<String, String> map = new HashMap<>();
        map.put(StandardSSLContextService.KEYSTORE.getName(), "src/test/resources/keystore.jks");
        map.put(StandardSSLContextService.KEYSTORE_PASSWORD.getName(), "passwordpassword");
        map.put(StandardSSLContextService.KEYSTORE_TYPE.getName(), "JKS");
        map.put(StandardSSLContextService.TRUSTSTORE.getName(), "src/test/resources/truststore.jks");
        map.put(StandardSSLContextService.TRUSTSTORE_PASSWORD.getName(), "passwordpassword");
        map.put(StandardSSLContextService.TRUSTSTORE_TYPE.getName(), "JKS");
        return map;
    }
}
