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
package org.apache.nifi.c2.http;

import org.apache.nifi.c2.client.C2HttpClientBase;
import org.apache.nifi.c2.client.api.C2Properties;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class C2HttpClient extends C2HttpClientBase {
    private static final Logger logger = LoggerFactory.getLogger(C2HttpClient.class);

    public C2HttpClient(NiFiProperties niFiProperties) {
        super(generateProperties(niFiProperties));
    }

    private static Properties generateProperties(NiFiProperties niFiProperties) {
        Properties properties = new Properties();
        // TODO populate all C2 properties
        properties.put(C2Properties.C2_ENABLE_KEY, niFiProperties.getProperty(C2Properties.C2_ENABLE_KEY, "false"));
        properties.put(C2Properties.C2_REST_URL_KEY, niFiProperties.getProperty(C2Properties.C2_REST_URL_KEY, ""));
        properties.put(C2Properties.C2_REST_URL_ACK_KEY, niFiProperties.getProperty(C2Properties.C2_REST_URL_ACK_KEY, ""));
        properties.put(C2Properties.C2_AGENT_HEARTBEAT_PERIOD_KEY,
                niFiProperties.getProperty(C2Properties.C2_AGENT_HEARTBEAT_PERIOD_KEY, String.valueOf(C2Properties.C2_AGENT_DEFAULT_HEARTBEAT_PERIOD)));
        properties.put(C2Properties.C2_AGENT_CLASS_KEY, niFiProperties.getProperty(C2Properties.C2_AGENT_CLASS_KEY, ""));
        properties.put(C2Properties.C2_AGENT_IDENTIFIER_KEY, niFiProperties.getProperty(C2Properties.C2_AGENT_IDENTIFIER_KEY, ""));
        properties.put(TRUSTSTORE_LOCATION_KEY, niFiProperties.getProperty(TRUSTSTORE_LOCATION_KEY, ""));
        properties.put(TRUSTSTORE_PASSWORD_KEY, niFiProperties.getProperty(TRUSTSTORE_PASSWORD_KEY, ""));
        properties.put(TRUSTSTORE_TYPE_KEY, niFiProperties.getProperty(TRUSTSTORE_TYPE_KEY, ""));
        properties.put(KEYSTORE_LOCATION_KEY, niFiProperties.getProperty(KEYSTORE_LOCATION_KEY, ""));
        properties.put(KEYSTORE_PASSWORD_KEY, niFiProperties.getProperty(KEYSTORE_PASSWORD_KEY, ""));
        properties.put(KEYSTORE_TYPE_KEY, niFiProperties.getProperty(KEYSTORE_TYPE_KEY, ""));
        return properties;
    }
}
