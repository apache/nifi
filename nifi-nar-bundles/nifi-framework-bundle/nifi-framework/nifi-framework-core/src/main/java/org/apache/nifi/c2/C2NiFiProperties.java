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
package org.apache.nifi.c2;

import java.util.concurrent.TimeUnit;

public class C2NiFiProperties {

    public static final String C2_PREFIX = "c2.";

    public static final String C2_ENABLE_KEY = C2_PREFIX + "enable";
    public static final String C2_AGENT_PROTOCOL_KEY = C2_PREFIX + "agent.protocol.class";
    public static final String C2_COAP_HOST_KEY = C2_PREFIX + "agent.coap.host";
    public static final String C2_COAP_PORT_KEY = C2_PREFIX + "agent.coap.port";
    public static final String C2_CONFIG_DIRECTORY_KEY = C2_PREFIX + "config.directory";
    public static final String C2_RUNTIME_MANIFEST_IDENTIFIER_KEY = C2_PREFIX + "runtime.manifest.identifier";
    public static final String C2_RUNTIME_TYPE_KEY = C2_PREFIX + "runtime.type";
    public static final String C2_REST_URL_KEY = C2_PREFIX + "rest.url";
    public static final String C2_REST_URL_ACK_KEY = C2_PREFIX + "rest.url.ack";
    public static final String C2_ROOT_CLASSES_KEY = C2_PREFIX + "root.classes";
    public static final String C2_AGENT_HEARTBEAT_PERIOD_KEY = C2_PREFIX + "agent.heartbeat.period";
    public static final String C2_CONNECTION_TIMEOUT = C2_PREFIX + "rest.connectionTimeout";
    public static final String C2_READ_TIMEOUT = C2_PREFIX + "rest.readTimeout";
    public static final String C2_CALL_TIMEOUT = C2_PREFIX + "rest.callTimeout";
    public static final String C2_AGENT_CLASS_KEY = C2_PREFIX + "agent.class";
    public static final String C2_AGENT_IDENTIFIER_KEY = C2_PREFIX + "agent.identifier";

    public static final String C2_ROOT_CLASS_DEFINITIONS_KEY = C2_PREFIX + "root.class.definitions";
    public static final String C2_METRICS_NAME_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.name";
    public static final String C2_METRICS_METRICS_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics";
    public static final String C2_METRICS_METRICS_TYPED_METRICS_NAME_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics.typedmetrics.name";
    public static final String C2_METRICS_METRICS_QUEUED_METRICS_NAME_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics.queuemetrics.name";
    public static final String C2_METRICS_METRICS_QUEUE_METRICS_CLASSES_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics.queuemetrics.classes";
    public static final String C2_METRICS_METRICS_TYPED_METRICS_CLASSES_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics.typedmetrics.classes";
    public static final String C2_METRICS_METRICS_PROCESSOR_METRICS_NAME_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics.processorMetrics.name";
    public static final String C2_METRICS_METRICS_PROCESSOR_METRICS_CLASSES_KEY = C2_ROOT_CLASS_DEFINITIONS_KEY + ".metrics.metrics.processorMetrics.classes";

    /* C2 Client Security Properties */
    private static final String C2_REST_SECURITY_BASE_KEY = C2_PREFIX + "security";
    public static final String TRUSTSTORE_LOCATION_KEY = C2_REST_SECURITY_BASE_KEY + ".truststore.location";
    public static final String TRUSTSTORE_PASSWORD_KEY = C2_REST_SECURITY_BASE_KEY + ".truststore.password";
    public static final String TRUSTSTORE_TYPE_KEY = C2_REST_SECURITY_BASE_KEY + ".truststore.type";
    public static final String KEYSTORE_LOCATION_KEY = C2_REST_SECURITY_BASE_KEY + ".keystore.location";
    public static final String KEYSTORE_PASSWORD_KEY = C2_REST_SECURITY_BASE_KEY + ".keystore.password";
    public static final String KEYSTORE_TYPE_KEY = C2_REST_SECURITY_BASE_KEY + ".keystore.type";

    // Defaults
    // Heartbeat period of 1 second
    public static final long C2_AGENT_DEFAULT_HEARTBEAT_PERIOD = TimeUnit.SECONDS.toMillis(1);

    // Connection timeout of 5 seconds
    public static final String C2_DEFAULT_CONNECTION_TIMEOUT = "5 sec";
    // Read timeout of 5 seconds
    public static final String C2_DEFAULT_READ_TIMEOUT = "5 sec";
    // Call timeout of 10 seconds
    public static final String C2_DEFAULT_CALL_TIMEOUT = "10 sec";
}
