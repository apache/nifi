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

package org.apache.nifi.minifi;

import static org.apache.nifi.minifi.MiNiFiProperties.ValidatorNames.BOOLEAN_VALIDATOR;
import static org.apache.nifi.minifi.MiNiFiProperties.ValidatorNames.LONG_VALIDATOR;
import static org.apache.nifi.minifi.MiNiFiProperties.ValidatorNames.NON_NEGATIVE_INTEGER_VALIDATOR;
import static org.apache.nifi.minifi.MiNiFiProperties.ValidatorNames.PORT_VALIDATOR;
import static org.apache.nifi.minifi.MiNiFiProperties.ValidatorNames.TIME_PERIOD_VALIDATOR;
import static org.apache.nifi.minifi.MiNiFiProperties.ValidatorNames.VALID;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum MiNiFiProperties {
    JAVA("java", "java", false, false, VALID),
    RUN_AS("run.as", null, false, false, VALID),
    LIB_DIR("lib.dir", "./lib", false, false, VALID),
    CONF_DIR("conf.dir", "./conf", false, false, VALID),
    GRACEFUL_SHUTDOWN_SECOND("graceful.shutdown.seconds", "20", false, true, NON_NEGATIVE_INTEGER_VALIDATOR),
    NIFI_MINIFI_CONFIG("nifi.minifi.config", "./conf/config.yml", false, true, VALID),
    NIFI_MINIFI_SECURITY_KEYSTORE("nifi.minifi.security.keystore", null, false, false, VALID),
    NIFI_MINIFI_SECURITY_KEYSTORE_TYPE("nifi.minifi.security.keystoreType", null, false, false, VALID),
    NIFI_MINIFI_SECURITY_KEYSTORE_PASSWD("nifi.minifi.security.keystorePasswd", null, true, false, VALID),
    NIFI_MINIFI_SECURITY_KEY_PASSWD("nifi.minifi.security.keyPasswd", null, true, false, VALID),
    NIFI_MINIFI_SECURITY_TRUSTSTORE("nifi.minifi.security.truststore", null, false, false, VALID),
    NIFI_MINIFI_SECURITY_TRUSTSTORE_TYPE("nifi.minifi.security.truststoreType", null, false, false, VALID),
    NIFI_MINIFI_SECURITY_TRUSTSTORE_PASSWD("nifi.minifi.security.truststorePasswd", null, true, false, VALID),
    NIFI_MINIFI_SECURITY_SSL_PROTOCOL("nifi.minifi.security.ssl.protocol", null, false, false, VALID),
    NIFI_MINIFI_SENSITIVE_PROPS_KEY("nifi.minifi.sensitive.props.key", null, true, false, VALID),
    NIFI_MINIFI_SENSITIVE_PROPS_ALGORITHM("nifi.minifi.sensitive.props.algorithm", null, true, false, VALID),
    NIFI_MINIFI_PROVENANCE_REPORTING_COMMENT("nifi.minifi.provenance.reporting.comment", null, false, true, VALID),
    NIFI_MINIFI_PROVENANCE_REPORTING_SCHEDULING_STRATEGY("nifi.minifi.provenance.reporting.scheduling.strategy", null, false, true, VALID),
    NIFI_MINIFI_PROVENANCE_REPORTING_SCHEDULING_PERIOD("nifi.minifi.provenance.reporting.scheduling.period", null, false, true, TIME_PERIOD_VALIDATOR),
    NIFI_MINIFI_PROVENANCE_REPORTING_DESTINATION_URL("nifi.minifi.provenance.reporting.destination.url", null, false, true, VALID),
    NIFI_MINIFI_PROVENANCE_REPORTING_INPUT_PORT_NAME("nifi.minifi.provenance.reporting.input.port.name", null, false, true, VALID),
    NIFI_MINIFI_PROVENANCE_REPORTING_INSTANCE_URL("nifi.minifi.provenance.reporting.instance.url", null, false, true, VALID),
    NIFI_MINIFI_PROVENANCE_REPORTING_BATCH_SIZE("nifi.minifi.provenance.reporting.batch.size", null, false, true, NON_NEGATIVE_INTEGER_VALIDATOR),
    NIFI_MINIFI_PROVENANCE_REPORTING_COMMUNICATIONS_TIMEOUT("nifi.minifi.provenance.reporting.communications.timeout", null, false, true, TIME_PERIOD_VALIDATOR),
    NIFI_MINIFI_FLOW_USE_PARENT_SSL("nifi.minifi.flow.use.parent.ssl", null, false, true, VALID),
    NIFI_MINIFI_NOTIFIER_INGESTORS("nifi.minifi.notifier.ingestors", null, false, true, VALID),
    NIFI_MINIFI_NOTIFIER_INGESTORS_FILE_CONFIG_PATH("nifi.minifi.notifier.ingestors.file.config.path", null, false, true, VALID),
    NIFI_MINIFI_NOTIFIER_INGESTORS_FILE_POLLING_PERIOD_SECONDS("nifi.minifi.notifier.ingestors.file.polling.period.seconds", null, false, true, NON_NEGATIVE_INTEGER_VALIDATOR),
    NIFI_MINIFI_NOTIFIER_INGESTORS_RECEIVE_HTTP_PORT("nifi.minifi.notifier.ingestors.receive.http.port", null, false, true, PORT_VALIDATOR),
    NIFI_MINIFI_NOTIFIER_INGESTORS_PULL_HTTP_HOSTNAME("nifi.minifi.notifier.ingestors.pull.http.hostname", null, false, true, VALID),
    NIFI_MINIFI_NOTIFIER_INGESTORS_PULL_HTTP_PORT("nifi.minifi.notifier.ingestors.pull.http.port", null, false, true, PORT_VALIDATOR),
    NIFI_MINIFI_NOTIFIER_INGESTORS_PULL_HTTP_PATH("nifi.minifi.notifier.ingestors.pull.http.path", null, false, true, VALID),
    NIFI_MINIFI_NOTIFIER_INGESTORS_PULL_HTTP_QUERY("nifi.minifi.notifier.ingestors.pull.http.query", null, false, true, VALID),
    NIFI_MINIFI_NOTIFIER_INGESTORS_PULL_HTTP_PERIOD_MS("nifi.minifi.notifier.ingestors.pull.http.period.ms", null, false, true, NON_NEGATIVE_INTEGER_VALIDATOR),
    NIFI_MINIFI_STATUS_REPORTER_COMPONENTS("nifi.minifi.status.reporter.components", null, false, true, VALID),
    NIFI_MINIFI_STATUS_REPORTER_LOG_QUERY("nifi.minifi.status.reporter.log.query", null, false, true, VALID),
    NIFI_MINIFI_STATUS_REPORTER_LOG_LEVEL("nifi.minifi.status.reporter.log.level", null, false, true, VALID),
    NIFI_MINIFI_STATUS_REPORTER_LOG_PERIOD("nifi.minifi.status.reporter.log.period", null, false, true, VALID),
    JAVA_ARG_1("java.arg.1", null, false, true, VALID),
    JAVA_ARG_2("java.arg.2", null, false, true, VALID),
    JAVA_ARG_3("java.arg.3", null, false, true, VALID),
    JAVA_ARG_4("java.arg.4", null, false, true, VALID),
    JAVA_ARG_5("java.arg.5", null, false, true, VALID),
    JAVA_ARG_6("java.arg.6", null, false, true, VALID),
    JAVA_ARG_7("java.arg.7", null, false, true, VALID),
    JAVA_ARG_8("java.arg.8", null, false, true, VALID),
    JAVA_ARG_9("java.arg.9", null, false, true, VALID),
    JAVA_ARG_10("java.arg.10", null, false, true, VALID),
    JAVA_ARG_11("java.arg.11", null, false, true, VALID),
    JAVA_ARG_12("java.arg.12", null, false, true, VALID),
    JAVA_ARG_13("java.arg.13", null, false, true, VALID),
    JAVA_ARG_14("java.arg.14", null, false, true, VALID),
    C2_ENABLE("c2.enable", "false", false, true, BOOLEAN_VALIDATOR),
    C2_REST_URL("c2.rest.url", "", false, true, VALID),
    C2_REST_URL_ACK("c2.rest.url.ack", "", false, true, VALID),
    C2_REST_CONNECTION_TIMEOUT("c2.rest.connectionTimeout", "5 sec", false, true, TIME_PERIOD_VALIDATOR),
    C2_REST_READ_TIMEOUT("c2.rest.readTimeout", "5 sec", false, true, TIME_PERIOD_VALIDATOR),
    C2_REST_CALL_TIMEOUT("c2.rest.callTimeout", "10 sec", false, true, TIME_PERIOD_VALIDATOR),
    C2_MAX_IDLE_CONNECTIONS("c2.rest.maxIdleConnections", "5", false, true, NON_NEGATIVE_INTEGER_VALIDATOR),
    C2_KEEP_ALIVE_DURATION("c2.rest.keepAliveDuration", "5 min", false, true, TIME_PERIOD_VALIDATOR),
    C2_AGENT_HEARTBEAT_PERIOD("c2.agent.heartbeat.period", "1000", false, true, LONG_VALIDATOR),
    C2_AGENT_CLASS("c2.agent.class", "", false, true, VALID),
    C2_CONFIG_DIRECTORY("c2.config.directory", "./conf", false, true, VALID),
    C2_RUNTIME_MANIFEST_IDENTIFIER("c2.runtime.manifest.identifier", "", false, true, VALID),
    C2_RUNTIME_TYPE("c2.runtime.type", "", false, true, VALID),
    C2_AGENT_IDENTIFIER("c2.agent.identifier", null, false, true, VALID),
    C2_FULL_HEARTBEAT("c2.full.heartbeat", "true", false, true, BOOLEAN_VALIDATOR),
    C2_SECURITY_TRUSTSTORE_LOCATION("c2.security.truststore.location", "", false, false, VALID),
    C2_SECURITY_TRUSTSTORE_PASSWORD("c2.security.truststore.password", "", true, false, VALID),
    C2_SECURITY_TRUSTSTORE_TYPE("c2.security.truststore.type", "JKS", false, false, VALID),
    C2_SECURITY_KEYSTORE_LOCATION("c2.security.keystore.location", "", false, false, VALID),
    C2_SECURITY_KEYSTORE_PASSWORD("c2.security.keystore.password", "", true, false, VALID),
    C2_SECURITY_KEYSTORE_TYPE("c2.security.keystore.type", "JKS", false, false, VALID),
    C2_REQUEST_COMPRESSION("c2.request.compression", "none", false, true, VALID),
    C2_ASSET_DIRECTORY("c2.asset.directory", "./asset", false, true, VALID);

    public static final LinkedHashMap<String, MiNiFiProperties> PROPERTIES_BY_KEY = Arrays.stream(MiNiFiProperties.values())
        .sorted()
        .collect(Collectors.toMap(MiNiFiProperties::getKey, Function.identity(), (x, y) -> y, LinkedHashMap::new));

    private final String key;
    private final String defaultValue;
    private final boolean sensitive;
    private final boolean modifiable;
    private final ValidatorNames validator;

    MiNiFiProperties(String key, String defaultValue, boolean sensitive, boolean modifiable, ValidatorNames validator) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.sensitive = sensitive;
        this.modifiable = modifiable;
        this.validator = validator;
    }

    public String getKey() {
        return key;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public boolean isSensitive() {
        return sensitive;
    }

    public boolean isModifiable() {
        return modifiable;
    }

    public ValidatorNames getValidator() {
        return validator;
    }

    public enum ValidatorNames {
        VALID, BOOLEAN_VALIDATOR, LONG_VALIDATOR, NON_NEGATIVE_INTEGER_VALIDATOR, TIME_PERIOD_VALIDATOR, PORT_VALIDATOR
    }

}
