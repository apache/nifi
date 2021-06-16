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

package org.apache.nifi.minifi.commons.schema.common;

import org.apache.nifi.minifi.commons.schema.ProvenanceReportingSchema;
import org.apache.nifi.minifi.commons.schema.SecurityPropertiesSchema;
import org.apache.nifi.minifi.commons.schema.SensitivePropsSchema;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.minifi.commons.schema.RemoteProcessGroupSchema.TIMEOUT_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.COMMENT_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_PERIOD_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SCHEDULING_STRATEGY_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.USE_COMPRESSION_KEY;

public class BootstrapPropertyKeys {

    public static final String NOTIFIER_PROPERTY_PREFIX = "nifi.minifi.notifier";
    public static final String NOTIFIER_COMPONENTS_KEY = NOTIFIER_PROPERTY_PREFIX + ".components";

    public static final String STATUS_REPORTER_PROPERTY_PREFIX = "nifi.minifi.status.reporter";
    public static final String STATUS_REPORTER_COMPONENTS_KEY = STATUS_REPORTER_PROPERTY_PREFIX + ".components";

    public static final String USE_PARENT_SSL = "nifi.minifi.flow.use.parent.ssl";

    public static final String SECURITY_KEYSTORE_KEY = "nifi.minifi.security.keystore";
    public static final String SECURITY_KEYSTORE_TYPE_KEY = "nifi.minifi.security.keystoreType";
    public static final String SECURITY_KEYSTORE_PASSWORD_KEY = "nifi.minifi.security.keystorePasswd";
    public static final String SECURITY_KEY_PASSWORD_KEY = "nifi.minifi.security.keyPasswd";
    public static final String SECURITY_TRUSTSTORE_KEY = "nifi.minifi.security.truststore";
    public static final String SECURITY_TRUSTSTORE_TYPE_KEY = "nifi.minifi.security.truststoreType";
    public static final String SECURITY_TRUSTSTORE_PASSWORD_KEY = "nifi.minifi.security.truststorePasswd";
    public static final String SECURITY_SSL_PROTOCOL_KEY = "nifi.minifi.security.ssl.protocol";

    public static final String SENSITIVE_PROPS_KEY_KEY = "nifi.minifi.sensitive.props.key";
    public static final String SENSITIVE_PROPS_ALGORITHM_KEY = "nifi.minifi.sensitive.props.algorithm";
    public static final String SENSITIVE_PROPS_PROVIDER_KEY = "nifi.minifi.sensitive.props.provider";

    public static final Set<String> BOOTSTRAP_SECURITY_PROPERTY_KEYS = new HashSet<>(
            Arrays.asList(SECURITY_KEYSTORE_KEY,
                    SECURITY_KEYSTORE_TYPE_KEY,
                    SECURITY_KEYSTORE_PASSWORD_KEY,
                    SECURITY_KEY_PASSWORD_KEY,
                    SECURITY_TRUSTSTORE_KEY,
                    SECURITY_TRUSTSTORE_TYPE_KEY,
                    SECURITY_TRUSTSTORE_PASSWORD_KEY,
                    SECURITY_SSL_PROTOCOL_KEY));

    public static final Set<String> BOOTSTRAP_SENSITIVE_PROPERTY_KEYS = new HashSet<>(
            Arrays.asList(
                    SENSITIVE_PROPS_KEY_KEY,
                    SENSITIVE_PROPS_ALGORITHM_KEY,
                    SENSITIVE_PROPS_PROVIDER_KEY));

    public static final String PROVENANCE_REPORTING_COMMENT_KEY = "nifi.minifi.provenance.reporting.comment";
    public static final String PROVENANCE_REPORTING_SCHEDULING_STRATEGY_KEY = "nifi.minifi.provenance.reporting.scheduling.strategy";
    public static final String PROVENANCE_REPORTING_SCHEDULING_PERIOD_KEY = "nifi.minifi.provenance.reporting.scheduling.period";
    public static final String PROVENANCE_REPORTING_DESTINATION_URL_KEY = "nifi.minifi.provenance.reporting.destination.url";
    public static final String PROVENANCE_REPORTING_INPUT_PORT_NAME_KEY = "nifi.minifi.provenance.reporting.input.port.name";
    public static final String PROVENANCE_REPORTING_INSTANCE_URL_KEY = "nifi.minifi.provenance.reporting.instance.url";
    public static final String PROVENANCE_REPORTING_COMPRESS_EVENTS_KEY = "nifi.minifi.provenance.reporting.compress.events";
    public static final String PROVENANCE_REPORTING_BATCH_SIZE_KEY = "nifi.minifi.provenance.reporting.batch.size";
    public static final String PROVENANCE_REPORTING_COMMUNICATIONS_TIMEOUT_KEY = "nifi.minifi.provenance.reporting.communications.timeout";

    public static final Set<String> BOOTSTRAP_PROVENANCE_REPORTING_KEYS = new HashSet<>(
            Arrays.asList(PROVENANCE_REPORTING_COMMENT_KEY,
                    PROVENANCE_REPORTING_SCHEDULING_STRATEGY_KEY,
                    PROVENANCE_REPORTING_SCHEDULING_PERIOD_KEY,
                    PROVENANCE_REPORTING_DESTINATION_URL_KEY,
                    PROVENANCE_REPORTING_INPUT_PORT_NAME_KEY,
                    PROVENANCE_REPORTING_INSTANCE_URL_KEY,
                    PROVENANCE_REPORTING_COMPRESS_EVENTS_KEY,
                    PROVENANCE_REPORTING_BATCH_SIZE_KEY,
                    PROVENANCE_REPORTING_COMMUNICATIONS_TIMEOUT_KEY
            ));

    public static final Map<String, String> BOOTSTRAP_KEYS_TO_YML_KEYS;

    static {
        final Map<String, String> mutableMap = new HashMap<>();
        mutableMap.put(SECURITY_KEYSTORE_KEY, SecurityPropertiesSchema.KEYSTORE_KEY);
        mutableMap.put(SECURITY_KEYSTORE_TYPE_KEY, SecurityPropertiesSchema.KEYSTORE_TYPE_KEY);
        mutableMap.put(SECURITY_KEYSTORE_PASSWORD_KEY, SecurityPropertiesSchema.KEYSTORE_PASSWORD_KEY);
        mutableMap.put(SECURITY_KEY_PASSWORD_KEY, SecurityPropertiesSchema.KEY_PASSWORD_KEY);

        mutableMap.put(SECURITY_TRUSTSTORE_KEY, SecurityPropertiesSchema.TRUSTSTORE_KEY);
        mutableMap.put(SECURITY_TRUSTSTORE_TYPE_KEY, SecurityPropertiesSchema.TRUSTSTORE_TYPE_KEY);
        mutableMap.put(SECURITY_TRUSTSTORE_PASSWORD_KEY, SecurityPropertiesSchema.TRUSTSTORE_PASSWORD_KEY);

        mutableMap.put(SECURITY_SSL_PROTOCOL_KEY, SecurityPropertiesSchema.SSL_PROTOCOL_KEY);

        mutableMap.put(SENSITIVE_PROPS_KEY_KEY, SensitivePropsSchema.SENSITIVE_PROPS_KEY_KEY);
        mutableMap.put(SENSITIVE_PROPS_ALGORITHM_KEY, SensitivePropsSchema.SENSITIVE_PROPS_ALGORITHM_KEY);
        mutableMap.put(SENSITIVE_PROPS_PROVIDER_KEY, SensitivePropsSchema.SENSITIVE_PROPS_PROVIDER_KEY);

        mutableMap.put(PROVENANCE_REPORTING_COMMENT_KEY, COMMENT_KEY);
        mutableMap.put(PROVENANCE_REPORTING_SCHEDULING_STRATEGY_KEY, SCHEDULING_STRATEGY_KEY);
        mutableMap.put(PROVENANCE_REPORTING_SCHEDULING_PERIOD_KEY, SCHEDULING_PERIOD_KEY);
        mutableMap.put(PROVENANCE_REPORTING_DESTINATION_URL_KEY, ProvenanceReportingSchema.DESTINATION_URL_KEY);
        mutableMap.put(PROVENANCE_REPORTING_INPUT_PORT_NAME_KEY, ProvenanceReportingSchema.PORT_NAME_KEY);
        mutableMap.put(PROVENANCE_REPORTING_INSTANCE_URL_KEY, ProvenanceReportingSchema.ORIGINATING_URL_KEY);
        mutableMap.put(PROVENANCE_REPORTING_COMPRESS_EVENTS_KEY, USE_COMPRESSION_KEY);
        mutableMap.put(PROVENANCE_REPORTING_BATCH_SIZE_KEY, ProvenanceReportingSchema.BATCH_SIZE_KEY);
        mutableMap.put(PROVENANCE_REPORTING_COMMUNICATIONS_TIMEOUT_KEY, TIMEOUT_KEY);

        BOOTSTRAP_KEYS_TO_YML_KEYS = Collections.unmodifiableMap(mutableMap);
    }

}
