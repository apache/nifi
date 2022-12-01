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

package org.apache.nifi.kafka.connect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.nifi.kafka.connect.validators.ConnectDirectoryExistsValidator;
import org.apache.nifi.kafka.connect.validators.ConnectHttpUrlValidator;
import org.apache.nifi.kafka.connect.validators.FlowSnapshotValidator;
import org.apache.nifi.stateless.config.ParameterOverride;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.common.config.ConfigDef.NonEmptyStringWithoutControlChars.nonEmptyStringWithoutControlChars;

public abstract class StatelessNiFiCommonConfig extends AbstractConfig {
    private static final Logger logger = LoggerFactory.getLogger(StatelessNiFiCommonConfig.class);
    public static final String NAR_DIRECTORY = "nar.directory";
    public static final String EXTENSIONS_DIRECTORY = "extensions.directory";
    public static final String WORKING_DIRECTORY = "working.directory";
    public static final String FLOW_SNAPSHOT = "flow.snapshot";
    public static final String KRB5_FILE = "krb5.file";
    public static final String NEXUS_BASE_URL = "nexus.url";
    public static final String DATAFLOW_TIMEOUT = "dataflow.timeout";
    public static final String DATAFLOW_NAME = "name";
    public static final String TRUSTSTORE_FILE = "security.truststore";
    public static final String TRUSTSTORE_TYPE = "security.truststoreType";
    public static final String TRUSTSTORE_PASSWORD = "security.truststorePasswd";
    public static final String KEYSTORE_FILE = "security.keystore";
    public static final String KEYSTORE_TYPE = "security.keystoreType";
    public static final String KEYSTORE_PASSWORD = "security.keystorePasswd";
    public static final String KEY_PASSWORD = "security.keyPasswd";
    public static final String SENSITIVE_PROPS_KEY = "sensitive.props.key";
    public static final String BOOTSTRAP_SNAPSHOT_URL = "nifi.stateless.flow.snapshot.url";
    public static final String BOOTSTRAP_SNAPSHOT_FILE = "nifi.stateless.flow.snapshot.file";
    public static final String BOOTSTRAP_SNAPSHOT_CONTENTS = "nifi.stateless.flow.snapshot.contents";
    public static final String BOOTSTRAP_FLOW_NAME = "nifi.stateless.flow.name";
    public static final String DEFAULT_KRB5_FILE = "/etc/krb5.conf";
    public static final String DEFAULT_DATAFLOW_TIMEOUT = "60 sec";
    public static final File DEFAULT_WORKING_DIRECTORY = new File("/tmp/nifi-stateless-working");
    public static final File DEFAULT_EXTENSIONS_DIRECTORY = new File("/tmp/nifi-stateless-extensions");
    public static final String DEFAULT_SENSITIVE_PROPS_KEY = "nifi-stateless";
    public static final String FLOW_GROUP = "Flow";
    public static final String DIRECTORIES_GROUP = "Directories";
    public static final String TLS_GROUP = "TLS";
    public static final String KERBEROS_GROUP = "Kerberos";
    public static final String NEXUS_GROUP = "Nexus";
    public static final String SECURITY_GROUP = "Security";
    public static final String RECORD_GROUP = "Record";

    protected static final Pattern PARAMETER_WITH_CONTEXT_PATTERN = Pattern.compile("parameter\\.(.*?):(.*)");
    protected static final Pattern PARAMETER_WITHOUT_CONTEXT_PATTERN = Pattern.compile("parameter\\.(.*)");

    protected StatelessNiFiCommonConfig(ConfigDef definition, Map<?, ?> originals, Map<String, ?> configProviderProps, boolean doLog) {
        super(definition, originals, configProviderProps, doLog);
    }

    protected StatelessNiFiCommonConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    protected StatelessNiFiCommonConfig(ConfigDef definition, Map<?, ?> originals, boolean doLog) {
        super(definition, originals, doLog);
    }

    public String getNarDirectory() {
        return getString(NAR_DIRECTORY);
    }

    public String getExtensionsDirectory() {
        return getString(EXTENSIONS_DIRECTORY);
    }

    public String getWorkingDirectory() {
        return getString(WORKING_DIRECTORY);
    }

    public String getDataflowName() {
        return getString(DATAFLOW_NAME);
    }

    public String getKrb5File() {
        return getString(KRB5_FILE);
    }

    public String getNexusBaseUrl() {
        return getString(NEXUS_BASE_URL);
    }

    public String getDataflowTimeout() {
        return getString(DATAFLOW_TIMEOUT);
    }

    public String getKeystoreFile() {
        return getString(KEYSTORE_FILE);
    }

    public String getKeystoreType() {
        return getString(KEYSTORE_TYPE);
    }

    public String getKeystorePassword() {
        return getOptionalPassword(KEYSTORE_PASSWORD);
    }

    public String getKeystoreKeyPassword() {
        return getOptionalPassword(KEY_PASSWORD);
    }

    public String getTruststoreFile() {
        return getString(TRUSTSTORE_FILE);
    }

    public String getTruststoreType() {
        return getString(TRUSTSTORE_TYPE);
    }

    public String getTruststorePassword() {
        return getOptionalPassword(TRUSTSTORE_PASSWORD);
    }

    public String getSensitivePropsKey() {
        return getOptionalPassword(SENSITIVE_PROPS_KEY);
    }

    /**
     * Populates the properties with the data flow definition parameters
     *
     * @param dataflowDefinitionProperties The properties to populate.
     */
    public void setFlowDefinition(final Map<String, String> dataflowDefinitionProperties) {
        String configuredFlowSnapshot = getString(FLOW_SNAPSHOT);
        if (configuredFlowSnapshot.startsWith("http://") || configuredFlowSnapshot.startsWith("https://")) {
            logger.debug("Configured Flow Snapshot appears to be a URL. Will use {} property to configured Stateless NiFi", StatelessNiFiCommonConfig.BOOTSTRAP_SNAPSHOT_URL);
            dataflowDefinitionProperties.put(StatelessNiFiCommonConfig.BOOTSTRAP_SNAPSHOT_URL, configuredFlowSnapshot);
        } else if (configuredFlowSnapshot.trim().startsWith("{")) {
            logger.debug("Configured Flow Snapshot appears to be JSON. Will use {} property to configured Stateless NiFi", StatelessNiFiCommonConfig.BOOTSTRAP_SNAPSHOT_CONTENTS);
            dataflowDefinitionProperties.put(StatelessNiFiCommonConfig.BOOTSTRAP_SNAPSHOT_CONTENTS, configuredFlowSnapshot);
        } else {
            logger.debug("Configured Flow Snapshot appears to be a File. Will use {} property to configured Stateless NiFi", StatelessNiFiCommonConfig.BOOTSTRAP_SNAPSHOT_FILE);
            final File flowSnapshotFile = new File(configuredFlowSnapshot);
            dataflowDefinitionProperties.put(StatelessNiFiCommonConfig.BOOTSTRAP_SNAPSHOT_FILE, flowSnapshotFile.getAbsolutePath());
        }
    }

    /**
     * Collect Parameter Context values that override standard properties
     *
     * @return The parameter overrides of the flow.
     */
    public List<ParameterOverride> getParameterOverrides() {
        final List<ParameterOverride> parameterOverrides = new ArrayList<>();

        for (final Map.Entry<String, String> entry : originalsStrings().entrySet()) {
            final String parameterValue = entry.getValue();

            ParameterOverride parameterOverride = null;
            final Matcher matcher = StatelessNiFiCommonConfig.PARAMETER_WITH_CONTEXT_PATTERN.matcher(entry.getKey());
            if (matcher.matches()) {
                final String contextName = matcher.group(1);
                final String parameterName = matcher.group(2);
                parameterOverride = new ParameterOverride(contextName, parameterName, parameterValue);
            } else {
                final Matcher noContextMatcher = StatelessNiFiCommonConfig.PARAMETER_WITHOUT_CONTEXT_PATTERN.matcher(entry.getKey());
                if (noContextMatcher.matches()) {
                    final String parameterName = noContextMatcher.group(1);
                    parameterOverride = new ParameterOverride(parameterName, parameterValue);
                }
            }

            if (parameterOverride != null) {
                parameterOverrides.add(parameterOverride);
            }
        }

        return parameterOverrides;
    }

    protected String getOptionalPassword(String key) {
        Password password = getPassword(key);
        return password == null ? null : password.value();
    }

    /**
     * Adds the flow definition related common configs to a config definition.
     *
     * @param configDef The config def to extend.
     */
    protected static void addFlowConfigElements(final ConfigDef configDef) {
        configDef.define(FLOW_SNAPSHOT, ConfigDef.Type.STRING, null, new FlowSnapshotValidator(), ConfigDef.Importance.HIGH,
                "Specifies the dataflow to run. This may be a file containing the dataflow, a URL that points to a dataflow, or a String containing the entire dataflow as an escaped JSON.",
                FLOW_GROUP, 0, ConfigDef.Width.NONE, "Flow snapshot");
    }

    /**
     * Adds the directory, NAR, kerberos and TLS common configs to a config definition.
     *
     * @param configDef The config def to extend.
     */
    protected static void addCommonConfigElements(final ConfigDef configDef) {
        configDef.define(NAR_DIRECTORY, ConfigDef.Type.STRING, null, new ConnectDirectoryExistsValidator(), ConfigDef.Importance.HIGH,
                "Specifies the directory that stores the NiFi Archives (NARs)", DIRECTORIES_GROUP, 0, ConfigDef.Width.NONE, "NAR directory");
        configDef.define(EXTENSIONS_DIRECTORY, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                "Specifies the directory that stores the extensions that will be downloaded (if any) from the configured Extension Client",
                DIRECTORIES_GROUP, 1, ConfigDef.Width.NONE, "Extensions directory");
        configDef.define(WORKING_DIRECTORY, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
                "Specifies the temporary working directory for expanding NiFi Archives (NARs)",
                DIRECTORIES_GROUP, 2, ConfigDef.Width.NONE, "Working directory");
        configDef.define(DATAFLOW_NAME, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, nonEmptyStringWithoutControlChars(), ConfigDef.Importance.HIGH, "The name of the dataflow.");

        configDef.define(
                KRB5_FILE, ConfigDef.Type.STRING, DEFAULT_KRB5_FILE, ConfigDef.Importance.MEDIUM,
                "Specifies the krb5.conf file to use if connecting to Kerberos-enabled services",
                KERBEROS_GROUP, 0, ConfigDef.Width.NONE, "krb5.conf file");
        configDef.define(
                NEXUS_BASE_URL, ConfigDef.Type.STRING, null, new ConnectHttpUrlValidator(), ConfigDef.Importance.MEDIUM,
                "Specifies the Base URL of the Nexus instance to source extensions from",
                NEXUS_GROUP, 0, ConfigDef.Width.NONE, "Nexus base URL");

        configDef.define(
                DATAFLOW_TIMEOUT, ConfigDef.Type.STRING, DEFAULT_DATAFLOW_TIMEOUT, ConfigDef.Importance.MEDIUM,
                "Specifies the amount of time to wait for the dataflow to finish processing input before considering the dataflow a failure",
                FLOW_GROUP, 1, ConfigDef.Width.NONE, "Dataflow processing timeout");

        configDef.define(TRUSTSTORE_FILE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                "Filename of the truststore that Stateless NiFi should use for connecting to NiFi Registry and for Site-to-Site communications." +
                        " If not specified, communications will occur only over http, not https.", TLS_GROUP, 0, ConfigDef.Width.NONE, "Truststore file");
        configDef.define(TRUSTSTORE_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                "The type of the Truststore file. Either JKS or PKCS12.", TLS_GROUP, 1, ConfigDef.Width.NONE, "Truststore type");
        configDef.define(TRUSTSTORE_PASSWORD, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM,
                "The password for the truststore.", TLS_GROUP, 2, ConfigDef.Width.NONE, "Truststore password");
        configDef.define(KEYSTORE_FILE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                "Filename of the keystore that Stateless NiFi should use for connecting to NiFi Registry and for Site-to-Site communications.",
                TLS_GROUP, 3, ConfigDef.Width.NONE, "Keystore file");
        configDef.define(KEYSTORE_TYPE, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                "The type of the Keystore file. Either JKS or PKCS12.", TLS_GROUP, 4, ConfigDef.Width.NONE, "Keystore type");
        configDef.define(KEYSTORE_PASSWORD, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM,
                "The password for the keystore.", TLS_GROUP, 5, ConfigDef.Width.NONE, "Keystore password");
        configDef.define(KEY_PASSWORD, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.MEDIUM,
                "The password for the key in the keystore. If not provided, the password is assumed to be the same as the keystore password.",
                TLS_GROUP, 6, ConfigDef.Width.NONE, "Keystore key password");

        configDef.define(SENSITIVE_PROPS_KEY, ConfigDef.Type.PASSWORD, DEFAULT_SENSITIVE_PROPS_KEY, ConfigDef.Importance.MEDIUM, "A key that components can use for encrypting and decrypting " +
                "sensitive values.", SECURITY_GROUP, 0, ConfigDef.Width.NONE, "Sensitive properties key");
    }
}
