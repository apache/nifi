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

package org.apache.nifi.minifi.bootstrap.service;

import static java.lang.String.join;
import static java.lang.System.getProperty;
import static java.util.Map.entry;
import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.nifi.minifi.bootstrap.service.BootstrapFileProvider.getBootstrapConfFile;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider.APP_LOG_FILE_EXTENSION;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider.APP_LOG_FILE_NAME;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider.BOOTSTRAP_LOG_FILE_EXTENSION;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider.BOOTSTRAP_LOG_FILE_NAME;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider.DEFAULT_APP_LOG_FILE_NAME;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider.DEFAULT_BOOTSTRAP_LOG_FILE_NAME;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider.DEFAULT_LOG_DIR;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider.DEFAULT_LOG_FILE_EXTENSION;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider.LOG_DIR;
import static org.apache.nifi.minifi.bootstrap.service.MiNiFiExecCommandProvider.getMiNiFiPropertiesPath;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.MINIFI_APP_LOG_FILE;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.MINIFI_BOOTSTRAP_FILE_PATH;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.MINIFI_BOOTSTRAP_LOG_FILE;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.MINIFI_LOG_DIRECTORY;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeException;
import org.apache.nifi.minifi.bootstrap.util.OrderedProperties;
import org.apache.nifi.minifi.commons.api.MiNiFiProperties;
import org.apache.nifi.minifi.properties.BootstrapProperties;
import org.apache.nifi.util.NiFiProperties;

public class MiNiFiPropertiesGenerator {

    public static final String PROPERTIES_FILE_APACHE_2_0_LICENSE =
        " Licensed to the Apache Software Foundation (ASF) under one or more\n" +
            "# contributor license agreements.  See the NOTICE file distributed with\n" +
            "# this work for additional information regarding copyright ownership.\n" +
            "# The ASF licenses this file to You under the Apache License, Version 2.0\n" +
            "# (the \"License\"); you may not use this file except in compliance with\n" +
            "# the License.  You may obtain a copy of the License at\n" +
            "#\n" +
            "#     http://www.apache.org/licenses/LICENSE-2.0\n" +
            "#\n" +
            "# Unless required by applicable law or agreed to in writing, software\n" +
            "# distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
            "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
            "# See the License for the specific language governing permissions and\n" +
            "# limitations under the License.\n" +
            "\n";

    static final List<Triple<String, String, String>> NIFI_PROPERTIES_WITH_DEFAULT_VALUES_AND_COMMENTS = List.of(
        Triple.of(NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_ENABLED, "false", EMPTY),
        Triple.of(NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_DIR, "./conf/archive/", EMPTY),
        Triple.of(NiFiProperties.AUTO_RESUME_STATE, "true", EMPTY),
        Triple.of(NiFiProperties.FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD, "10 sec", EMPTY),
        Triple.of(NiFiProperties.WRITE_DELAY_INTERVAL, "500 ms", EMPTY),
        Triple.of(NiFiProperties.ADMINISTRATIVE_YIELD_DURATION, "30 sec", EMPTY),
        Triple.of(NiFiProperties.BORED_YIELD_DURATION, "10 millis",
            "# If a component has no work to do (is \"bored\"), how long should we wait before checking again for work"),
        Triple.of(NiFiProperties.LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE, "./conf/login-identity-providers.xml", EMPTY),
        Triple.of(NiFiProperties.UI_BANNER_TEXT, EMPTY, EMPTY),
        Triple.of(NiFiProperties.NAR_LIBRARY_DIRECTORY, "./lib", EMPTY),
        Triple.of(NiFiProperties.NAR_WORKING_DIRECTORY, "./work/nar/", EMPTY),
        Triple.of(NiFiProperties.NAR_LIBRARY_AUTOLOAD_DIRECTORY, "./extensions", EMPTY),
        Triple.of(NiFiProperties.STATE_MANAGEMENT_CONFIG_FILE, "./conf/state-management.xml", "# State Management"),
        Triple.of(NiFiProperties.STATE_MANAGEMENT_LOCAL_PROVIDER_ID, "local-provider", "# The ID of the local state provider"),
        Triple.of(NiFiProperties.REPOSITORY_DATABASE_DIRECTORY, "./database_repository", "# H2 Settings"),
        Triple.of(NiFiProperties.FLOWFILE_REPOSITORY_IMPLEMENTATION, "org.apache.nifi.controller.repository.WriteAheadFlowFileRepository", "# FlowFile Repository"),
        Triple.of(NiFiProperties.FLOWFILE_REPOSITORY_DIRECTORY, "./flowfile_repository", EMPTY),
        Triple.of(NiFiProperties.FLOWFILE_REPOSITORY_CHECKPOINT_INTERVAL, "20 secs", EMPTY),
        Triple.of(NiFiProperties.FLOWFILE_REPOSITORY_ALWAYS_SYNC, "false", EMPTY),
        Triple.of(NiFiProperties.FLOWFILE_SWAP_MANAGER_IMPLEMENTATION, "org.apache.nifi.controller.FileSystemSwapManager", EMPTY),
        Triple.of(NiFiProperties.QUEUE_SWAP_THRESHOLD, "20000", EMPTY),
        Triple.of(NiFiProperties.CONTENT_REPOSITORY_IMPLEMENTATION, "org.apache.nifi.controller.repository.FileSystemRepository", "# Content Repository"),
        Triple.of(NiFiProperties.MAX_APPENDABLE_CLAIM_SIZE, "50 KB", EMPTY),
        Triple.of(NiFiProperties.CONTENT_ARCHIVE_MAX_RETENTION_PERIOD, "7 days", EMPTY),
        Triple.of(NiFiProperties.CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE, "50%", EMPTY),
        Triple.of(NiFiProperties.CONTENT_ARCHIVE_ENABLED, "false", EMPTY),
        Triple.of(NiFiProperties.REPOSITORY_CONTENT_PREFIX + ".default", "./content_repository", EMPTY),
        Triple.of(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, "org.apache.nifi.provenance.NoOpProvenanceRepository",
            "# Provenance Repository Properties"),
        Triple.of(NiFiProperties.PROVENANCE_ROLLOVER_TIME, EMPTY, EMPTY),
        Triple.of(NiFiProperties.PROVENANCE_INDEX_SHARD_SIZE, EMPTY, EMPTY),
        Triple.of(NiFiProperties.PROVENANCE_MAX_STORAGE_SIZE, EMPTY, EMPTY),
        Triple.of(NiFiProperties.PROVENANCE_MAX_STORAGE_TIME, EMPTY, EMPTY),
        Triple.of(NiFiProperties.COMPONENT_STATUS_REPOSITORY_IMPLEMENTATION, "org.apache.nifi.controller.status.history.VolatileComponentStatusRepository",
            "# Component Status Repository"),
        Triple.of(NiFiProperties.COMPONENT_STATUS_SNAPSHOT_FREQUENCY, "1 min", EMPTY),
        Triple.of(NiFiProperties.WEB_HTTP_HOST, EMPTY, EMPTY),
        Triple.of(NiFiProperties.WEB_HTTP_PORT, EMPTY, EMPTY),
        Triple.of(NiFiProperties.WEB_HTTPS_HOST, EMPTY, EMPTY),
        Triple.of(NiFiProperties.WEB_HTTPS_PORT, EMPTY, EMPTY),
        Triple.of(NiFiProperties.WEB_WORKING_DIR, "./work/jetty", EMPTY),
        Triple.of(NiFiProperties.WEB_THREADS, "200", EMPTY),
        Triple.of(NiFiProperties.SECURITY_KEYSTORE, EMPTY, EMPTY),
        Triple.of(NiFiProperties.SECURITY_KEYSTORE_TYPE, EMPTY, EMPTY),
        Triple.of(NiFiProperties.SECURITY_KEYSTORE_PASSWD, EMPTY, EMPTY),
        Triple.of(NiFiProperties.SECURITY_KEY_PASSWD, EMPTY, EMPTY),
        Triple.of(NiFiProperties.SECURITY_TRUSTSTORE, EMPTY, EMPTY),
        Triple.of(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, EMPTY, EMPTY),
        Triple.of(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, EMPTY, EMPTY),
        Triple.of(NiFiProperties.SECURITY_OCSP_RESPONDER_URL, EMPTY, EMPTY),
        Triple.of(NiFiProperties.SECURITY_OCSP_RESPONDER_CERTIFICATE, EMPTY, EMPTY),
        Triple.of(NiFiProperties.CLUSTER_IS_NODE, "false", EMPTY),
        Triple.of(NiFiProperties.FLOW_CONFIGURATION_FILE, "./conf/flow.json.gz", EMPTY)
    );

    static final Map<String, String> MINIFI_TO_NIFI_PROPERTY_MAPPING = Map.ofEntries(
        entry(MiNiFiProperties.NIFI_MINIFI_FLOW_CONFIG.getKey(), NiFiProperties.FLOW_CONFIGURATION_FILE),
        entry(MiNiFiProperties.NIFI_MINIFI_SECURITY_KEYSTORE.getKey(), NiFiProperties.SECURITY_KEYSTORE),
        entry(MiNiFiProperties.NIFI_MINIFI_SECURITY_KEYSTORE_TYPE.getKey(), NiFiProperties.SECURITY_KEYSTORE_TYPE),
        entry(MiNiFiProperties.NIFI_MINIFI_SECURITY_KEYSTORE_PASSWD.getKey(), NiFiProperties.SECURITY_KEYSTORE_PASSWD),
        entry(MiNiFiProperties.NIFI_MINIFI_SECURITY_KEY_PASSWD.getKey(), NiFiProperties.SECURITY_KEY_PASSWD),
        entry(MiNiFiProperties.NIFI_MINIFI_SECURITY_TRUSTSTORE.getKey(), NiFiProperties.SECURITY_TRUSTSTORE),
        entry(MiNiFiProperties.NIFI_MINIFI_SECURITY_TRUSTSTORE_TYPE.getKey(), NiFiProperties.SECURITY_TRUSTSTORE_TYPE),
        entry(MiNiFiProperties.NIFI_MINIFI_SECURITY_TRUSTSTORE_PASSWD.getKey(), NiFiProperties.SECURITY_TRUSTSTORE_PASSWD)
    );

    static final String DEFAULT_SENSITIVE_PROPERTIES_ENCODING_ALGORITHM = "NIFI_PBKDF2_AES_GCM_256";

    private static final Base64.Encoder KEY_ENCODER = Base64.getEncoder().withoutPadding();
    private static final int SENSITIVE_PROPERTIES_KEY_LENGTH = 24;

    private static final String C2_PROPERTY_PREFIX = "c2.";
    private static final String NIFI_PREFIX = "nifi.";

    public static final String FILE_EXTENSION_DELIMITER = ".";

    public void generateMinifiProperties(String configDirectory, BootstrapProperties bootstrapProperties) throws ConfigurationChangeException {
        String minifiPropertiesFileName = Path.of(getMiNiFiPropertiesPath(bootstrapProperties, new File(configDirectory))).getFileName().toString();
        Path minifiPropertiesFile = Path.of(configDirectory, minifiPropertiesFileName);

        Map<String, String> existingSensitivePropertiesConfiguration = extractSensitivePropertiesConfiguration(minifiPropertiesFile);
        OrderedProperties minifiProperties = prepareMinifiProperties(bootstrapProperties, existingSensitivePropertiesConfiguration);

        persistMinifiProperties(minifiPropertiesFile, minifiProperties);
    }

    private Map<String, String> extractSensitivePropertiesConfiguration(Path minifiPropertiesFile) throws ConfigurationChangeException {
        if (!Files.exists(minifiPropertiesFile)) {
            return Map.of();
        }

        Properties minifiProperties = new Properties();
        try (InputStream inputStream = Files.newInputStream(minifiPropertiesFile)) {
            minifiProperties.load(inputStream);
        } catch (IOException e) {
            throw new ConfigurationChangeException("Unable to load MiNiFi properties from " + minifiPropertiesFile, e);
        }

        return Map.of(
            NiFiProperties.SENSITIVE_PROPS_KEY, minifiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY),
            NiFiProperties.SENSITIVE_PROPS_ALGORITHM, minifiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM)
        );
    }

    private OrderedProperties prepareMinifiProperties(BootstrapProperties bootstrapProperties, Map<String, String> existingSensitivePropertiesConfiguration) {
        OrderedProperties minifiProperties = new OrderedProperties();

        NIFI_PROPERTIES_WITH_DEFAULT_VALUES_AND_COMMENTS
            .forEach(triple -> minifiProperties.setProperty(triple.getLeft(), triple.getMiddle(), triple.getRight()));

        getNonBlankPropertiesWithPredicate(bootstrapProperties, MINIFI_TO_NIFI_PROPERTY_MAPPING::containsKey)
            .forEach(entry -> minifiProperties.setProperty(MINIFI_TO_NIFI_PROPERTY_MAPPING.get(entry.getKey()), entry.getValue()));

        getSensitiveProperties(bootstrapProperties, existingSensitivePropertiesConfiguration)
            .forEach(entry -> minifiProperties.setProperty(entry.getKey(), entry.getValue()));

        getNonBlankPropertiesWithPredicate(bootstrapProperties, key -> key.startsWith(C2_PROPERTY_PREFIX))
            .forEach(entry -> minifiProperties.setProperty(entry.getKey(), entry.getValue()));

        getNonBlankPropertiesWithPredicate(bootstrapProperties, key -> key.startsWith(NIFI_PREFIX))
            .forEach(entry -> minifiProperties.setProperty(entry.getKey(), entry.getValue()));

        bootstrapFileAndLogProperties()
            .forEach(entry -> minifiProperties.setProperty(entry.getKey(), entry.getValue()));

        return minifiProperties;
    }

    private List<Pair<String, String>> getNonBlankPropertiesWithPredicate(BootstrapProperties bootstrapProperties, Predicate<String> predicate) {
        return ofNullable(bootstrapProperties)
            .map(BootstrapProperties::getPropertyKeys)
            .orElseGet(Set::of)
            .stream()
            .filter(predicate)
            .map(key -> Pair.of(key, bootstrapProperties.getProperty(key)))
            .filter(pair -> isNotBlank(pair.getValue()))
            .sorted((o1, o2) -> Comparator.<String>naturalOrder().compare(o1.getKey(), o2.getKey()))
            .toList();
    }

    private List<Pair<String, String>> getSensitiveProperties(BootstrapProperties bootstrapProperties, Map<String, String> existingSensitivePropertiesConfiguration) {
        return existingSensitivePropertiesConfiguration.isEmpty()
            ? List.of(
                Pair.of(NiFiProperties.SENSITIVE_PROPS_KEY,
                    ofNullable(bootstrapProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_SENSITIVE_PROPS_KEY.getKey()))
                        .filter(StringUtils::isNotBlank)
                        .orElseGet(this::generateSensitivePropertiesKey)),
                Pair.of(NiFiProperties.SENSITIVE_PROPS_ALGORITHM,
                    ofNullable(bootstrapProperties.getProperty(MiNiFiProperties.NIFI_MINIFI_SENSITIVE_PROPS_ALGORITHM.getKey()))
                        .filter(StringUtils::isNotBlank)
                        .orElse(DEFAULT_SENSITIVE_PROPERTIES_ENCODING_ALGORITHM)))
            : existingSensitivePropertiesConfiguration.entrySet()
                .stream()
                .map(entry -> Pair.of(entry.getKey(), entry.getValue()))
                .toList();
    }

    private String generateSensitivePropertiesKey() {
        SecureRandom secureRandom = new SecureRandom();
        byte[] sensitivePropertiesKeyBinary = new byte[SENSITIVE_PROPERTIES_KEY_LENGTH];
        secureRandom.nextBytes(sensitivePropertiesKeyBinary);
        return KEY_ENCODER.encodeToString(sensitivePropertiesKeyBinary);
    }

    private List<Pair<String, String>> bootstrapFileAndLogProperties() {
        return List.of(
            Pair.of(MINIFI_BOOTSTRAP_FILE_PATH, getBootstrapConfFile().getAbsolutePath()),
            Pair.of(MINIFI_LOG_DIRECTORY, getProperty(LOG_DIR, DEFAULT_LOG_DIR).trim()),
            Pair.of(MINIFI_APP_LOG_FILE, join(FILE_EXTENSION_DELIMITER,
                getProperty(APP_LOG_FILE_NAME, DEFAULT_APP_LOG_FILE_NAME).trim(),
                getProperty(APP_LOG_FILE_EXTENSION, DEFAULT_LOG_FILE_EXTENSION).trim())),
            Pair.of(MINIFI_BOOTSTRAP_LOG_FILE, join(FILE_EXTENSION_DELIMITER,
                getProperty(BOOTSTRAP_LOG_FILE_NAME, DEFAULT_BOOTSTRAP_LOG_FILE_NAME).trim(),
                getProperty(BOOTSTRAP_LOG_FILE_EXTENSION, DEFAULT_LOG_FILE_EXTENSION).trim()))
        );
    }

    private void persistMinifiProperties(Path minifiPropertiesFile, OrderedProperties minifiProperties) throws ConfigurationChangeException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             FileOutputStream fileOutputStream = new FileOutputStream(minifiPropertiesFile.toString())) {
            minifiProperties.store(byteArrayOutputStream, PROPERTIES_FILE_APACHE_2_0_LICENSE);
            byteArrayOutputStream.writeTo(fileOutputStream);
        } catch (Exception e) {
            throw new ConfigurationChangeException("Failed to write MiNiFi properties to " + minifiPropertiesFile, e);
        }
    }
}
