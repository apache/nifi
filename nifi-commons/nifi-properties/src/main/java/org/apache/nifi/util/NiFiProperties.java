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
package org.apache.nifi.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The NiFiProperties class holds all properties which are needed for various
 * values to be available at runtime. It is strongly tied to the startup
 * properties needed and is often refer to as the 'nifi.properties' file. The
 * properties contains keys and values. Great care should be taken in leveraging
 * this class or passing it along. Its use should be refactored and minimized
 * over time.
 */
public abstract class NiFiProperties {

    // core properties
    public static final String PROPERTIES_FILE_PATH = "nifi.properties.file.path";
    public static final String FLOW_CONFIGURATION_FILE = "nifi.flow.configuration.file";
    public static final String FLOW_CONFIGURATION_ARCHIVE_ENABLED = "nifi.flow.configuration.archive.enabled";
    public static final String FLOW_CONFIGURATION_ARCHIVE_DIR = "nifi.flow.configuration.archive.dir";
    public static final String FLOW_CONFIGURATION_ARCHIVE_MAX_TIME = "nifi.flow.configuration.archive.max.time";
    public static final String FLOW_CONFIGURATION_ARCHIVE_MAX_STORAGE = "nifi.flow.configuration.archive.max.storage";
    public static final String FLOW_CONFIGURATION_ARCHIVE_MAX_COUNT = "nifi.flow.configuration.archive.max.count";
    public static final String AUTHORIZER_CONFIGURATION_FILE = "nifi.authorizer.configuration.file";
    public static final String LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE = "nifi.login.identity.provider.configuration.file";
    public static final String REPOSITORY_DATABASE_DIRECTORY = "nifi.database.directory";
    public static final String RESTORE_DIRECTORY = "nifi.restore.directory";
    public static final String WRITE_DELAY_INTERVAL = "nifi.flowservice.writedelay.interval";
    public static final String AUTO_RESUME_STATE = "nifi.flowcontroller.autoResumeState";
    public static final String FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD = "nifi.flowcontroller.graceful.shutdown.period";
    public static final String NAR_LIBRARY_DIRECTORY = "nifi.nar.library.directory";
    public static final String NAR_LIBRARY_DIRECTORY_PREFIX = "nifi.nar.library.directory.";
    public static final String NAR_WORKING_DIRECTORY = "nifi.nar.working.directory";
    public static final String COMPONENT_DOCS_DIRECTORY = "nifi.documentation.working.directory";
    public static final String SENSITIVE_PROPS_KEY = "nifi.sensitive.props.key";
    public static final String SENSITIVE_PROPS_ALGORITHM = "nifi.sensitive.props.algorithm";
    public static final String SENSITIVE_PROPS_PROVIDER = "nifi.sensitive.props.provider";
    public static final String H2_URL_APPEND = "nifi.h2.url.append";
    public static final String REMOTE_INPUT_HOST = "nifi.remote.input.host";
    public static final String REMOTE_INPUT_PORT = "nifi.remote.input.socket.port";
    public static final String SITE_TO_SITE_SECURE = "nifi.remote.input.secure";
    public static final String SITE_TO_SITE_HTTP_ENABLED = "nifi.remote.input.http.enabled";
    public static final String SITE_TO_SITE_HTTP_TRANSACTION_TTL = "nifi.remote.input.http.transaction.ttl";
    public static final String TEMPLATE_DIRECTORY = "nifi.templates.directory";
    public static final String ADMINISTRATIVE_YIELD_DURATION = "nifi.administrative.yield.duration";
    public static final String PERSISTENT_STATE_DIRECTORY = "nifi.persistent.state.directory";
    public static final String BORED_YIELD_DURATION = "nifi.bored.yield.duration";
    public static final String PROCESSOR_SCHEDULING_TIMEOUT = "nifi.processor.scheduling.timeout";

    // content repository properties
    public static final String REPOSITORY_CONTENT_PREFIX = "nifi.content.repository.directory.";
    public static final String CONTENT_REPOSITORY_IMPLEMENTATION = "nifi.content.repository.implementation";
    public static final String MAX_APPENDABLE_CLAIM_SIZE = "nifi.content.claim.max.appendable.size";
    public static final String MAX_FLOWFILES_PER_CLAIM = "nifi.content.claim.max.flow.files";
    public static final String CONTENT_ARCHIVE_MAX_RETENTION_PERIOD = "nifi.content.repository.archive.max.retention.period";
    public static final String CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE = "nifi.content.repository.archive.max.usage.percentage";
    public static final String CONTENT_ARCHIVE_BACK_PRESSURE_PERCENTAGE = "nifi.content.repository.archive.backpressure.percentage";
    public static final String CONTENT_ARCHIVE_ENABLED = "nifi.content.repository.archive.enabled";
    public static final String CONTENT_ARCHIVE_CLEANUP_FREQUENCY = "nifi.content.repository.archive.cleanup.frequency";
    public static final String CONTENT_VIEWER_URL = "nifi.content.viewer.url";

    // flowfile repository properties
    public static final String FLOWFILE_REPOSITORY_IMPLEMENTATION = "nifi.flowfile.repository.implementation";
    public static final String FLOWFILE_REPOSITORY_ALWAYS_SYNC = "nifi.flowfile.repository.always.sync";
    public static final String FLOWFILE_REPOSITORY_DIRECTORY = "nifi.flowfile.repository.directory";
    public static final String FLOWFILE_REPOSITORY_PARTITIONS = "nifi.flowfile.repository.partitions";
    public static final String FLOWFILE_REPOSITORY_CHECKPOINT_INTERVAL = "nifi.flowfile.repository.checkpoint.interval";
    public static final String FLOWFILE_SWAP_MANAGER_IMPLEMENTATION = "nifi.swap.manager.implementation";
    public static final String QUEUE_SWAP_THRESHOLD = "nifi.queue.swap.threshold";
    public static final String SWAP_IN_THREADS = "nifi.swap.in.threads";
    public static final String SWAP_IN_PERIOD = "nifi.swap.in.period";
    public static final String SWAP_OUT_THREADS = "nifi.swap.out.threads";
    public static final String SWAP_OUT_PERIOD = "nifi.swap.out.period";

    // provenance properties
    public static final String PROVENANCE_REPO_IMPLEMENTATION_CLASS = "nifi.provenance.repository.implementation";
    public static final String PROVENANCE_REPO_DIRECTORY_PREFIX = "nifi.provenance.repository.directory.";
    public static final String PROVENANCE_MAX_STORAGE_TIME = "nifi.provenance.repository.max.storage.time";
    public static final String PROVENANCE_MAX_STORAGE_SIZE = "nifi.provenance.repository.max.storage.size";
    public static final String PROVENANCE_ROLLOVER_TIME = "nifi.provenance.repository.rollover.time";
    public static final String PROVENANCE_ROLLOVER_SIZE = "nifi.provenance.repository.rollover.size";
    public static final String PROVENANCE_QUERY_THREAD_POOL_SIZE = "nifi.provenance.repository.query.threads";
    public static final String PROVENANCE_INDEX_THREAD_POOL_SIZE = "nifi.provenance.repository.index.threads";
    public static final String PROVENANCE_COMPRESS_ON_ROLLOVER = "nifi.provenance.repository.compress.on.rollover";
    public static final String PROVENANCE_INDEXED_FIELDS = "nifi.provenance.repository.indexed.fields";
    public static final String PROVENANCE_INDEXED_ATTRIBUTES = "nifi.provenance.repository.indexed.attributes";
    public static final String PROVENANCE_INDEX_SHARD_SIZE = "nifi.provenance.repository.index.shard.size";
    public static final String PROVENANCE_JOURNAL_COUNT = "nifi.provenance.repository.journal.count";
    public static final String PROVENANCE_REPO_ENCRYPTION_KEY = "nifi.provenance.repository.encryption.key";
    public static final String PROVENANCE_REPO_ENCRYPTION_KEY_ID = "nifi.provenance.repository.encryption.key.id";
    public static final String PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS = "nifi.provenance.repository.encryption.key.provider.implementation";
    public static final String PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_LOCATION = "nifi.provenance.repository.encryption.key.provider.location";
    public static final String PROVENANCE_REPO_DEBUG_FREQUENCY = "nifi.provenance.repository.debug.frequency";

    // component status repository properties
    public static final String COMPONENT_STATUS_REPOSITORY_IMPLEMENTATION = "nifi.components.status.repository.implementation";
    public static final String COMPONENT_STATUS_SNAPSHOT_FREQUENCY = "nifi.components.status.snapshot.frequency";

    // encryptor properties
    public static final String NF_SENSITIVE_PROPS_KEY = "nifi.sensitive.props.key";
    public static final String NF_SENSITIVE_PROPS_ALGORITHM = "nifi.sensitive.props.algorithm";
    public static final String NF_SENSITIVE_PROPS_PROVIDER = "nifi.sensitive.props.provider";

    // security properties
    public static final String SECURITY_KEYSTORE = "nifi.security.keystore";
    public static final String SECURITY_KEYSTORE_TYPE = "nifi.security.keystoreType";
    public static final String SECURITY_KEYSTORE_PASSWD = "nifi.security.keystorePasswd";
    public static final String SECURITY_KEY_PASSWD = "nifi.security.keyPasswd";
    public static final String SECURITY_TRUSTSTORE = "nifi.security.truststore";
    public static final String SECURITY_TRUSTSTORE_TYPE = "nifi.security.truststoreType";
    public static final String SECURITY_TRUSTSTORE_PASSWD = "nifi.security.truststorePasswd";
    public static final String SECURITY_NEED_CLIENT_AUTH = "nifi.security.needClientAuth";
    public static final String SECURITY_USER_AUTHORIZER = "nifi.security.user.authorizer";
    public static final String SECURITY_USER_LOGIN_IDENTITY_PROVIDER = "nifi.security.user.login.identity.provider";
    public static final String SECURITY_OCSP_RESPONDER_URL = "nifi.security.ocsp.responder.url";
    public static final String SECURITY_OCSP_RESPONDER_CERTIFICATE = "nifi.security.ocsp.responder.certificate";
    public static final String SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX = "nifi.security.identity.mapping.pattern.";
    public static final String SECURITY_IDENTITY_MAPPING_VALUE_PREFIX = "nifi.security.identity.mapping.value.";

    // oidc
    public static final String SECURITY_USER_OIDC_DISCOVERY_URL = "nifi.security.user.oidc.discovery.url";
    public static final String SECURITY_USER_OIDC_CONNECT_TIMEOUT = "nifi.security.user.oidc.connect.timeout";
    public static final String SECURITY_USER_OIDC_READ_TIMEOUT = "nifi.security.user.oidc.read.timeout";
    public static final String SECURITY_USER_OIDC_CLIENT_ID = "nifi.security.user.oidc.client.id";
    public static final String SECURITY_USER_OIDC_CLIENT_SECRET = "nifi.security.user.oidc.client.secret";
    public static final String SECURITY_USER_OIDC_PREFERRED_JWSALGORITHM = "nifi.security.user.oidc.preferred.jwsalgorithm";

    // apache knox
    public static final String SECURITY_USER_KNOX_URL = "nifi.security.user.knox.url";
    public static final String SECURITY_USER_KNOX_PUBLIC_KEY = "nifi.security.user.knox.publicKey";
    public static final String SECURITY_USER_KNOX_COOKIE_NAME = "nifi.security.user.knox.cookieName";
    public static final String SECURITY_USER_KNOX_AUDIENCES = "nifi.security.user.knox.audiences";

    // web properties
    public static final String WEB_WAR_DIR = "nifi.web.war.directory";
    public static final String WEB_HTTP_PORT = "nifi.web.http.port";
    public static final String WEB_HTTP_PORT_FORWARDING = "nifi.web.http.port.forwarding";
    public static final String WEB_HTTP_HOST = "nifi.web.http.host";
    public static final String WEB_HTTP_NETWORK_INTERFACE_PREFIX = "nifi.web.http.network.interface.";
    public static final String WEB_HTTPS_PORT = "nifi.web.https.port";
    public static final String WEB_HTTPS_PORT_FORWARDING = "nifi.web.https.port.forwarding";
    public static final String WEB_HTTPS_HOST = "nifi.web.https.host";
    public static final String WEB_HTTPS_NETWORK_INTERFACE_PREFIX = "nifi.web.https.network.interface.";
    public static final String WEB_WORKING_DIR = "nifi.web.jetty.working.directory";
    public static final String WEB_THREADS = "nifi.web.jetty.threads";

    // ui properties
    public static final String UI_BANNER_TEXT = "nifi.ui.banner.text";
    public static final String UI_AUTO_REFRESH_INTERVAL = "nifi.ui.autorefresh.interval";

    // cluster common properties
    public static final String CLUSTER_PROTOCOL_HEARTBEAT_INTERVAL = "nifi.cluster.protocol.heartbeat.interval";
    public static final String CLUSTER_PROTOCOL_IS_SECURE = "nifi.cluster.protocol.is.secure";

    // cluster node properties
    public static final String CLUSTER_IS_NODE = "nifi.cluster.is.node";
    public static final String CLUSTER_NODE_ADDRESS = "nifi.cluster.node.address";
    public static final String CLUSTER_NODE_PROTOCOL_PORT = "nifi.cluster.node.protocol.port";
    public static final String CLUSTER_NODE_PROTOCOL_THREADS = "nifi.cluster.node.protocol.threads";
    public static final String CLUSTER_NODE_PROTOCOL_MAX_THREADS = "nifi.cluster.node.protocol.max.threads";
    public static final String CLUSTER_NODE_CONNECTION_TIMEOUT = "nifi.cluster.node.connection.timeout";
    public static final String CLUSTER_NODE_READ_TIMEOUT = "nifi.cluster.node.read.timeout";
    public static final String CLUSTER_NODE_MAX_CONCURRENT_REQUESTS = "nifi.cluster.node.max.concurrent.requests";
    public static final String CLUSTER_FIREWALL_FILE = "nifi.cluster.firewall.file";
    public static final String FLOW_ELECTION_MAX_WAIT_TIME = "nifi.cluster.flow.election.max.wait.time";
    public static final String FLOW_ELECTION_MAX_CANDIDATES = "nifi.cluster.flow.election.max.candidates";

    // zookeeper properties
    public static final String ZOOKEEPER_CONNECT_STRING = "nifi.zookeeper.connect.string";
    public static final String ZOOKEEPER_CONNECT_TIMEOUT = "nifi.zookeeper.connect.timeout";
    public static final String ZOOKEEPER_SESSION_TIMEOUT = "nifi.zookeeper.session.timeout";
    public static final String ZOOKEEPER_ROOT_NODE = "nifi.zookeeper.root.node";
    public static final String ZOOKEEPER_AUTH_TYPE = "nifi.zookeeper.auth.type";
    public static final String ZOOKEEPER_KERBEROS_REMOVE_HOST_FROM_PRINCIPAL = "nifi.zookeeper.kerberos.removeHostFromPrincipal";
    public static final String ZOOKEEPER_KERBEROS_REMOVE_REALM_FROM_PRINCIPAL = "nifi.zookeeper.kerberos.removeRealmFromPrincipal";

    // kerberos properties
    public static final String KERBEROS_KRB5_FILE = "nifi.kerberos.krb5.file";
    public static final String KERBEROS_SERVICE_PRINCIPAL = "nifi.kerberos.service.principal";
    public static final String KERBEROS_SERVICE_KEYTAB_LOCATION = "nifi.kerberos.service.keytab.location";
    public static final String KERBEROS_SPNEGO_PRINCIPAL = "nifi.kerberos.spnego.principal";
    public static final String KERBEROS_SPNEGO_KEYTAB_LOCATION = "nifi.kerberos.spnego.keytab.location";
    public static final String KERBEROS_AUTHENTICATION_EXPIRATION = "nifi.kerberos.spnego.authentication.expiration";

    // state management
    public static final String STATE_MANAGEMENT_CONFIG_FILE = "nifi.state.management.configuration.file";
    public static final String STATE_MANAGEMENT_LOCAL_PROVIDER_ID = "nifi.state.management.provider.local";
    public static final String STATE_MANAGEMENT_CLUSTER_PROVIDER_ID = "nifi.state.management.provider.cluster";
    public static final String STATE_MANAGEMENT_START_EMBEDDED_ZOOKEEPER = "nifi.state.management.embedded.zookeeper.start";
    public static final String STATE_MANAGEMENT_ZOOKEEPER_PROPERTIES = "nifi.state.management.embedded.zookeeper.properties";

    // expression language properties
    public static final String VARIABLE_REGISTRY_PROPERTIES = "nifi.variable.registry.properties";

    // defaults
    public static final Boolean DEFAULT_AUTO_RESUME_STATE = true;
    public static final String DEFAULT_AUTHORIZER_CONFIGURATION_FILE = "conf/authorizers.xml";
    public static final String DEFAULT_LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE = "conf/login-identity-providers.xml";
    public static final Integer DEFAULT_REMOTE_INPUT_PORT = null;
    public static final Path DEFAULT_TEMPLATE_DIRECTORY = Paths.get("conf", "templates");
    public static final int DEFAULT_WEB_THREADS = 200;
    public static final String DEFAULT_WEB_WORKING_DIR = "./work/jetty";
    public static final String DEFAULT_NAR_WORKING_DIR = "./work/nar";
    public static final String DEFAULT_COMPONENT_DOCS_DIRECTORY = "./work/docs/components";
    public static final String DEFAULT_NAR_LIBRARY_DIR = "./lib";
    public static final String DEFAULT_FLOWFILE_REPO_PARTITIONS = "256";
    public static final String DEFAULT_FLOWFILE_CHECKPOINT_INTERVAL = "2 min";
    public static final int DEFAULT_MAX_FLOWFILES_PER_CLAIM = 100;
    public static final String DEFAULT_MAX_APPENDABLE_CLAIM_SIZE = "1 MB";
    public static final int DEFAULT_QUEUE_SWAP_THRESHOLD = 20000;
    public static final String DEFAULT_SWAP_STORAGE_LOCATION = "./flowfile_repository/swap";
    public static final String DEFAULT_SWAP_IN_PERIOD = "1 sec";
    public static final String DEFAULT_SWAP_OUT_PERIOD = "5 sec";
    public static final int DEFAULT_SWAP_IN_THREADS = 4;
    public static final int DEFAULT_SWAP_OUT_THREADS = 4;
    public static final String DEFAULT_ADMINISTRATIVE_YIELD_DURATION = "30 sec";
    public static final String DEFAULT_PERSISTENT_STATE_DIRECTORY = "./conf/state";
    public static final String DEFAULT_COMPONENT_STATUS_SNAPSHOT_FREQUENCY = "5 mins";
    public static final String DEFAULT_BORED_YIELD_DURATION = "10 millis";
    public static final String DEFAULT_ZOOKEEPER_CONNECT_TIMEOUT = "3 secs";
    public static final String DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = "3 secs";
    public static final String DEFAULT_ZOOKEEPER_ROOT_NODE = "/nifi";
    public static final String DEFAULT_ZOOKEEPER_AUTH_TYPE = "default";
    public static final String DEFAULT_ZOOKEEPER_KERBEROS_REMOVE_HOST_FROM_PRINCIPAL  = "true";
    public static final String DEFAULT_ZOOKEEPER_KERBEROS_REMOVE_REALM_FROM_PRINCIPAL  = "true";
    public static final String DEFAULT_SITE_TO_SITE_HTTP_TRANSACTION_TTL = "30 secs";
    public static final String DEFAULT_FLOW_CONFIGURATION_ARCHIVE_ENABLED = "true";
    public static final String DEFAULT_FLOW_CONFIGURATION_ARCHIVE_MAX_TIME = "30 days";
    public static final String DEFAULT_FLOW_CONFIGURATION_ARCHIVE_MAX_STORAGE = "500 MB";
    public static final String DEFAULT_SECURITY_USER_OIDC_CONNECT_TIMEOUT = "5 secs";
    public static final String DEFAULT_SECURITY_USER_OIDC_READ_TIMEOUT = "5 secs";

    // cluster common defaults
    public static final String DEFAULT_CLUSTER_PROTOCOL_HEARTBEAT_INTERVAL = "5 sec";
    public static final String DEFAULT_CLUSTER_PROTOCOL_MULTICAST_SERVICE_BROADCAST_DELAY = "500 ms";
    public static final int DEFAULT_CLUSTER_PROTOCOL_MULTICAST_SERVICE_LOCATOR_ATTEMPTS = 3;
    public static final String DEFAULT_CLUSTER_PROTOCOL_MULTICAST_SERVICE_LOCATOR_ATTEMPTS_DELAY = "1 sec";
    public static final String DEFAULT_CLUSTER_NODE_READ_TIMEOUT = "5 sec";
    public static final String DEFAULT_CLUSTER_NODE_CONNECTION_TIMEOUT = "5 sec";
    public static final int DEFAULT_CLUSTER_NODE_MAX_CONCURRENT_REQUESTS = 100;

    // cluster node defaults
    public static final int DEFAULT_CLUSTER_NODE_PROTOCOL_THREADS = 10;
    public static final int DEFAULT_CLUSTER_NODE_PROTOCOL_MAX_THREADS = 50;
    public static final String DEFAULT_REQUEST_REPLICATION_CLAIM_TIMEOUT = "15 secs";
    public static final String DEFAULT_FLOW_ELECTION_MAX_WAIT_TIME = "5 mins";

    // state management defaults
    public static final String DEFAULT_STATE_MANAGEMENT_CONFIG_FILE = "conf/state-management.xml";

    // Kerberos defaults
    public static final String DEFAULT_KERBEROS_AUTHENTICATION_EXPIRATION = "12 hours";

    /**
     * Retrieves the property value for the given property key.
     *
     * @param key the key of property value to lookup
     * @return value of property at given key or null if not found
     */
    public abstract String getProperty(String key);

    /**
     * Retrieves all known property keys.
     *
     * @return all known property keys
     */
    public abstract Set<String> getPropertyKeys();

    // getters for core properties //
    public File getFlowConfigurationFile() {
        try {
            return new File(getProperty(FLOW_CONFIGURATION_FILE));
        } catch (Exception ex) {
            return null;
        }
    }

    public File getFlowConfigurationFileDir() {
        try {
            return getFlowConfigurationFile().getParentFile();
        } catch (Exception ex) {
            return null;
        }
    }

    private Integer getPropertyAsPort(final String propertyName, final Integer defaultValue) {
        final String port = getProperty(propertyName);
        if (StringUtils.isEmpty(port)) {
            return defaultValue;
        }
        try {
            final int val = Integer.parseInt(port);
            if (val <= 0 || val > 65535) {
                throw new RuntimeException("Valid port range is 0 - 65535 but got " + val);
            }
            return val;
        } catch (final NumberFormatException e) {
            return defaultValue;
        }
    }

    public int getQueueSwapThreshold() {
        final String thresholdValue = getProperty(QUEUE_SWAP_THRESHOLD);
        if (thresholdValue == null) {
            return DEFAULT_QUEUE_SWAP_THRESHOLD;
        }

        try {
            return Integer.parseInt(thresholdValue);
        } catch (final NumberFormatException e) {
            return DEFAULT_QUEUE_SWAP_THRESHOLD;
        }
    }

    public Integer getIntegerProperty(final String propertyName, final Integer defaultValue) {
        final String value = getProperty(propertyName);
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value.trim());
        } catch (final Exception e) {
            return defaultValue;
        }
    }

    public int getSwapInThreads() {
        return getIntegerProperty(SWAP_IN_THREADS, DEFAULT_SWAP_IN_THREADS);
    }

    public int getSwapOutThreads() {
        final String value = getProperty(SWAP_OUT_THREADS);
        if (value == null) {
            return DEFAULT_SWAP_OUT_THREADS;
        }

        try {
            return Integer.parseInt(getProperty(SWAP_OUT_THREADS));
        } catch (final Exception e) {
            return DEFAULT_SWAP_OUT_THREADS;
        }
    }

    public String getSwapInPeriod() {
        return getProperty(SWAP_IN_PERIOD, DEFAULT_SWAP_IN_PERIOD);
    }

    public String getSwapOutPeriod() {
        return getProperty(SWAP_OUT_PERIOD, DEFAULT_SWAP_OUT_PERIOD);
    }

    public String getAdministrativeYieldDuration() {
        return getProperty(ADMINISTRATIVE_YIELD_DURATION, DEFAULT_ADMINISTRATIVE_YIELD_DURATION);
    }

    /**
     * The host name that will be given out to clients to connect to the Remote
     * Input Port.
     *
     * @return the remote input host name or null if not configured
     */
    public String getRemoteInputHost() {
        final String value = getProperty(REMOTE_INPUT_HOST);
        return StringUtils.isBlank(value) ? null : value;
    }

    /**
     * The socket port to listen on for a Remote Input Port.
     *
     * @return the remote input port for RAW socket communication
     */
    public Integer getRemoteInputPort() {
        return getPropertyAsPort(REMOTE_INPUT_PORT, DEFAULT_REMOTE_INPUT_PORT);
    }

    /**
     * @return False if property value is 'false'; True otherwise.
     */
    public Boolean isSiteToSiteSecure() {
        final String secureVal = getProperty(SITE_TO_SITE_SECURE, "true");

        return !"false".equalsIgnoreCase(secureVal);

    }

    /**
     * @return True if property value is 'true'; False otherwise.
     */
    public Boolean isSiteToSiteHttpEnabled() {
        final String remoteInputHttpEnabled = getProperty(SITE_TO_SITE_HTTP_ENABLED, "false");

        return "true".equalsIgnoreCase(remoteInputHttpEnabled);

    }

    /**
     * The HTTP or HTTPS Web API port for a Remote Input Port.
     *
     * @return the remote input port for HTTP(S) communication, or null if
     * HTTP(S) Site-to-Site is not enabled
     */
    public Integer getRemoteInputHttpPort() {
        if (!isSiteToSiteHttpEnabled()) {
            return null;
        }

        final String propertyKey;
        if (isSiteToSiteSecure()) {
            if (StringUtils.isBlank(getProperty(NiFiProperties.WEB_HTTPS_PORT_FORWARDING))) {
                propertyKey = WEB_HTTPS_PORT;
            } else {
                propertyKey = WEB_HTTPS_PORT_FORWARDING;
            }
        } else {
            if (StringUtils.isBlank(getProperty(NiFiProperties.WEB_HTTP_PORT_FORWARDING))) {
                propertyKey = WEB_HTTP_PORT;
            } else {
                propertyKey = WEB_HTTP_PORT_FORWARDING;
            }
        }

        final Integer port = getIntegerProperty(propertyKey, null);
        if (port == null) {
            throw new RuntimeException("Remote input HTTP" + (isSiteToSiteSecure() ? "S" : "")
                    + " is enabled but " + propertyKey + " is not specified.");
        }
        return port;
    }

    /**
     * Returns the directory to which Templates are to be persisted
     *
     * @return the template directory
     */
    public Path getTemplateDirectory() {
        final String strVal = getProperty(TEMPLATE_DIRECTORY);
        return (strVal == null) ? DEFAULT_TEMPLATE_DIRECTORY : Paths.get(strVal);
    }

    /**
     * Get the flow service write delay.
     *
     * @return The write delay
     */
    public String getFlowServiceWriteDelay() {
        return getProperty(WRITE_DELAY_INTERVAL);
    }

    /**
     * Returns whether the processors should be started automatically when the
     * application loads.
     *
     * @return Whether to auto start the processors or not
     */
    public boolean getAutoResumeState() {
        final String rawAutoResumeState = getProperty(AUTO_RESUME_STATE,
                DEFAULT_AUTO_RESUME_STATE.toString());
        return Boolean.parseBoolean(rawAutoResumeState);
    }

    /**
     * Returns the number of partitions that should be used for the FlowFile
     * Repository
     *
     * @return the number of partitions
     */
    public int getFlowFileRepositoryPartitions() {
        final String rawProperty = getProperty(FLOWFILE_REPOSITORY_PARTITIONS,
                DEFAULT_FLOWFILE_REPO_PARTITIONS);
        return Integer.parseInt(rawProperty);
    }

    /**
     * Returns the number of milliseconds between FlowFileRepository
     * checkpointing
     *
     * @return the number of milliseconds between checkpoint events
     */
    public String getFlowFileRepositoryCheckpointInterval() {
        return getProperty(FLOWFILE_REPOSITORY_CHECKPOINT_INTERVAL,
                DEFAULT_FLOWFILE_CHECKPOINT_INTERVAL);
    }

    /**
     * @return the restore directory or null if not configured
     */
    public File getRestoreDirectory() {
        final String value = getProperty(RESTORE_DIRECTORY);
        if (StringUtils.isBlank(value)) {
            return null;
        } else {
            return new File(value);
        }
    }

    /**
     * @return the user authorizers file
     */
    public File getAuthorizerConfigurationFile() {
        final String value = getProperty(AUTHORIZER_CONFIGURATION_FILE);
        if (StringUtils.isBlank(value)) {
            return new File(DEFAULT_AUTHORIZER_CONFIGURATION_FILE);
        } else {
            return new File(value);
        }
    }

    /**
     * @return the user login identity provider file
     */
    public File getLoginIdentityProviderConfigurationFile() {
        final String value = getProperty(LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE);
        if (StringUtils.isBlank(value)) {
            return new File(DEFAULT_LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE);
        } else {
            return new File(value);
        }
    }

    /**
     * Will default to true unless the value is explicitly set to false.
     *
     * @return Whether client auth is required
     */
    public boolean getNeedClientAuth() {
        boolean needClientAuth = true;
        String rawNeedClientAuth = getProperty(SECURITY_NEED_CLIENT_AUTH);
        if ("false".equalsIgnoreCase(rawNeedClientAuth)) {
            needClientAuth = false;
        }
        return needClientAuth;
    }

    // getters for web properties //
    public Integer getPort() {
        Integer port = null;
        try {
            port = Integer.parseInt(getProperty(WEB_HTTP_PORT));
        } catch (NumberFormatException nfe) {
        }
        return port;
    }

    public Integer getSslPort() {
        Integer sslPort = null;
        try {
            sslPort = Integer.parseInt(getProperty(WEB_HTTPS_PORT));
        } catch (NumberFormatException nfe) {
        }
        return sslPort;
    }

    public int getWebThreads() {
        return getIntegerProperty(WEB_THREADS, DEFAULT_WEB_THREADS);
    }

    public int getClusterNodeMaxConcurrentRequests() {
        return getIntegerProperty(CLUSTER_NODE_MAX_CONCURRENT_REQUESTS, DEFAULT_CLUSTER_NODE_MAX_CONCURRENT_REQUESTS);
    }

    public File getWebWorkingDirectory() {
        return new File(getProperty(WEB_WORKING_DIR, DEFAULT_WEB_WORKING_DIR));
    }

    public File getComponentDocumentationWorkingDirectory() {
        return new File(getProperty(COMPONENT_DOCS_DIRECTORY, DEFAULT_COMPONENT_DOCS_DIRECTORY));
    }

    public File getNarWorkingDirectory() {
        return new File(getProperty(NAR_WORKING_DIRECTORY, DEFAULT_NAR_WORKING_DIR));
    }

    public File getFrameworkWorkingDirectory() {
        return new File(getNarWorkingDirectory(), "framework");
    }

    public File getExtensionsWorkingDirectory() {
        return new File(getNarWorkingDirectory(), "extensions");
    }

    public List<Path> getNarLibraryDirectories() {

        List<Path> narLibraryPaths = new ArrayList<>();

        // go through each property
        for (String propertyName : getPropertyKeys()) {
            // determine if the property is a nar library path
            if (StringUtils.startsWith(propertyName, NAR_LIBRARY_DIRECTORY_PREFIX)
                    || NAR_LIBRARY_DIRECTORY.equals(propertyName)) {
                // attempt to resolve the path specified
                String narLib = getProperty(propertyName);
                if (!StringUtils.isBlank(narLib)) {
                    narLibraryPaths.add(Paths.get(narLib));
                }
            }
        }

        if (narLibraryPaths.isEmpty()) {
            narLibraryPaths.add(Paths.get(DEFAULT_NAR_LIBRARY_DIR));
        }

        return narLibraryPaths;
    }

    // getters for ui properties //

    /**
     * Get the banner text.
     *
     * @return The banner text
     */
    public String getBannerText() {
        return this.getProperty(UI_BANNER_TEXT, StringUtils.EMPTY);
    }

    /**
     * Returns the auto refresh interval in seconds.
     *
     * @return the interval over which the properties should auto refresh
     */
    public String getAutoRefreshInterval() {
        return getProperty(UI_AUTO_REFRESH_INTERVAL);
    }

    // getters for cluster protocol properties //
    public String getClusterProtocolHeartbeatInterval() {
        return getProperty(CLUSTER_PROTOCOL_HEARTBEAT_INTERVAL,
                DEFAULT_CLUSTER_PROTOCOL_HEARTBEAT_INTERVAL);
    }

    public String getNodeHeartbeatInterval() {
        return getClusterProtocolHeartbeatInterval();
    }

    public String getClusterNodeReadTimeout() {
        return getProperty(CLUSTER_NODE_READ_TIMEOUT, DEFAULT_CLUSTER_NODE_READ_TIMEOUT);
    }

    public String getClusterNodeConnectionTimeout() {
        return getProperty(CLUSTER_NODE_CONNECTION_TIMEOUT,
                DEFAULT_CLUSTER_NODE_CONNECTION_TIMEOUT);
    }

    public File getPersistentStateDirectory() {
        final String dirName = getProperty(PERSISTENT_STATE_DIRECTORY,
                DEFAULT_PERSISTENT_STATE_DIRECTORY);
        final File file = new File(dirName);
        if (!file.exists()) {
            file.mkdirs();
        }
        return file;
    }

    // getters for cluster node properties //
    public boolean isNode() {
        return Boolean.parseBoolean(getProperty(CLUSTER_IS_NODE));
    }

    public InetSocketAddress getClusterNodeProtocolAddress() {
        try {
            String socketAddress = getProperty(CLUSTER_NODE_ADDRESS);
            if (StringUtils.isBlank(socketAddress)) {
                socketAddress = "localhost";
            }
            int socketPort = getClusterNodeProtocolPort();
            return InetSocketAddress.createUnresolved(socketAddress, socketPort);
        } catch (Exception ex) {
            throw new RuntimeException("Invalid node protocol address/port due to: " + ex, ex);
        }
    }

    public Integer getClusterNodeProtocolPort() {
        try {
            return Integer.parseInt(getProperty(CLUSTER_NODE_PROTOCOL_PORT));
        } catch (NumberFormatException nfe) {
            return null;
        }
    }

    /**
     * @deprecated Use getClusterNodeProtocolCorePoolSize() and getClusterNodeProtocolMaxPoolSize() instead
     */
    @Deprecated()
    public int getClusterNodeProtocolThreads() {
        return getClusterNodeProtocolCorePoolSize();
    }

    public int getClusterNodeProtocolCorePoolSize() {
        try {
            return Integer.parseInt(getProperty(CLUSTER_NODE_PROTOCOL_THREADS));
        } catch (NumberFormatException nfe) {
            return DEFAULT_CLUSTER_NODE_PROTOCOL_THREADS;
        }
    }

    public int getClusterNodeProtocolMaxPoolSize() {
        try {
            return Integer.parseInt(getProperty(CLUSTER_NODE_PROTOCOL_MAX_THREADS));
        } catch (NumberFormatException nfe) {
            return DEFAULT_CLUSTER_NODE_PROTOCOL_MAX_THREADS;
        }
    }

    public boolean isClustered() {
        return Boolean.parseBoolean(getProperty(CLUSTER_IS_NODE));
    }

    public File getClusterNodeFirewallFile() {
        final String firewallFile = getProperty(CLUSTER_FIREWALL_FILE);
        if (StringUtils.isBlank(firewallFile)) {
            return null;
        } else {
            return new File(firewallFile);
        }
    }

    public String getClusterProtocolManagerToNodeApiScheme() {
        final String isSecureProperty = getProperty(CLUSTER_PROTOCOL_IS_SECURE);
        if (Boolean.valueOf(isSecureProperty)) {
            return "https";
        } else {
            return "http";
        }
    }

    public File getKerberosConfigurationFile() {
        final String krb5File = getProperty(KERBEROS_KRB5_FILE);
        if (krb5File != null && krb5File.trim().length() > 0) {
            return new File(krb5File.trim());
        } else {
            return null;
        }
    }

    public String getKerberosServicePrincipal() {
        final String servicePrincipal = getProperty(KERBEROS_SERVICE_PRINCIPAL);
        if (!StringUtils.isBlank(servicePrincipal)) {
            return servicePrincipal.trim();
        } else {
            return null;
        }
    }

    public String getKerberosServiceKeytabLocation() {
        final String keytabLocation = getProperty(KERBEROS_SERVICE_KEYTAB_LOCATION);
        if (!StringUtils.isBlank(keytabLocation)) {
            return keytabLocation.trim();
        } else {
            return null;
        }
    }

    public String getKerberosSpnegoPrincipal() {
        final String spengoPrincipal = getProperty(KERBEROS_SPNEGO_PRINCIPAL);
        if (!StringUtils.isBlank(spengoPrincipal)) {
            return spengoPrincipal.trim();
        } else {
            return null;
        }
    }

    public String getKerberosSpnegoKeytabLocation() {
        final String keytabLocation = getProperty(KERBEROS_SPNEGO_KEYTAB_LOCATION);
        if (!StringUtils.isBlank(keytabLocation)) {
            return keytabLocation.trim();
        } else {
            return null;
        }
    }

    public String getKerberosAuthenticationExpiration() {
        final String authenticationExpirationString = getProperty(KERBEROS_AUTHENTICATION_EXPIRATION, DEFAULT_KERBEROS_AUTHENTICATION_EXPIRATION);
        if (!StringUtils.isBlank(authenticationExpirationString)) {
            return authenticationExpirationString.trim();
        } else {
            return null;
        }
    }

    /**
     * Returns true if the Kerberos service principal and keytab location
     * properties are populated.
     *
     * @return true if Kerberos service support is enabled
     */
    public boolean isKerberosSpnegoSupportEnabled() {
        return !StringUtils.isBlank(getKerberosSpnegoPrincipal()) && !StringUtils.isBlank(getKerberosSpnegoKeytabLocation());
    }

    /**
     * Returns true if the login identity provider has been configured.
     *
     * @return true if the login identity provider has been configured
     */
    public boolean isLoginIdentityProviderEnabled() {
        return !StringUtils.isBlank(getProperty(NiFiProperties.SECURITY_USER_LOGIN_IDENTITY_PROVIDER));
    }

    /**
     * Returns whether an OpenId Connect (OIDC) URL is set.
     *
     * @return whether an OpenId Connection URL is set
     */
    public boolean isOidcEnabled() {
        return !StringUtils.isBlank(getOidcDiscoveryUrl());
    }

    /**
     * Returns the OpenId Connect (OIDC) URL. Null otherwise.
     *
     * @return OIDC discovery url
     */
    public String getOidcDiscoveryUrl() {
        return getProperty(SECURITY_USER_OIDC_DISCOVERY_URL);
    }

    /**
     * Returns the OpenId Connect connect timeout. Non null.
     *
     * @return OIDC connect timeout
     */
    public String getOidcConnectTimeout() {
        return getProperty(SECURITY_USER_OIDC_CONNECT_TIMEOUT, DEFAULT_SECURITY_USER_OIDC_CONNECT_TIMEOUT);
    }

    /**
     * Returns the OpenId Connect read timeout. Non null.
     *
     * @return OIDC read timeout
     */
    public String getOidcReadTimeout() {
        return getProperty(SECURITY_USER_OIDC_READ_TIMEOUT, DEFAULT_SECURITY_USER_OIDC_READ_TIMEOUT);
    }

    /**
     * Returns the OpenId Connect client id.
     *
     * @return OIDC client id
     */
    public String getOidcClientId() {
        return getProperty(SECURITY_USER_OIDC_CLIENT_ID);
    }

    /**
     * Returns the OpenId Connect client secret.
     *
     * @return OIDC client secret
     */
    public String getOidcClientSecret() {
        return getProperty(SECURITY_USER_OIDC_CLIENT_SECRET);
    }

    /**
     * Returns the preferred json web signature algorithm. May be null/blank.
     *
     * @return OIDC preferred json web signature algorithm
     */
    public String getOidcPreferredJwsAlgorithm() {
        return getProperty(SECURITY_USER_OIDC_PREFERRED_JWSALGORITHM);
    }

    /**
     * Returns whether Knox SSO is enabled.
     *
     * @return whether Knox SSO is enabled
     */
    public boolean isKnoxSsoEnabled() {
        return !StringUtils.isBlank(getKnoxUrl());
    }

    /**
     * Returns the Knox URL.
     *
     * @return Knox URL
     */
    public String getKnoxUrl() {
        return getProperty(SECURITY_USER_KNOX_URL);
    }

    /**
     * Gets the configured Knox Audiences.
     *
     * @return Knox audiences
     */
    public Set<String> getKnoxAudiences() {
        final String rawAudiences = getProperty(SECURITY_USER_KNOX_AUDIENCES);
        if (StringUtils.isBlank(rawAudiences)) {
            return null;
        } else {
            final String[] audienceTokens = rawAudiences.split(",");
            return Stream.of(audienceTokens).map(String::trim).filter(aud -> !StringUtils.isEmpty(aud)).collect(Collectors.toSet());
        }
    }

    /**
     * Returns the path to the Knox public key.
     *
     * @return path to the Knox public key
     */
    public Path getKnoxPublicKeyPath() {
        return Paths.get(getProperty(SECURITY_USER_KNOX_PUBLIC_KEY));
    }

    /**
     * Returns the name of the Knox cookie.
     *
     * @return name of the Knox cookie
     */
    public String getKnoxCookieName() {
        return getProperty(SECURITY_USER_KNOX_COOKIE_NAME);
    }

    /**
     * Returns true if client certificates are required for REST API. Determined
     * if the following conditions are all true:
     * <p>
     * - login identity provider is not populated
     * - Kerberos service support is not enabled
     * - openid connect is not enabled
     * - knox sso is not enabled
     * </p>
     *
     * @return true if client certificates are required for access to the REST API
     */
    public boolean isClientAuthRequiredForRestApi() {
        return !isLoginIdentityProviderEnabled() && !isKerberosSpnegoSupportEnabled() && !isOidcEnabled() && !isKnoxSsoEnabled();
    }

    public InetSocketAddress getNodeApiAddress() {

        final String rawScheme = getClusterProtocolManagerToNodeApiScheme();
        final String scheme = (rawScheme == null) ? "http" : rawScheme;

        final String host;
        final Integer port;
        if ("http".equalsIgnoreCase(scheme)) {
            // get host
            if (StringUtils.isBlank(getProperty(WEB_HTTP_HOST))) {
                host = "localhost";
            } else {
                host = getProperty(WEB_HTTP_HOST);
            }
            // get port
            port = getPort();

            if (port == null) {
                throw new RuntimeException(String.format("The %s must be specified if running in a cluster with %s set to false.", WEB_HTTP_PORT, CLUSTER_PROTOCOL_IS_SECURE));
            }
        } else {
            // get host
            if (StringUtils.isBlank(getProperty(WEB_HTTPS_HOST))) {
                host = "localhost";
            } else {
                host = getProperty(WEB_HTTPS_HOST);
            }
            // get port
            port = getSslPort();

            if (port == null) {
                throw new RuntimeException(String.format("The %s must be specified if running in a cluster with %s set to true.", WEB_HTTPS_PORT, CLUSTER_PROTOCOL_IS_SECURE));
            }
        }

        return InetSocketAddress.createUnresolved(host, port);

    }

    /**
     * Returns the database repository path. It simply returns the value
     * configured. No directories will be created as a result of this operation.
     *
     * @return database repository path
     * @throws InvalidPathException If the configured path is invalid
     */
    public Path getDatabaseRepositoryPath() {
        return Paths.get(getProperty(REPOSITORY_DATABASE_DIRECTORY));
    }

    /**
     * Returns the flow file repository path. It simply returns the value
     * configured. No directories will be created as a result of this operation.
     *
     * @return database repository path
     * @throws InvalidPathException If the configured path is invalid
     */
    public Path getFlowFileRepositoryPath() {
        return Paths.get(getProperty(FLOWFILE_REPOSITORY_DIRECTORY));
    }

    /**
     * Returns the content repository paths. This method returns a mapping of
     * file repository name to file repository paths. It simply returns the
     * values configured. No directories will be created as a result of this
     * operation.
     *
     * @return file repositories paths
     * @throws InvalidPathException If any of the configured paths are invalid
     */
    public Map<String, Path> getContentRepositoryPaths() {
        final Map<String, Path> contentRepositoryPaths = new HashMap<>();

        // go through each property
        for (String propertyName : getPropertyKeys()) {
            // determine if the property is a file repository path
            if (StringUtils.startsWith(propertyName, REPOSITORY_CONTENT_PREFIX)) {
                // get the repository key
                final String key = StringUtils.substringAfter(propertyName,
                        REPOSITORY_CONTENT_PREFIX);

                // attempt to resolve the path specified
                contentRepositoryPaths.put(key, Paths.get(getProperty(propertyName)));
            }
        }
        return contentRepositoryPaths;
    }

    /**
     * Returns the provenance repository paths. This method returns a mapping of
     * file repository name to file repository paths. It simply returns the
     * values configured. No directories will be created as a result of this
     * operation.
     *
     * @return the name and paths of all provenance repository locations
     */
    public Map<String, Path> getProvenanceRepositoryPaths() {
        final Map<String, Path> provenanceRepositoryPaths = new HashMap<>();

        // go through each property
        for (String propertyName : getPropertyKeys()) {
            // determine if the property is a file repository path
            if (StringUtils.startsWith(propertyName, PROVENANCE_REPO_DIRECTORY_PREFIX)) {
                // get the repository key
                final String key = StringUtils.substringAfter(propertyName,
                        PROVENANCE_REPO_DIRECTORY_PREFIX);

                // attempt to resolve the path specified
                provenanceRepositoryPaths.put(key, Paths.get(getProperty(propertyName)));
            }
        }
        return provenanceRepositoryPaths;
    }

    /**
     * Returns the number of claims to keep open for writing. Ideally, this will be at
     * least as large as the number of threads that will be updating the repository simultaneously but we don't want
     * to get too large because it will hold open up to this many FileOutputStreams.
     *
     * Default is {@link #DEFAULT_MAX_FLOWFILES_PER_CLAIM}
     *
     * @return the maximum number of flow files per claim
     */
    public int getMaxFlowFilesPerClaim() {
        try {
            return Integer.parseInt(getProperty(MAX_FLOWFILES_PER_CLAIM));
        } catch (NumberFormatException nfe) {
            return DEFAULT_MAX_FLOWFILES_PER_CLAIM;
        }
    }

    /**
     * Returns the maximum size, in bytes, that claims should grow before writing a new file. This means that we won't continually write to one
     * file that keeps growing but gives us a chance to bunch together many small files.
     *
     * Default is {@link #DEFAULT_MAX_APPENDABLE_CLAIM_SIZE}
     *
     * @return the maximum appendable claim size
     */
    public String getMaxAppendableClaimSize() {
        return getProperty(MAX_APPENDABLE_CLAIM_SIZE, DEFAULT_MAX_APPENDABLE_CLAIM_SIZE);
    }

    public String getProperty(final String key, final String defaultValue) {
        final String value = getProperty(key);
        return (value == null || value.trim().isEmpty()) ? defaultValue : value;
    }

    public String getBoredYieldDuration() {
        return getProperty(BORED_YIELD_DURATION, DEFAULT_BORED_YIELD_DURATION);
    }

    public File getStateManagementConfigFile() {
        return new File(getProperty(STATE_MANAGEMENT_CONFIG_FILE, DEFAULT_STATE_MANAGEMENT_CONFIG_FILE));
    }

    public String getLocalStateProviderId() {
        return getProperty(STATE_MANAGEMENT_LOCAL_PROVIDER_ID);
    }

    public String getClusterStateProviderId() {
        return getProperty(STATE_MANAGEMENT_CLUSTER_PROVIDER_ID);
    }

    public File getEmbeddedZooKeeperPropertiesFile() {
        final String filename = getProperty(STATE_MANAGEMENT_ZOOKEEPER_PROPERTIES);
        return filename == null ? null : new File(filename);
    }

    public boolean isStartEmbeddedZooKeeper() {
        return Boolean.parseBoolean(getProperty(STATE_MANAGEMENT_START_EMBEDDED_ZOOKEEPER));
    }

    public boolean isFlowConfigurationArchiveEnabled() {
        return Boolean.parseBoolean(getProperty(FLOW_CONFIGURATION_ARCHIVE_ENABLED, DEFAULT_FLOW_CONFIGURATION_ARCHIVE_ENABLED));
    }

    public String getFlowConfigurationArchiveDir() {
        return getProperty(FLOW_CONFIGURATION_ARCHIVE_DIR);
    }

    public String getFlowElectionMaxWaitTime() {
        return getProperty(FLOW_ELECTION_MAX_WAIT_TIME, DEFAULT_FLOW_ELECTION_MAX_WAIT_TIME);
    }

    public Integer getFlowElectionMaxCandidates() {
        return getIntegerProperty(FLOW_ELECTION_MAX_CANDIDATES, null);
    }

    public String getFlowConfigurationArchiveMaxTime() {
        return getProperty(FLOW_CONFIGURATION_ARCHIVE_MAX_TIME, null);
    }

    public String getFlowConfigurationArchiveMaxStorage() {
        return getProperty(FLOW_CONFIGURATION_ARCHIVE_MAX_STORAGE, null);
    }

    public Integer getFlowConfigurationArchiveMaxCount() {
        return getIntegerProperty(FLOW_CONFIGURATION_ARCHIVE_MAX_COUNT, null);
    }

    public String getVariableRegistryProperties() {
        return getProperty(VARIABLE_REGISTRY_PROPERTIES);
    }

    public Path[] getVariableRegistryPropertiesPaths() {
        final List<Path> vrPropertiesPaths = new ArrayList<>();

        final String vrPropertiesFiles = getVariableRegistryProperties();
        if (!StringUtils.isEmpty(vrPropertiesFiles)) {

            final List<String> vrPropertiesFileList = Arrays.asList(vrPropertiesFiles.split(","));

            for (String propertiesFile : vrPropertiesFileList) {
                vrPropertiesPaths.add(Paths.get(propertiesFile));
            }

            return vrPropertiesPaths.toArray(new Path[vrPropertiesPaths.size()]);
        } else {
            return new Path[]{};
        }
    }

    /**
     * Returns the network interface list to use for HTTP. This method returns a mapping of
     * network interface property names to network interface names.
     *
     * @return the property name and network interface name of all HTTP network interfaces
     */
    public Map<String, String> getHttpNetworkInterfaces() {
        final Map<String, String> networkInterfaces = new HashMap<>();

        // go through each property
        for (String propertyName : getPropertyKeys()) {
            // determine if the property is a network interface name
            if (StringUtils.startsWith(propertyName, WEB_HTTP_NETWORK_INTERFACE_PREFIX)) {
                // get the network interface property key
                final String key = StringUtils.substringAfter(propertyName,
                        WEB_HTTP_NETWORK_INTERFACE_PREFIX);
                networkInterfaces.put(key, getProperty(propertyName));
            }
        }
        return networkInterfaces;
    }

    /**
     * Returns the network interface list to use for HTTPS. This method returns a mapping of
     * network interface property names to network interface names.
     *
     * @return the property name and network interface name of all HTTPS network interfaces
     */
    public Map<String, String> getHttpsNetworkInterfaces() {
        final Map<String, String> networkInterfaces = new HashMap<>();

        // go through each property
        for (String propertyName : getPropertyKeys()) {
            // determine if the property is a network interface name
            if (StringUtils.startsWith(propertyName, WEB_HTTPS_NETWORK_INTERFACE_PREFIX)) {
                // get the network interface property key
                final String key = StringUtils.substringAfter(propertyName,
                        WEB_HTTPS_NETWORK_INTERFACE_PREFIX);
                networkInterfaces.put(key, getProperty(propertyName));
            }
        }
        return networkInterfaces;
    }

    public int size() {
        return getPropertyKeys().size();
    }

    public String getProvenanceRepoEncryptionKeyId() {
        return getProperty(PROVENANCE_REPO_ENCRYPTION_KEY_ID);
    }

    /**
     * Returns the active provenance repository encryption key if a {@code StaticKeyProvider} is in use.
     * If no key ID is specified in the properties file, the default
     * {@code nifi.provenance.repository.encryption.key} value is returned. If a key ID is specified in
     * {@code nifi.provenance.repository.encryption.key.id}, it will attempt to read from
     * {@code nifi.provenance.repository.encryption.key.id.XYZ} where {@code XYZ} is the provided key
     * ID. If that value is empty, it will use the default property
     * {@code nifi.provenance.repository.encryption.key}.
     *
     * @return the provenance repository encryption key in hex form
     */
    public String getProvenanceRepoEncryptionKey() {
        String keyId = getProvenanceRepoEncryptionKeyId();
        String keyKey = StringUtils.isBlank(keyId) ? PROVENANCE_REPO_ENCRYPTION_KEY : PROVENANCE_REPO_ENCRYPTION_KEY + ".id." + keyId;
        return getProperty(keyKey, getProperty(PROVENANCE_REPO_ENCRYPTION_KEY));
    }

    /**
     * Returns a map of keyId -> key in hex loaded from the {@code nifi.properties} file if a
     * {@code StaticKeyProvider} is defined. If {@code FileBasedKeyProvider} is defined, use
     * {@code CryptoUtils#readKeys()} instead -- this method will return an empty map.
     *
     * @return a Map of the keys identified by key ID
     */
    public Map<String, String> getProvenanceRepoEncryptionKeys() {
        Map<String, String> keys = new HashMap<>();
        List<String> keyProperties = getProvenanceRepositoryEncryptionKeyProperties();

        // Retrieve the actual key values and store non-empty values in the map
        for (String prop : keyProperties) {
            final String value = getProperty(prop);
            if (!StringUtils.isBlank(value)) {
                if (prop.equalsIgnoreCase(PROVENANCE_REPO_ENCRYPTION_KEY)) {
                    prop = getProvenanceRepoEncryptionKeyId();
                } else {
                    // Extract nifi.provenance.repository.encryption.key.id.key1 -> key1
                    prop = prop.substring(prop.lastIndexOf(".") + 1);
                }
                keys.put(prop, value);
            }
        }
        return keys;
    }

    private List<String> getProvenanceRepositoryEncryptionKeyProperties() {
        // Filter all the property keys that define a key
        return getPropertyKeys().stream().filter(k ->
                k.startsWith(PROVENANCE_REPO_ENCRYPTION_KEY_ID + ".") || k.equalsIgnoreCase(PROVENANCE_REPO_ENCRYPTION_KEY)
        ).collect(Collectors.toList());
    }

    /**
     * Creates an instance of NiFiProperties. This should likely not be called
     * by any classes outside of the NiFi framework but can be useful by the
     * framework for default property loading behavior or helpful in tests
     * needing to create specific instances of NiFiProperties. If properties
     * file specified cannot be found/read a runtime exception will be thrown.
     * If one is not specified no properties will be loaded by default.
     *
     * @param propertiesFilePath   if provided properties will be loaded from
     *                             given file; else will be loaded from System property. Can be null.
     * @param additionalProperties allows overriding of properties with the
     *                             supplied values. these will be applied after loading from any properties
     *                             file. Can be null or empty.
     * @return NiFiProperties
     */
    public static NiFiProperties createBasicNiFiProperties(final String propertiesFilePath, final Map<String, String> additionalProperties) {
        final Map<String, String> addProps = (additionalProperties == null) ? Collections.EMPTY_MAP : additionalProperties;
        final Properties properties = new Properties();
        final String nfPropertiesFilePath = (propertiesFilePath == null)
                ? System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)
                : propertiesFilePath;
        if (nfPropertiesFilePath != null) {
            final File propertiesFile = new File(nfPropertiesFilePath.trim());
            if (!propertiesFile.exists()) {
                throw new RuntimeException("Properties file doesn't exist \'"
                        + propertiesFile.getAbsolutePath() + "\'");
            }
            if (!propertiesFile.canRead()) {
                throw new RuntimeException("Properties file exists but cannot be read \'"
                        + propertiesFile.getAbsolutePath() + "\'");
            }
            InputStream inStream = null;
            try {
                inStream = new BufferedInputStream(new FileInputStream(propertiesFile));
                properties.load(inStream);
            } catch (final Exception ex) {
                throw new RuntimeException("Cannot load properties file due to "
                        + ex.getLocalizedMessage(), ex);
            } finally {
                if (null != inStream) {
                    try {
                        inStream.close();
                    } catch (final Exception ex) {
                        /**
                         * do nothing *
                         */
                    }
                }
            }
        }
        addProps.entrySet().stream().forEach((entry) -> {
            properties.setProperty(entry.getKey(), entry.getValue());
        });
        return new NiFiProperties() {
            @Override
            public String getProperty(String key) {
                return properties.getProperty(key);
            }

            @Override
            public Set<String> getPropertyKeys() {
                return properties.stringPropertyNames();
            }
        };
    }

    /**
     * This method is used to validate the NiFi properties when the file is loaded
     * for the first time. The objective is to stop NiFi startup in case a property
     * is not correctly configured and could cause issues afterwards.
     */
    public void validate() {
        // REMOTE_INPUT_HOST should be a valid hostname
        String remoteInputHost = getProperty(REMOTE_INPUT_HOST);
        if (!StringUtils.isBlank(remoteInputHost) && remoteInputHost.split(":").length > 1) { // no scheme/port needed here (http://)
            throw new IllegalArgumentException(remoteInputHost + " is not a correct value for " + REMOTE_INPUT_HOST + ". It should be a valid hostname without protocol or port.");
        }
        // Other properties to validate...
    }
}
