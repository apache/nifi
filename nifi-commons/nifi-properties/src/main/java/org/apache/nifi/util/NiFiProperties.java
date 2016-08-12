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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class NiFiProperties extends Properties {

    private static final long serialVersionUID = 2119177359005492702L;

    private static final Logger LOG = LoggerFactory.getLogger(NiFiProperties.class);
    private static NiFiProperties instance = null;

    // core properties
    public static final String PROPERTIES_FILE_PATH = "nifi.properties.file.path";
    public static final String FLOW_CONFIGURATION_FILE = "nifi.flow.configuration.file";
    public static final String FLOW_CONFIGURATION_ARCHIVE_ENABLED = "nifi.flow.configuration.archive.enabled";
    public static final String FLOW_CONFIGURATION_ARCHIVE_DIR = "nifi.flow.configuration.archive.dir";
    public static final String FLOW_CONFIGURATION_ARCHIVE_MAX_TIME = "nifi.flow.configuration.archive.max.time";
    public static final String FLOW_CONFIGURATION_ARCHIVE_MAX_STORAGE = "nifi.flow.configuration.archive.max.storage";
    public static final String AUTHORIZER_CONFIGURATION_FILE = "nifi.authorizer.configuration.file";
    public static final String LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE = "nifi.login.identity.provider.configuration.file";
    public static final String REPOSITORY_DATABASE_DIRECTORY = "nifi.database.directory";
    public static final String RESTORE_DIRECTORY = "nifi.restore.directory";
    public static final String VERSION = "nifi.version";
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
    public static final String SECURITY_CLUSTER_AUTHORITY_PROVIDER_PORT = "nifi.security.cluster.authority.provider.port";
    public static final String SECURITY_CLUSTER_AUTHORITY_PROVIDER_THREADS = "nifi.security.cluster.authority.provider.threads";
    public static final String SECURITY_OCSP_RESPONDER_URL = "nifi.security.ocsp.responder.url";
    public static final String SECURITY_OCSP_RESPONDER_CERTIFICATE = "nifi.security.ocsp.responder.certificate";
    public static final String SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX = "nifi.security.identity.mapping.pattern.";
    public static final String SECURITY_IDENTITY_MAPPING_VALUE_PREFIX = "nifi.security.identity.mapping.value.";

    // web properties
    public static final String WEB_WAR_DIR = "nifi.web.war.directory";
    public static final String WEB_HTTP_PORT = "nifi.web.http.port";
    public static final String WEB_HTTP_HOST = "nifi.web.http.host";
    public static final String WEB_HTTPS_PORT = "nifi.web.https.port";
    public static final String WEB_HTTPS_HOST = "nifi.web.https.host";
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
    public static final String CLUSTER_NODE_CONNECTION_TIMEOUT = "nifi.cluster.node.connection.timeout";
    public static final String CLUSTER_NODE_READ_TIMEOUT = "nifi.cluster.node.read.timeout";
    public static final String CLUSTER_FIREWALL_FILE = "nifi.cluster.firewall.file";

    // zookeeper properties
    public static final String ZOOKEEPER_CONNECT_STRING = "nifi.zookeeper.connect.string";
    public static final String ZOOKEEPER_CONNECT_TIMEOUT = "nifi.zookeeper.connect.timeout";
    public static final String ZOOKEEPER_SESSION_TIMEOUT = "nifi.zookeeper.session.timeout";
    public static final String ZOOKEEPER_ROOT_NODE = "nifi.zookeeper.root.node";


    // kerberos properties
    public static final String KERBEROS_KRB5_FILE = "nifi.kerberos.krb5.file";
    public static final String KERBEROS_SERVICE_PRINCIPAL = "nifi.kerberos.service.principal";
    public static final String KERBEROS_KEYTAB_LOCATION = "nifi.kerberos.keytab.location";
    public static final String KERBEROS_AUTHENTICATION_EXPIRATION = "nifi.kerberos.authentication.expiration";

    // state management
    public static final String STATE_MANAGEMENT_CONFIG_FILE = "nifi.state.management.configuration.file";
    public static final String STATE_MANAGEMENT_LOCAL_PROVIDER_ID = "nifi.state.management.provider.local";
    public static final String STATE_MANAGEMENT_CLUSTER_PROVIDER_ID = "nifi.state.management.provider.cluster";
    public static final String STATE_MANAGEMENT_START_EMBEDDED_ZOOKEEPER = "nifi.state.management.embedded.zookeeper.start";
    public static final String STATE_MANAGEMENT_ZOOKEEPER_PROPERTIES = "nifi.state.management.embedded.zookeeper.properties";

    // expression language properties
    public static final String VARIABLE_REGISTRY_PROPERTIES = "nifi.variable.registry.properties";

    // defaults
    public static final String DEFAULT_TITLE = "NiFi";
    public static final Boolean DEFAULT_AUTO_RESUME_STATE = true;
    public static final String DEFAULT_AUTHORIZER_CONFIGURATION_FILE = "conf/authorizers.xml";
    public static final String DEFAULT_LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE = "conf/login-identity-providers.xml";
    public static final String DEFAULT_USER_CREDENTIAL_CACHE_DURATION = "24 hours";
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
    public static final String DEFAULT_SITE_TO_SITE_HTTP_TRANSACTION_TTL = "30 secs";
    public static final String DEFAULT_FLOW_CONFIGURATION_ARCHIVE_ENABLED = "true";
    public static final String DEFAULT_FLOW_CONFIGURATION_ARCHIVE_MAX_TIME = "30 days";
    public static final String DEFAULT_FLOW_CONFIGURATION_ARCHIVE_MAX_STORAGE = "500 MB";

    // cluster common defaults
    public static final String DEFAULT_CLUSTER_PROTOCOL_HEARTBEAT_INTERVAL = "5 sec";
    public static final String DEFAULT_CLUSTER_PROTOCOL_MULTICAST_SERVICE_BROADCAST_DELAY = "500 ms";
    public static final int DEFAULT_CLUSTER_PROTOCOL_MULTICAST_SERVICE_LOCATOR_ATTEMPTS = 3;
    public static final String DEFAULT_CLUSTER_PROTOCOL_MULTICAST_SERVICE_LOCATOR_ATTEMPTS_DELAY = "1 sec";
    public static final String DEFAULT_CLUSTER_NODE_READ_TIMEOUT = "5 sec";
    public static final String DEFAULT_CLUSTER_NODE_CONNECTION_TIMEOUT = "5 sec";

    // cluster node defaults
    public static final int DEFAULT_CLUSTER_NODE_PROTOCOL_THREADS = 2;
    public static final String DEFAULT_REQUEST_REPLICATION_CLAIM_TIMEOUT = "15 secs";

    // state management defaults
    public static final String DEFAULT_STATE_MANAGEMENT_CONFIG_FILE = "conf/state-management.xml";

    // Kerberos defaults
    public static final String DEFAULT_KERBEROS_AUTHENTICATION_EXPIRATION = "12 hours";

    private NiFiProperties() {
        super();
    }

    public NiFiProperties copy() {
        final NiFiProperties copy = new NiFiProperties();
        copy.putAll(this);
        return copy;
    }

    /**
     * Factory method to create an instance of the {@link NiFiProperties}. This
     * method employs a standard singleton pattern by caching the instance if it
     * was already obtained
     *
     * @return instance of {@link NiFiProperties}
     */
    public static synchronized NiFiProperties getInstance() {
        // NOTE: unit tests can set instance to null (with reflection) to effectively create a new singleton.
        //       changing the below as a check for whether the instance was initialized will break those
        //       unit tests.
        if (null == instance) {
            final NiFiProperties suspectInstance = new NiFiProperties();
            final String nfPropertiesFilePath = System
                    .getProperty(NiFiProperties.PROPERTIES_FILE_PATH);
            if (null == nfPropertiesFilePath || nfPropertiesFilePath.trim().length() == 0) {
                throw new RuntimeException("Requires a system property called \'"
                        + NiFiProperties.PROPERTIES_FILE_PATH
                        + "\' and this is not set or has no value");
            }
            final File propertiesFile = new File(nfPropertiesFilePath);
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
                suspectInstance.load(inStream);
            } catch (final Exception ex) {
                LOG.error("Cannot load properties file due to " + ex.getLocalizedMessage());
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
            instance = suspectInstance;
        }
        return instance;
    }

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
        if (value == null) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(getProperty(propertyName));
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
     * The host name that will be given out to clients to connect to the Remote Input Port.
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

        if ("false".equalsIgnoreCase(secureVal)) {
            return false;
        } else {
            return true;
        }

    }

    /**
     * @return True if property value is 'true'; False otherwise.
     */
    public Boolean isSiteToSiteHttpEnabled() {
        final String remoteInputHttpEnabled = getProperty(SITE_TO_SITE_HTTP_ENABLED, "false");

        if ("true".equalsIgnoreCase(remoteInputHttpEnabled)) {
            return true;
        } else {
            return false;
        }

    }

    /**
     * The HTTP or HTTPS Web API port for a Remote Input Port.
     * @return the remote input port for HTTP(S) communication, or null if HTTP(S) Site-to-Site is not enabled
     */
    public Integer getRemoteInputHttpPort() {
        if (!isSiteToSiteHttpEnabled()) {
            return null;
        }

        String propertyKey = isSiteToSiteSecure() ? NiFiProperties.WEB_HTTPS_PORT : NiFiProperties.WEB_HTTP_PORT;
        Integer port = getIntegerProperty(propertyKey, 0);
        if (port == 0) {
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
     * Returns whether the processors should be started automatically when the application loads.
     *
     * @return Whether to auto start the processors or not
     */
    public boolean getAutoResumeState() {
        final String rawAutoResumeState = getProperty(AUTO_RESUME_STATE,
                DEFAULT_AUTO_RESUME_STATE.toString());
        return Boolean.parseBoolean(rawAutoResumeState);
    }

    /**
     * Returns the number of partitions that should be used for the FlowFile Repository
     *
     * @return the number of partitions
     */
    public int getFlowFileRepositoryPartitions() {
        final String rawProperty = getProperty(FLOWFILE_REPOSITORY_PARTITIONS,
                DEFAULT_FLOWFILE_REPO_PARTITIONS);
        return Integer.parseInt(rawProperty);
    }

    /**
     * Returns the number of milliseconds between FlowFileRepository checkpointing
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
        for (String propertyName : stringPropertyNames()) {
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
     * Get the title for the UI.
     *
     * @return The UI title
     */
    public String getUiTitle() {
        return this.getProperty(VERSION, DEFAULT_TITLE);
    }

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

    public int getClusterNodeProtocolThreads() {
        try {
            return Integer.parseInt(getProperty(CLUSTER_NODE_PROTOCOL_THREADS));
        } catch (NumberFormatException nfe) {
            return DEFAULT_CLUSTER_NODE_PROTOCOL_THREADS;
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

    public String getKerberosKeytabLocation() {
        final String keytabLocation = getProperty(KERBEROS_KEYTAB_LOCATION);
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
     * Returns true if the Kerberos service principal and keytab location properties are populated.
     *
     * @return true if Kerberos service support is enabled
     */
    public boolean isKerberosServiceSupportEnabled() {
        return !StringUtils.isBlank(getKerberosServicePrincipal()) && !StringUtils.isBlank(getKerberosKeytabLocation());
    }

    /**
     * Returns true if client certificates are required for REST API. Determined if the following conditions are all true:
     *
     * - login identity provider is not populated
     * - Kerberos service support is not enabled
     *
     * @return true if client certificates are required for access to the REST API
     */
    public boolean isClientAuthRequiredForRestApi() {
        return StringUtils.isBlank(getProperty(NiFiProperties.SECURITY_USER_LOGIN_IDENTITY_PROVIDER)) && !isKerberosServiceSupportEnabled();
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
     * Returns the database repository path. It simply returns the value configured. No directories will be created as a result of this operation.
     *
     * @return database repository path
     * @throws InvalidPathException If the configured path is invalid
     */
    public Path getDatabaseRepositoryPath() {
        return Paths.get(getProperty(REPOSITORY_DATABASE_DIRECTORY));
    }

    /**
     * Returns the flow file repository path. It simply returns the value configured. No directories will be created as a result of this operation.
     *
     * @return database repository path
     * @throws InvalidPathException If the configured path is invalid
     */
    public Path getFlowFileRepositoryPath() {
        return Paths.get(getProperty(FLOWFILE_REPOSITORY_DIRECTORY));
    }

    /**
     * Returns the content repository paths. This method returns a mapping of file repository name to file repository paths. It simply returns the values configured. No directories will be created as
     * a result of this operation.
     *
     * @return file repositories paths
     * @throws InvalidPathException If any of the configured paths are invalid
     */
    public Map<String, Path> getContentRepositoryPaths() {
        final Map<String, Path> contentRepositoryPaths = new HashMap<>();

        // go through each property
        for (String propertyName : stringPropertyNames()) {
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
     * Returns the provenance repository paths. This method returns a mapping of file repository name to file repository paths. It simply returns the values configured. No directories will be created
     * as a result of this operation.
     *
     * @return the name and paths of all provenance repository locations
     */
    public Map<String, Path> getProvenanceRepositoryPaths() {
        final Map<String, Path> provenanceRepositoryPaths = new HashMap<>();

        // go through each property
        for (String propertyName : stringPropertyNames()) {
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

    public int getMaxFlowFilesPerClaim() {
        try {
            return Integer.parseInt(getProperty(MAX_FLOWFILES_PER_CLAIM));
        } catch (NumberFormatException nfe) {
            return DEFAULT_MAX_FLOWFILES_PER_CLAIM;
        }
    }

    public String getMaxAppendableClaimSize() {
        return getProperty(MAX_APPENDABLE_CLAIM_SIZE);
    }

    @Override
    public String getProperty(final String key, final String defaultValue) {
        final String value = super.getProperty(key, defaultValue);
        if (value == null) {
            return null;
        }

        if (value.trim().isEmpty()) {
            return defaultValue;
        }
        return value;
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

    public String getFlowConfigurationArchiveMaxTime() {
        return getProperty(FLOW_CONFIGURATION_ARCHIVE_MAX_TIME, DEFAULT_FLOW_CONFIGURATION_ARCHIVE_MAX_TIME);
    }

    public String getFlowConfigurationArchiveMaxStorage() {
        return getProperty(FLOW_CONFIGURATION_ARCHIVE_MAX_STORAGE, DEFAULT_FLOW_CONFIGURATION_ARCHIVE_MAX_STORAGE);
    }

    public String getVariableRegistryProperties(){
        return getProperty(VARIABLE_REGISTRY_PROPERTIES);
    }

    public Path[] getVariableRegistryPropertiesPaths() {
        final List<Path> vrPropertiesPaths = new ArrayList<>();

        final String vrPropertiesFiles = getVariableRegistryProperties();
        if(!StringUtils.isEmpty(vrPropertiesFiles)) {

            final List<String> vrPropertiesFileList = Arrays.asList(vrPropertiesFiles.split(","));

            for(String propertiesFile : vrPropertiesFileList){
                vrPropertiesPaths.add(Paths.get(propertiesFile));
            }

            return vrPropertiesPaths.toArray( new Path[vrPropertiesPaths.size()]);
        } else {
            return new Path[]{};
        }
    }

}
