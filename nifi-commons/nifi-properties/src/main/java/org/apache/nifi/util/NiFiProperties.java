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

import org.apache.nifi.properties.ApplicationProperties;
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
public class NiFiProperties extends ApplicationProperties {
    private static final Logger logger = LoggerFactory.getLogger(NiFiProperties.class);

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
    public static final String NAR_LIBRARY_AUTOLOAD_DIRECTORY = "nifi.nar.library.autoload.directory";
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
    public static final String REMOTE_CONTENTS_CACHE_EXPIRATION = "nifi.remote.contents.cache.expiration";
    public static final String TEMPLATE_DIRECTORY = "nifi.templates.directory";
    public static final String ADMINISTRATIVE_YIELD_DURATION = "nifi.administrative.yield.duration";
    public static final String BORED_YIELD_DURATION = "nifi.bored.yield.duration";
    public static final String PROCESSOR_SCHEDULING_TIMEOUT = "nifi.processor.scheduling.timeout";
    public static final String BACKPRESSURE_COUNT = "nifi.queue.backpressure.count";
    public static final String BACKPRESSURE_SIZE = "nifi.queue.backpressure.size";

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
    public static final String CONTENT_REPOSITORY_ENCRYPTION_KEY = "nifi.content.repository.encryption.key";
    public static final String CONTENT_REPOSITORY_ENCRYPTION_KEY_ID = "nifi.content.repository.encryption.key.id";
    public static final String CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS = "nifi.content.repository.encryption.key.provider.implementation";
    public static final String CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION = "nifi.content.repository.encryption.key.provider.location";
    public static final String CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_PASSWORD = "nifi.content.repository.encryption.key.provider.password";

    // flowfile repository properties
    public static final String FLOWFILE_REPOSITORY_IMPLEMENTATION = "nifi.flowfile.repository.implementation";
    public static final String FLOWFILE_REPOSITORY_WAL_IMPLEMENTATION = "nifi.flowfile.repository.wal.implementation";
    public static final String FLOWFILE_REPOSITORY_ALWAYS_SYNC = "nifi.flowfile.repository.always.sync";
    public static final String FLOWFILE_REPOSITORY_DIRECTORY = "nifi.flowfile.repository.directory";
    public static final String FLOWFILE_REPOSITORY_CHECKPOINT_INTERVAL = "nifi.flowfile.repository.checkpoint.interval";
    public static final String FLOWFILE_REPOSITORY_ENCRYPTION_KEY = "nifi.flowfile.repository.encryption.key";
    public static final String FLOWFILE_REPOSITORY_ENCRYPTION_KEY_ID = "nifi.flowfile.repository.encryption.key.id";
    public static final String FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS = "nifi.flowfile.repository.encryption.key.provider.implementation";
    public static final String FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION = "nifi.flowfile.repository.encryption.key.provider.location";
    public static final String FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_PASSWORD = "nifi.flowfile.repository.encryption.key.provider.password";
    public static final String FLOWFILE_SWAP_MANAGER_IMPLEMENTATION = "nifi.swap.manager.implementation";
    public static final String QUEUE_SWAP_THRESHOLD = "nifi.queue.swap.threshold";

    // provenance properties
    public static final String PROVENANCE_REPO_IMPLEMENTATION_CLASS = "nifi.provenance.repository.implementation";
    public static final String PROVENANCE_REPO_DIRECTORY_PREFIX = "nifi.provenance.repository.directory.";
    public static final String PROVENANCE_MAX_STORAGE_TIME = "nifi.provenance.repository.max.storage.time";
    public static final String PROVENANCE_MAX_STORAGE_SIZE = "nifi.provenance.repository.max.storage.size";
    public static final String PROVENANCE_ROLLOVER_TIME = "nifi.provenance.repository.rollover.time";
    public static final String PROVENANCE_ROLLOVER_SIZE = "nifi.provenance.repository.rollover.size";
    public static final String PROVENANCE_ROLLOVER_EVENT_COUNT = "nifi.provenance.repository.rollover.events";
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
    public static final String PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_PASSWORD = "nifi.provenance.repository.encryption.key.provider.password";
    public static final String PROVENANCE_REPO_DEBUG_FREQUENCY = "nifi.provenance.repository.debug.frequency";

    // status repository properties
    public static final String COMPONENT_STATUS_REPOSITORY_IMPLEMENTATION = "nifi.components.status.repository.implementation";
    public static final String COMPONENT_STATUS_SNAPSHOT_FREQUENCY = "nifi.components.status.snapshot.frequency";

    // questdb status storage properties
    public static final String STATUS_REPOSITORY_QUESTDB_PERSIST_NODE_DAYS = "nifi.status.repository.questdb.persist.node.days";
    public static final String STATUS_REPOSITORY_QUESTDB_PERSIST_COMPONENT_DAYS = "nifi.status.repository.questdb.persist.component.days";
    public static final String STATUS_REPOSITORY_QUESTDB_PERSIST_LOCATION = "nifi.status.repository.questdb.persist.location";

    // security properties
    public static final String SECURITY_KEYSTORE = "nifi.security.keystore";
    public static final String SECURITY_KEYSTORE_TYPE = "nifi.security.keystoreType";
    public static final String SECURITY_KEYSTORE_PASSWD = "nifi.security.keystorePasswd";
    public static final String SECURITY_KEY_PASSWD = "nifi.security.keyPasswd";
    public static final String SECURITY_TRUSTSTORE = "nifi.security.truststore";
    public static final String SECURITY_TRUSTSTORE_TYPE = "nifi.security.truststoreType";
    public static final String SECURITY_TRUSTSTORE_PASSWD = "nifi.security.truststorePasswd";
    public static final String SECURITY_AUTO_RELOAD_ENABLED = "nifi.security.autoreload.enabled";
    public static final String SECURITY_AUTO_RELOAD_INTERVAL = "nifi.security.autoreload.interval";
    public static final String SECURITY_USER_AUTHORIZER = "nifi.security.user.authorizer";
    public static final String SECURITY_ANONYMOUS_AUTHENTICATION = "nifi.security.allow.anonymous.authentication";
    public static final String SECURITY_USER_LOGIN_IDENTITY_PROVIDER = "nifi.security.user.login.identity.provider";
    public static final String SECURITY_OCSP_RESPONDER_URL = "nifi.security.ocsp.responder.url";
    public static final String SECURITY_OCSP_RESPONDER_CERTIFICATE = "nifi.security.ocsp.responder.certificate";
    public static final String SECURITY_IDENTITY_MAPPING_PATTERN_PREFIX = "nifi.security.identity.mapping.pattern.";
    public static final String SECURITY_IDENTITY_MAPPING_VALUE_PREFIX = "nifi.security.identity.mapping.value.";
    public static final String SECURITY_IDENTITY_MAPPING_TRANSFORM_PREFIX = "nifi.security.identity.mapping.transform.";
    public static final String SECURITY_GROUP_MAPPING_PATTERN_PREFIX = "nifi.security.group.mapping.pattern.";
    public static final String SECURITY_GROUP_MAPPING_VALUE_PREFIX = "nifi.security.group.mapping.value.";
    public static final String SECURITY_GROUP_MAPPING_TRANSFORM_PREFIX = "nifi.security.group.mapping.transform.";

    // oidc
    public static final String SECURITY_USER_OIDC_DISCOVERY_URL = "nifi.security.user.oidc.discovery.url";
    public static final String SECURITY_USER_OIDC_CONNECT_TIMEOUT = "nifi.security.user.oidc.connect.timeout";
    public static final String SECURITY_USER_OIDC_READ_TIMEOUT = "nifi.security.user.oidc.read.timeout";
    public static final String SECURITY_USER_OIDC_CLIENT_ID = "nifi.security.user.oidc.client.id";
    public static final String SECURITY_USER_OIDC_CLIENT_SECRET = "nifi.security.user.oidc.client.secret";
    public static final String SECURITY_USER_OIDC_PREFERRED_JWSALGORITHM = "nifi.security.user.oidc.preferred.jwsalgorithm";
    public static final String SECURITY_USER_OIDC_ADDITIONAL_SCOPES = "nifi.security.user.oidc.additional.scopes";
    public static final String SECURITY_USER_OIDC_CLAIM_IDENTIFYING_USER = "nifi.security.user.oidc.claim.identifying.user";
    public static final String SECURITY_USER_OIDC_FALLBACK_CLAIMS_IDENTIFYING_USER = "nifi.security.user.oidc.fallback.claims.identifying.user";

    // apache knox
    public static final String SECURITY_USER_KNOX_URL = "nifi.security.user.knox.url";
    public static final String SECURITY_USER_KNOX_PUBLIC_KEY = "nifi.security.user.knox.publicKey";
    public static final String SECURITY_USER_KNOX_COOKIE_NAME = "nifi.security.user.knox.cookieName";
    public static final String SECURITY_USER_KNOX_AUDIENCES = "nifi.security.user.knox.audiences";

    // saml
    public static final String SECURITY_USER_SAML_IDP_METADATA_URL = "nifi.security.user.saml.idp.metadata.url";
    public static final String SECURITY_USER_SAML_SP_ENTITY_ID = "nifi.security.user.saml.sp.entity.id";
    public static final String SECURITY_USER_SAML_IDENTITY_ATTRIBUTE_NAME = "nifi.security.user.saml.identity.attribute.name";
    public static final String SECURITY_USER_SAML_GROUP_ATTRIBUTE_NAME = "nifi.security.user.saml.group.attribute.name";
    public static final String SECURITY_USER_SAML_METADATA_SIGNING_ENABLED = "nifi.security.user.saml.metadata.signing.enabled";
    public static final String SECURITY_USER_SAML_REQUEST_SIGNING_ENABLED = "nifi.security.user.saml.request.signing.enabled";
    public static final String SECURITY_USER_SAML_WANT_ASSERTIONS_SIGNED = "nifi.security.user.saml.want.assertions.signed";
    public static final String SECURITY_USER_SAML_SIGNATURE_ALGORITHM = "nifi.security.user.saml.signature.algorithm";
    public static final String SECURITY_USER_SAML_SIGNATURE_DIGEST_ALGORITHM = "nifi.security.user.saml.signature.digest.algorithm";
    public static final String SECURITY_USER_SAML_MESSAGE_LOGGING_ENABLED = "nifi.security.user.saml.message.logging.enabled";
    public static final String SECURITY_USER_SAML_AUTHENTICATION_EXPIRATION = "nifi.security.user.saml.authentication.expiration";
    public static final String SECURITY_USER_SAML_SINGLE_LOGOUT_ENABLED = "nifi.security.user.saml.single.logout.enabled";
    public static final String SECURITY_USER_SAML_HTTP_CLIENT_TRUSTSTORE_STRATEGY = "nifi.security.user.saml.http.client.truststore.strategy";
    public static final String SECURITY_USER_SAML_HTTP_CLIENT_CONNECT_TIMEOUT = "nifi.security.user.saml.http.client.connect.timeout";
    public static final String SECURITY_USER_SAML_HTTP_CLIENT_READ_TIMEOUT = "nifi.security.user.saml.http.client.read.timeout";

    // web properties
    public static final String WEB_HTTP_PORT = "nifi.web.http.port";
    public static final String WEB_HTTP_PORT_FORWARDING = "nifi.web.http.port.forwarding";
    public static final String WEB_HTTP_HOST = "nifi.web.http.host";
    public static final String WEB_HTTP_NETWORK_INTERFACE_PREFIX = "nifi.web.http.network.interface.";
    public static final String WEB_HTTPS_PORT = "nifi.web.https.port";
    public static final String WEB_HTTPS_PORT_FORWARDING = "nifi.web.https.port.forwarding";
    public static final String WEB_HTTPS_HOST = "nifi.web.https.host";
    public static final String WEB_HTTPS_CIPHERSUITES_INCLUDE = "nifi.web.https.ciphersuites.include";
    public static final String WEB_HTTPS_CIPHERSUITES_EXCLUDE = "nifi.web.https.ciphersuites.exclude";
    public static final String WEB_HTTPS_NETWORK_INTERFACE_PREFIX = "nifi.web.https.network.interface.";
    public static final String WEB_WORKING_DIR = "nifi.web.jetty.working.directory";
    public static final String WEB_THREADS = "nifi.web.jetty.threads";
    public static final String WEB_MAX_HEADER_SIZE = "nifi.web.max.header.size";
    public static final String WEB_PROXY_CONTEXT_PATH = "nifi.web.proxy.context.path";
    public static final String WEB_PROXY_HOST = "nifi.web.proxy.host";
    public static final String WEB_MAX_CONTENT_SIZE = "nifi.web.max.content.size";
    public static final String WEB_MAX_REQUESTS_PER_SECOND = "nifi.web.max.requests.per.second";
    public static final String WEB_REQUEST_TIMEOUT = "nifi.web.request.timeout";
    public static final String WEB_REQUEST_IP_WHITELIST = "nifi.web.request.ip.whitelist";
    public static final String WEB_SHOULD_SEND_SERVER_VERSION = "nifi.web.should.send.server.version";

    // ui properties
    public static final String UI_BANNER_TEXT = "nifi.ui.banner.text";
    public static final String UI_AUTO_REFRESH_INTERVAL = "nifi.ui.autorefresh.interval";

    // cluster common properties
    public static final String CLUSTER_PROTOCOL_HEARTBEAT_INTERVAL = "nifi.cluster.protocol.heartbeat.interval";
    public static final String CLUSTER_PROTOCOL_HEARTBEAT_MISSABLE_MAX = "nifi.cluster.protocol.heartbeat.missable.max";
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

    // cluster load balance properties
    public static final String LOAD_BALANCE_HOST = "nifi.cluster.load.balance.host";
    public static final String LOAD_BALANCE_PORT = "nifi.cluster.load.balance.port";
    public static final String LOAD_BALANCE_CONNECTIONS_PER_NODE = "nifi.cluster.load.balance.connections.per.node";
    public static final String LOAD_BALANCE_MAX_THREAD_COUNT = "nifi.cluster.load.balance.max.thread.count";
    public static final String LOAD_BALANCE_COMMS_TIMEOUT = "nifi.cluster.load.balance.comms.timeout";

    // zookeeper properties
    public static final String ZOOKEEPER_CONNECT_STRING = "nifi.zookeeper.connect.string";
    public static final String ZOOKEEPER_CONNECT_TIMEOUT = "nifi.zookeeper.connect.timeout";
    public static final String ZOOKEEPER_SESSION_TIMEOUT = "nifi.zookeeper.session.timeout";
    public static final String ZOOKEEPER_ROOT_NODE = "nifi.zookeeper.root.node";
    public static final String ZOOKEEPER_CLIENT_SECURE = "nifi.zookeeper.client.secure";
    public static final String ZOOKEEPER_SECURITY_KEYSTORE = "nifi.zookeeper.security.keystore";
    public static final String ZOOKEEPER_SECURITY_KEYSTORE_TYPE = "nifi.zookeeper.security.keystoreType";
    public static final String ZOOKEEPER_SECURITY_KEYSTORE_PASSWD = "nifi.zookeeper.security.keystorePasswd";
    public static final String ZOOKEEPER_SECURITY_TRUSTSTORE = "nifi.zookeeper.security.truststore";
    public static final String ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE = "nifi.zookeeper.security.truststoreType";
    public static final String ZOOKEEPER_SECURITY_TRUSTSTORE_PASSWD = "nifi.zookeeper.security.truststorePasswd";
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

    // analytics properties
    public static final String ANALYTICS_PREDICTION_ENABLED = "nifi.analytics.predict.enabled";
    public static final String ANALYTICS_PREDICTION_INTERVAL = "nifi.analytics.predict.interval";
    public static final String ANALYTICS_QUERY_INTERVAL = "nifi.analytics.query.interval";
    public static final String ANALYTICS_CONNECTION_MODEL_IMPLEMENTATION = "nifi.analytics.connection.model.implementation";
    public static final String ANALYTICS_CONNECTION_MODEL_SCORE_NAME = "nifi.analytics.connection.model.score.name";
    public static final String ANALYTICS_CONNECTION_MODEL_SCORE_THRESHOLD = "nifi.analytics.connection.model.score.threshold";

    // runtime monitoring properties
    public static final String MONITOR_LONG_RUNNING_TASK_SCHEDULE = "nifi.monitor.long.running.task.schedule";
    public static final String MONITOR_LONG_RUNNING_TASK_THRESHOLD = "nifi.monitor.long.running.task.threshold";

    // defaults
    public static final Boolean DEFAULT_AUTO_RESUME_STATE = true;
    public static final String DEFAULT_AUTHORIZER_CONFIGURATION_FILE = "conf/authorizers.xml";
    public static final String DEFAULT_LOGIN_IDENTITY_PROVIDER_CONFIGURATION_FILE = "conf/login-identity-providers.xml";
    public static final Integer DEFAULT_REMOTE_INPUT_PORT = null;
    public static final Path DEFAULT_TEMPLATE_DIRECTORY = Paths.get("conf", "templates");
    public static final int DEFAULT_WEB_THREADS = 200;
    public static final String DEFAULT_WEB_MAX_HEADER_SIZE = "16 KB";
    public static final String DEFAULT_WEB_WORKING_DIR = "./work/jetty";
    public static final String DEFAULT_WEB_MAX_CONTENT_SIZE = "20 MB";
    public static final String DEFAULT_WEB_MAX_REQUESTS_PER_SECOND = "30000";
    public static final String DEFAULT_WEB_REQUEST_TIMEOUT = "60 secs";
    public static final String DEFAULT_NAR_WORKING_DIR = "./work/nar";
    public static final String DEFAULT_COMPONENT_DOCS_DIRECTORY = "./work/docs/components";
    public static final String DEFAULT_NAR_LIBRARY_DIR = "./lib";
    public static final String DEFAULT_NAR_LIBRARY_AUTOLOAD_DIR = "./extensions";
    public static final String DEFAULT_FLOWFILE_CHECKPOINT_INTERVAL = "20 secs";
    public static final int DEFAULT_MAX_FLOWFILES_PER_CLAIM = 100;
    public static final String DEFAULT_MAX_APPENDABLE_CLAIM_SIZE = "1 MB";
    public static final int DEFAULT_QUEUE_SWAP_THRESHOLD = 20000;
    public static final long DEFAULT_BACKPRESSURE_COUNT = 10_000L;
    public static final String DEFAULT_BACKPRESSURE_SIZE = "1 GB";
    public static final String DEFAULT_ADMINISTRATIVE_YIELD_DURATION = "30 sec";
    public static final String DEFAULT_COMPONENT_STATUS_SNAPSHOT_FREQUENCY = "5 mins";
    public static final String DEFAULT_BORED_YIELD_DURATION = "10 millis";
    public static final String DEFAULT_ZOOKEEPER_CONNECT_TIMEOUT = "3 secs";
    public static final String DEFAULT_ZOOKEEPER_SESSION_TIMEOUT = "3 secs";
    public static final String DEFAULT_ZOOKEEPER_ROOT_NODE = "/nifi";
    public static final boolean DEFAULT_ZOOKEEPER_CLIENT_SECURE = false;
    public static final String DEFAULT_ZOOKEEPER_AUTH_TYPE = "default";
    public static final String DEFAULT_ZOOKEEPER_KERBEROS_REMOVE_HOST_FROM_PRINCIPAL = "true";
    public static final String DEFAULT_ZOOKEEPER_KERBEROS_REMOVE_REALM_FROM_PRINCIPAL = "true";
    public static final String DEFAULT_SECURITY_AUTO_RELOAD_INTERVAL = "10 secs";
    public static final String DEFAULT_SITE_TO_SITE_HTTP_TRANSACTION_TTL = "30 secs";
    public static final String DEFAULT_FLOW_CONFIGURATION_ARCHIVE_ENABLED = "true";
    public static final String DEFAULT_FLOW_CONFIGURATION_ARCHIVE_MAX_TIME = "30 days";
    public static final String DEFAULT_FLOW_CONFIGURATION_ARCHIVE_MAX_STORAGE = "500 MB";
    public static final String DEFAULT_SECURITY_USER_OIDC_CONNECT_TIMEOUT = "5 secs";
    public static final String DEFAULT_SECURITY_USER_OIDC_READ_TIMEOUT = "5 secs";
    public static final String DEFAULT_SECURITY_USER_SAML_METADATA_SIGNING_ENABLED = "false";
    public static final String DEFAULT_SECURITY_USER_SAML_REQUEST_SIGNING_ENABLED = "false";
    public static final String DEFAULT_SECURITY_USER_SAML_WANT_ASSERTIONS_SIGNED = "true";
    public static final String DEFAULT_SECURITY_USER_SAML_SIGNATURE_ALGORITHM = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256";
    public static final String DEFAULT_SECURITY_USER_SAML_DIGEST_ALGORITHM = "http://www.w3.org/2001/04/xmlenc#sha256";
    public static final String DEFAULT_SECURITY_USER_SAML_MESSAGE_LOGGING_ENABLED = "false";
    public static final String DEFAULT_SECURITY_USER_SAML_AUTHENTICATION_EXPIRATION = "12 hours";
    public static final String DEFAULT_SECURITY_USER_SAML_SINGLE_LOGOUT_ENABLED = "false";
    public static final String DEFAULT_SECURITY_USER_SAML_HTTP_CLIENT_TRUSTSTORE_STRATEGY = "JDK";
    public static final String DEFAULT_SECURITY_USER_SAML_HTTP_CLIENT_CONNECT_TIMEOUT = "30 secs";
    public static final String DEFAULT_SECURITY_USER_SAML_HTTP_CLIENT_READ_TIMEOUT = "30 secs";
    public static final String DEFAULT_WEB_SHOULD_SEND_SERVER_VERSION = "true";

    // cluster common defaults
    public static final String DEFAULT_CLUSTER_PROTOCOL_HEARTBEAT_INTERVAL = "5 sec";
    public static final int DEFAULT_CLUSTER_PROTOCOL_HEARTBEAT_MISSABLE_MAX = 8;
    public static final String DEFAULT_CLUSTER_NODE_READ_TIMEOUT = "5 sec";
    public static final String DEFAULT_CLUSTER_NODE_CONNECTION_TIMEOUT = "5 sec";
    public static final int DEFAULT_CLUSTER_NODE_MAX_CONCURRENT_REQUESTS = 100;

    // cluster node defaults
    public static final int DEFAULT_CLUSTER_NODE_PROTOCOL_THREADS = 10;
    public static final int DEFAULT_CLUSTER_NODE_PROTOCOL_MAX_THREADS = 50;
    public static final String DEFAULT_FLOW_ELECTION_MAX_WAIT_TIME = "5 mins";

    // cluster load balance defaults
    public static final int DEFAULT_LOAD_BALANCE_PORT = 6342;
    public static final int DEFAULT_LOAD_BALANCE_CONNECTIONS_PER_NODE = 4;
    public static final int DEFAULT_LOAD_BALANCE_MAX_THREAD_COUNT = 8;
    public static final String DEFAULT_LOAD_BALANCE_COMMS_TIMEOUT = "30 sec";


    // state management defaults
    public static final String DEFAULT_STATE_MANAGEMENT_CONFIG_FILE = "conf/state-management.xml";

    // Kerberos defaults
    public static final String DEFAULT_KERBEROS_AUTHENTICATION_EXPIRATION = "12 hours";

    // analytics defaults
    public static final String DEFAULT_ANALYTICS_PREDICTION_ENABLED = "false";
    public static final String DEFAULT_ANALYTICS_PREDICTION_INTERVAL = "3 mins";
    public static final String DEFAULT_ANALYTICS_QUERY_INTERVAL = "3 mins";
    public final static String DEFAULT_ANALYTICS_CONNECTION_MODEL_IMPLEMENTATION = "org.apache.nifi.controller.status.analytics.models.OrdinaryLeastSquares";
    public static final String DEFAULT_ANALYTICS_CONNECTION_SCORE_NAME = "rSquared";
    public static final double DEFAULT_ANALYTICS_CONNECTION_SCORE_THRESHOLD = .90;

    // Status repository defaults
    public static final int DEFAULT_COMPONENT_STATUS_REPOSITORY_PERSIST_NODE_DAYS = 14;
    public static final int DEFAULT_COMPONENT_STATUS_REPOSITORY_PERSIST_COMPONENT_DAYS = 3;
    public static final String DEFAULT_COMPONENT_STATUS_REPOSITORY_PERSIST_LOCATION = "./status_repository";

    public NiFiProperties() {
        this(Collections.EMPTY_MAP);
    }

    public NiFiProperties(final Map<String, String> props) {
        super(props);
    }

    public NiFiProperties(final Properties props) {
        super(props);
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
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value.trim());
        } catch (final Exception e) {
            return defaultValue;
        }
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
     * Returns the number of milliseconds between FlowFileRepository
     * checkpointing
     *
     * @return the number of milliseconds between checkpoint events
     */
    public String getFlowFileRepositoryCheckpointInterval() {
        return getProperty(FLOWFILE_REPOSITORY_CHECKPOINT_INTERVAL, DEFAULT_FLOWFILE_CHECKPOINT_INTERVAL);
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

    public boolean isHTTPSConfigured() {
        return getSslPort() != null;
    }

    /**
     * Determines the HTTP/HTTPS port NiFi is configured to bind to. Prefers the HTTPS port. Throws an exception if neither is configured.
     *
     * @return the configured port number
     */
    public Integer getConfiguredHttpOrHttpsPort() throws RuntimeException {
        if (getSslPort() != null) {
            return getSslPort();
        } else if (getPort() != null) {
            return getPort();
        } else {
            throw new RuntimeException("The HTTP or HTTPS port must be configured");
        }
    }

    public String getWebMaxHeaderSize() {
        return getProperty(WEB_MAX_HEADER_SIZE, DEFAULT_WEB_MAX_HEADER_SIZE);
    }

    /**
     * Returns the {@code nifi.web.max.content.size} value from {@code nifi.properties}.
     * Does not provide a default value because the presence of any value here enables the
     * {@code ContentLengthFilter}.
     *
     * @return the specified max content-length and units for an incoming HTTP request
     */
    public String getWebMaxContentSize() {
        return getProperty(WEB_MAX_CONTENT_SIZE);
    }

    public String getMaxWebRequestsPerSecond() {
        return getProperty(WEB_MAX_REQUESTS_PER_SECOND, DEFAULT_WEB_MAX_REQUESTS_PER_SECOND);
    }

    public String getWebRequestTimeout() {
        return getProperty(WEB_REQUEST_TIMEOUT, DEFAULT_WEB_REQUEST_TIMEOUT);
    }

    public String getWebRequestIpWhitelist() {
        return getProperty(WEB_REQUEST_IP_WHITELIST);
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
                    || NAR_LIBRARY_DIRECTORY.equals(propertyName)
                    || NAR_LIBRARY_AUTOLOAD_DIRECTORY.equals(propertyName)) {
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

    public File getNarAutoLoadDirectory() {
        return new File(getProperty(NAR_LIBRARY_AUTOLOAD_DIRECTORY, DEFAULT_NAR_LIBRARY_AUTOLOAD_DIR));
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

    /**
     * Returns true if auto reload of the keystore and truststore is enabled.
     * @return true if auto reload of the keystore and truststore is enabled.
     */
    public boolean isSecurityAutoReloadEnabled() {
        return this.getProperty(SECURITY_AUTO_RELOAD_ENABLED, Boolean.FALSE.toString()).equals(Boolean.TRUE.toString());
    }

    /**
     * Returns the auto reload interval of the keystore and truststore.
     * @return The interval over which the keystore and truststore should auto-reload.
     */
    public String getSecurityAutoReloadInterval() {
        return getProperty(SECURITY_AUTO_RELOAD_INTERVAL, DEFAULT_SECURITY_AUTO_RELOAD_INTERVAL);
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
            if (socketPort == 0) {
                throw new RuntimeException("Cluster Node Protocol port cannot be 0. Port must be inclusively in the range [1, 65535].");
            }
            return InetSocketAddress.createUnresolved(socketAddress, socketPort);
        } catch (Exception ex) {
            throw new RuntimeException("Invalid node protocol address/port due to: " + ex, ex);
        }
    }

    public InetSocketAddress getClusterLoadBalanceAddress() {
        try {
            String host = getProperty(LOAD_BALANCE_HOST);
            if (StringUtils.isBlank(host)) {
                host = getProperty(CLUSTER_NODE_ADDRESS);
            }
            if (StringUtils.isBlank(host)) {
                host = "localhost";
            }

            final int port = getIntegerProperty(LOAD_BALANCE_PORT, DEFAULT_LOAD_BALANCE_PORT);
            return InetSocketAddress.createUnresolved(host, port);
        } catch (final Exception e) {
            throw new RuntimeException("Invalid load balance address/port due to: " + e, e);
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
     * @return True if property value is 'true'; False otherwise.
     */
    public Boolean isAnonymousAuthenticationAllowed() {
        final String anonymousAuthenticationAllowed = getProperty(SECURITY_ANONYMOUS_AUTHENTICATION, "false");

        return "true".equalsIgnoreCase(anonymousAuthenticationAllowed);
    }

    /**
     * Returns whether an OpenId Connect (OIDC) URL is set.
     *
     * @return whether an OpenId Connect URL is set
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
     * Returns additional scopes to be sent when requesting the access token from the IDP.
     *
     * @return List of additional scopes to be sent
     */
    public List<String> getOidcAdditionalScopes() {
        String rawProperty = getProperty(SECURITY_USER_OIDC_ADDITIONAL_SCOPES, "");
        if (rawProperty.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> additionalScopes = Arrays.asList(rawProperty.split(","));
        return additionalScopes.stream().map(String::trim).collect(Collectors.toList());
    }

    /**
     * Returns the claim to be used to identify a user.
     * Claim must be requested by adding the scope for it.
     * Default is 'email'.
     *
     * @return The claim to be used to identify the user.
     */
    public String getOidcClaimIdentifyingUser() {
        return getProperty(SECURITY_USER_OIDC_CLAIM_IDENTIFYING_USER, "email").trim();
    }

    /**
     * Returns the list of fallback claims to be used to identify a user when the configured claim is empty for a user
     *
     * @return The list of fallback claims to be used to identify the user
     */
    public List<String> getOidcFallbackClaimsIdentifyingUser() {
        String rawProperty = getProperty(SECURITY_USER_OIDC_FALLBACK_CLAIMS_IDENTIFYING_USER, "").trim();
        if (StringUtils.isBlank(rawProperty)) {
            return Collections.emptyList();
        } else {
            List<String> fallbackClaims = Arrays.asList(rawProperty.split(","));
            return fallbackClaims.stream().map(String::trim).filter(s->!s.isEmpty()).collect(Collectors.toList());
        }
    }

    public boolean shouldSendServerVersion() {
        return Boolean.parseBoolean(getProperty(WEB_SHOULD_SEND_SERVER_VERSION, DEFAULT_WEB_SHOULD_SEND_SERVER_VERSION));
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
     * Returns whether SAML is enabled.
     *
     * @return whether saml is enabled
     */
    public boolean isSamlEnabled() {
        return !StringUtils.isBlank(getSamlIdentityProviderMetadataUrl());
    }

    /**
     * The URL to obtain the identity provider metadata.
     * Must be a value starting with 'file://' or 'http://'.
     *
     * @return the url to obtain the identity provider metadata
     */
    public String getSamlIdentityProviderMetadataUrl() {
        return getProperty(SECURITY_USER_SAML_IDP_METADATA_URL);
    }

    /**
     * The entity id for the service provider.
     *
     * @return the service provider entity id
     */
    public String getSamlServiceProviderEntityId() {
        return getProperty(SECURITY_USER_SAML_SP_ENTITY_ID);
    }

    /**
     * The name of an attribute in the SAML assertions that contains the user identity.
     *
     * If not specified, or missing, the NameID of the Subject will be used.
     *
     * @return the attribute name containing the user identity
     */
    public String getSamlIdentityAttributeName() {
        return getProperty(SECURITY_USER_SAML_IDENTITY_ATTRIBUTE_NAME);
    }

    /**
     * The name of the attribute in the SAML assertions that contains the groups the user belongs to.
     *
     * @return the attribute name containing user groups
     */
    public String getSamlGroupAttributeName() {
        return getProperty(SECURITY_USER_SAML_GROUP_ATTRIBUTE_NAME);
    }

    /**
     * The signing algorithm to use for signing SAML requests.
     *
     * @return the signing algorithm to use
     */
    public String getSamlSignatureAlgorithm() {
        return getProperty(SECURITY_USER_SAML_SIGNATURE_ALGORITHM, DEFAULT_SECURITY_USER_SAML_SIGNATURE_ALGORITHM);
    }

    /**
     * The digest algorithm to use for signing SAML requests.
     *
     * @return the digest algorithm
     */
    public String getSamlSignatureDigestAlgorithm() {
        return getProperty(SECURITY_USER_SAML_SIGNATURE_DIGEST_ALGORITHM, DEFAULT_SECURITY_USER_SAML_DIGEST_ALGORITHM);
    }

    /**
     * Whether or not to sign the service provider metadata.
     *
     * @return whether or not to sign the service provider metadata
     */
    public boolean isSamlMetadataSigningEnabled() {
        return Boolean.parseBoolean(getProperty(SECURITY_USER_SAML_METADATA_SIGNING_ENABLED, DEFAULT_SECURITY_USER_SAML_METADATA_SIGNING_ENABLED));
    }

    /**
     * Whether or not to sign requests sent to the identity provider.
     *
     * @return whether or not to sign requests sent to the identity provider
     */
    public boolean isSamlRequestSigningEnabled() {
        return Boolean.parseBoolean(getProperty(SECURITY_USER_SAML_REQUEST_SIGNING_ENABLED, DEFAULT_SECURITY_USER_SAML_REQUEST_SIGNING_ENABLED));
    }

    /**
     * Whether or not the identity provider should sign assertions when sending response back.
     *
     * @return whether or not the identity provider should sign assertions when sending response back
     */
    public boolean isSamlWantAssertionsSigned() {
        return Boolean.parseBoolean(getProperty(SECURITY_USER_SAML_WANT_ASSERTIONS_SIGNED, DEFAULT_SECURITY_USER_SAML_WANT_ASSERTIONS_SIGNED));
    }

    /**
     * Whether or not to log messages for debug purposes.
     *
     * @return whether or not to log messages
     */
    public boolean isSamlMessageLoggingEnabled() {
        return Boolean.parseBoolean(getProperty(SECURITY_USER_SAML_MESSAGE_LOGGING_ENABLED, DEFAULT_SECURITY_USER_SAML_MESSAGE_LOGGING_ENABLED));
    }

    /**
     * The expiration value for a JWT created from a SAML authentication.
     *
     * @return the expiration value for a SAML authentication
     */
    public String getSamlAuthenticationExpiration() {
        return getProperty(SECURITY_USER_SAML_AUTHENTICATION_EXPIRATION, DEFAULT_SECURITY_USER_SAML_AUTHENTICATION_EXPIRATION);
    }

    /**
     * Whether or not logging out of NiFi should logout of the SAML IDP using the SAML SingleLogoutService.
     *
     * @return whether or not SAML single logout is enabled
     */
    public boolean isSamlSingleLogoutEnabled() {
        return Boolean.parseBoolean(getProperty(SECURITY_USER_SAML_SINGLE_LOGOUT_ENABLED, DEFAULT_SECURITY_USER_SAML_SINGLE_LOGOUT_ENABLED));
    }

    /**
     * The truststore to use when interacting with a SAML IDP over https. Valid values are "JDK" and "NIFI".
     *
     * @return the type of truststore to use
     */
    public String getSamlHttpClientTruststoreStrategy() {
        return getProperty(SECURITY_USER_SAML_HTTP_CLIENT_TRUSTSTORE_STRATEGY, DEFAULT_SECURITY_USER_SAML_HTTP_CLIENT_TRUSTSTORE_STRATEGY);
    }

    /**
     * The connect timeout for the http client created for SAML operations.
     *
     * @return the connect timeout
     */
    public String getSamlHttpClientConnectTimeout() {
        return getProperty(SECURITY_USER_SAML_HTTP_CLIENT_CONNECT_TIMEOUT, DEFAULT_SECURITY_USER_SAML_HTTP_CLIENT_CONNECT_TIMEOUT);
    }

    /**
     * The read timeout for the http client created for SAML operations.
     *
     * @return the read timeout
     */
    public String getSamlHttpClientReadTimeout() {
        return getProperty(SECURITY_USER_SAML_HTTP_CLIENT_READ_TIMEOUT, DEFAULT_SECURITY_USER_SAML_HTTP_CLIENT_READ_TIMEOUT);
    }

    /**
     * Returns true if client certificates are required for REST API. Determined
     * if the following conditions are all true:
     * <p>
     * - login identity provider is not populated
     * - Kerberos service support is not enabled
     * - openid connect is not enabled
     * - knox sso is not enabled
     * - anonymous authentication is not enabled
     * </p>
     *
     * @return true if client certificates are required for access to the REST API
     */
    public boolean isClientAuthRequiredForRestApi() {
        return !isLoginIdentityProviderEnabled()
                && !isKerberosSpnegoSupportEnabled()
                && !isOidcEnabled()
                && !isKnoxSsoEnabled()
                && !isSamlEnabled()
                && !isAnonymousAuthenticationAllowed();
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
     * <p>
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
     * <p>
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

            final String[] vrPropertiesFileList = vrPropertiesFiles.split(",");

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

    /**
     * @return True if property value is 'true'; False if property value is 'false'. Throws an exception otherwise.
     */
    public boolean isZooKeeperClientSecure() {
        final String defaultValue = String.valueOf(DEFAULT_ZOOKEEPER_CLIENT_SECURE);
        final String clientSecure = getProperty(ZOOKEEPER_CLIENT_SECURE, defaultValue).trim();

        if (!"true".equalsIgnoreCase(clientSecure) && !"false".equalsIgnoreCase(clientSecure)) {
            throw new RuntimeException(String.format("%s was '%s', expected true or false", NiFiProperties.ZOOKEEPER_CLIENT_SECURE, clientSecure));
        }

        return Boolean.parseBoolean(clientSecure);
    }

    public boolean isZooKeeperTlsConfigurationPresent() {
        return StringUtils.isNotBlank(getProperty(NiFiProperties.ZOOKEEPER_CLIENT_SECURE))
            && StringUtils.isNotBlank(getProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE))
            && getProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_PASSWD) != null
            && StringUtils.isNotBlank(getProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE))
            && getProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_PASSWD) != null;
    }

    public boolean isTlsConfigurationPresent() {
        return StringUtils.isNotBlank(getProperty(SECURITY_KEYSTORE))
            && getProperty(SECURITY_KEYSTORE_PASSWD) != null
            && StringUtils.isNotBlank(getProperty(SECURITY_TRUSTSTORE))
            && getProperty(SECURITY_TRUSTSTORE_PASSWD) != null;
    }

    public String getFlowFileRepoEncryptionKeyId() {
        return getProperty(FLOWFILE_REPOSITORY_ENCRYPTION_KEY_ID);
    }

    /**
     * Returns the active flowfile repository encryption key if a {@code StaticKeyProvider} is in use.
     * If no key ID is specified in the properties file, the default
     * {@code nifi.flowfile.repository.encryption.key} value is returned. If a key ID is specified in
     * {@code nifi.flowfile.repository.encryption.key.id}, it will attempt to read from
     * {@code nifi.flowfile.repository.encryption.key.id.XYZ} where {@code XYZ} is the provided key
     * ID. If that value is empty, it will use the default property
     * {@code nifi.flowfile.repository.encryption.key}.
     *
     * @return the flowfile repository encryption key in hex form
     */
    public String getFlowFileRepoEncryptionKey() {
        String keyId = getFlowFileRepoEncryptionKeyId();
        String keyKey = StringUtils.isBlank(keyId) ? FLOWFILE_REPOSITORY_ENCRYPTION_KEY : FLOWFILE_REPOSITORY_ENCRYPTION_KEY + ".id." + keyId;
        return getProperty(keyKey, getProperty(FLOWFILE_REPOSITORY_ENCRYPTION_KEY));
    }

    /**
     * Returns a map of keyId -> key in hex loaded from the {@code nifi.properties} file if a
     * {@code StaticKeyProvider} is defined. If {@code FileBasedKeyProvider} is defined this method will return an empty map.
     *
     * @return a Map of the keys identified by key ID
     */
    public Map<String, String> getFlowFileRepoEncryptionKeys() {
        return getRepositoryEncryptionKeys("flowfile");
    }

    /**
     * Returns the map of key IDs to keys retrieved from the properties for the given repository type.
     *
     * @param repositoryType "provenance", "content", or "flowfile"
     * @return the key map
     */
    private Map<String, String> getRepositoryEncryptionKeys(String repositoryType) {
        Map<String, String> keys = new HashMap<>();
        List<String> keyProperties = getRepositoryEncryptionKeyProperties(repositoryType);
        if (keyProperties.size() == 0) {
            logger.warn("No " + repositoryType + " repository encryption key properties were available. Check the "
                    + "exact format specified in the Admin Guide - Encrypted " + StringUtils.toTitleCase(repositoryType)
                    + " Repository Properties");
            return keys;
        }
        final String REPOSITORY_ENCRYPTION_KEY = getRepositoryEncryptionKey(repositoryType);
        final String REPOSITORY_ENCRYPTION_KEY_ID = getRepositoryEncryptionKeyId(repositoryType);

        // Retrieve the actual key values and store non-empty values in the map
        for (String prop : keyProperties) {
            logger.debug("Parsing " + prop);
            final String value = getProperty(prop);
            if (!StringUtils.isBlank(value)) {
                // If this property is .key (the actual hex key), put it in the map under the value of .key.id (i.e. key1)
                if (prop.equalsIgnoreCase(REPOSITORY_ENCRYPTION_KEY)) {
                    keys.put(getProperty(REPOSITORY_ENCRYPTION_KEY_ID), value);
                } else {
                    // Extract nifi.*.repository.encryption.key.id.key1 -> key1
                    String extractedKeyId = prop.substring(prop.lastIndexOf(".") + 1);
                    if (keys.containsKey(extractedKeyId)) {
                        logger.warn("The {} repository encryption key map already contains an entry for {}. Ignoring new value from {}", repositoryType, extractedKeyId, prop);
                    } else {
                        keys.put(extractedKeyId, value);
                    }
                }
            }
        }
        return keys;
    }

    /**
     * Returns the list of encryption key properties for the specified repository type. If an unknown repository type
     * is provided, returns an empty list.
     *
     * @param repositoryType "provenance", "content", or "flowfile"
     * @return the list of encryption key properties
     */
    private List<String> getRepositoryEncryptionKeyProperties(String repositoryType) {
        switch (repositoryType.toLowerCase()) {
            case "flowfile":
                return getFlowFileRepositoryEncryptionKeyProperties();
            case "content":
                return getContentRepositoryEncryptionKeyProperties();
            case "provenance":
                return getProvenanceRepositoryEncryptionKeyProperties();
            default:
                return Collections.emptyList();
        }
    }

    /**
     * Returns the encryption key property key for the specified repository type. If an unknown repository type
     * is provided, returns an empty string.
     *
     * @param repositoryType "provenance", "content", or "flowfile"
     * @return the encryption key property (i.e. {@code FLOWFILE_REPOSITORY_ENCRYPTION_KEY})
     */
    private String getRepositoryEncryptionKey(String repositoryType) {
        switch (repositoryType.toLowerCase()) {
            case "flowfile":
                return FLOWFILE_REPOSITORY_ENCRYPTION_KEY;
            case "content":
                return CONTENT_REPOSITORY_ENCRYPTION_KEY;
            case "provenance":
                return PROVENANCE_REPO_ENCRYPTION_KEY;
            default:
                return "";
        }
    }

    /**
     * Returns the encryption key ID property key for the specified repository type. If an unknown repository type
     * is provided, returns an empty string.
     *
     * @param repositoryType "provenance", "content", or "flowfile"
     * @return the encryption key ID property (i.e. {@code FLOWFILE_REPOSITORY_ENCRYPTION_KEY_ID})
     */
    private String getRepositoryEncryptionKeyId(String repositoryType) {
        switch (repositoryType.toLowerCase()) {
            case "flowfile":
                return FLOWFILE_REPOSITORY_ENCRYPTION_KEY_ID;
            case "content":
                return CONTENT_REPOSITORY_ENCRYPTION_KEY_ID;
            case "provenance":
                return PROVENANCE_REPO_ENCRYPTION_KEY_ID;
            default:
                return "";
        }
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
     * {@code StaticKeyProvider} is defined. If {@code FileBasedKeyProvider} is defined this method will return an empty map.
     *
     * @return a Map of the keys identified by key ID
     */
    public Map<String, String> getProvenanceRepoEncryptionKeys() {
        return getRepositoryEncryptionKeys("provenance");
    }

    public String getContentRepositoryEncryptionKeyId() {
        return getProperty(CONTENT_REPOSITORY_ENCRYPTION_KEY_ID);
    }

    /**
     * Returns the active content repository encryption key if a {@code StaticKeyProvider} is in use.
     * If no key ID is specified in the properties file, the default
     * {@code nifi.content.repository.encryption.key} value is returned. If a key ID is specified in
     * {@code nifi.content.repository.encryption.key.id}, it will attempt to read from
     * {@code nifi.content.repository.encryption.key.id.XYZ} where {@code XYZ} is the provided key
     * ID. If that value is empty, it will use the default property
     * {@code nifi.content.repository.encryption.key}.
     *
     * @return the content repository encryption key in hex form
     */
    public String getContentRepositoryEncryptionKey() {
        String keyId = getContentRepositoryEncryptionKeyId();
        String keyKey = StringUtils.isBlank(keyId) ? CONTENT_REPOSITORY_ENCRYPTION_KEY : CONTENT_REPOSITORY_ENCRYPTION_KEY + ".id." + keyId;
        return getProperty(keyKey, getProperty(CONTENT_REPOSITORY_ENCRYPTION_KEY));
    }

    /**
     * Returns a map of keyId -> key in hex loaded from the {@code nifi.properties} file if a
     * {@code StaticKeyProvider} is defined. If {@code FileBasedKeyProvider} is defined this method will return an empty map.
     *
     * @return a Map of the keys identified by key ID
     */
    public Map<String, String> getContentRepositoryEncryptionKeys() {
        return getRepositoryEncryptionKeys("content");
    }

    /**
     * Returns the allowed proxy hostnames (and IP addresses) as a comma-delimited string.
     * The hosts have been normalized to the form {@code somehost.com}, {@code somehost.com:port}, or {@code 127.0.0.1}.
     * <p>
     * Note: Calling {@code NiFiProperties.getProperty(NiFiProperties.WEB_PROXY_HOST)} will not normalize the hosts.
     *
     * @return the hostname(s)
     */
    public String getAllowedHosts() {
        return StringUtils.join(getAllowedHostsAsList(), ",");
    }

    /**
     * Returns the allowed proxy hostnames (and IP addresses) as a List. The hosts have been normalized to the form {@code somehost.com}, {@code somehost.com:port}, or {@code 127.0.0.1}.
     *
     * @return the hostname(s)
     */
    public List<String> getAllowedHostsAsList() {
        String rawProperty = getProperty(WEB_PROXY_HOST, "");
        List<String> hosts = Arrays.asList(rawProperty.split(","));
        return hosts.stream()
                .map(this::normalizeHost).filter(host -> !StringUtils.isBlank(host)).collect(Collectors.toList());
    }

    String normalizeHost(String host) {
        if (host == null || host.equalsIgnoreCase("")) {
            return "";
        } else {
            return host.trim();
        }
    }

    /**
     * Returns the allowed proxy context paths as a comma-delimited string. The paths have been normalized to the form {@code /some/context/path}.
     * <p>
     * Note: Calling {@code NiFiProperties.getProperty(NiFiProperties.WEB_PROXY_CONTEXT_PATH)} will not normalize the paths.
     *
     * @return the path(s)
     */
    public String getAllowedContextPaths() {
        return StringUtils.join(getAllowedContextPathsAsList(), ",");
    }

    /**
     * Returns the allowed proxy context paths as a list of paths. The paths have been normalized to the form {@code /some/context/path}.
     *
     * @return the path(s)
     */
    public List<String> getAllowedContextPathsAsList() {
        String rawProperty = getProperty(WEB_PROXY_CONTEXT_PATH, "");
        List<String> contextPaths = Arrays.asList(rawProperty.split(","));
        return contextPaths.stream()
                .map(this::normalizeContextPath).collect(Collectors.toList());
    }

    private String normalizeContextPath(String cp) {
        if (cp == null || cp.equalsIgnoreCase("")) {
            return "";
        } else {
            String trimmedCP = cp.trim();
            // Ensure it starts with a leading slash and does not end in a trailing slash
            // There's a potential for the path to be something like bad/path/// but this is semi-trusted data from an admin-accessible file and there are way worse possibilities here
            trimmedCP = trimmedCP.startsWith("/") ? trimmedCP : "/" + trimmedCP;
            trimmedCP = trimmedCP.endsWith("/") ? trimmedCP.substring(0, trimmedCP.length() - 1) : trimmedCP;
            return trimmedCP;
        }
    }

    private List<String> getFlowFileRepositoryEncryptionKeyProperties() {
        // Filter all the property keys that define a key
        return getPropertyKeys().stream().filter(k ->
                k.startsWith(FLOWFILE_REPOSITORY_ENCRYPTION_KEY_ID + ".") || k.equalsIgnoreCase(FLOWFILE_REPOSITORY_ENCRYPTION_KEY)
        ).collect(Collectors.toList());
    }

    private List<String> getProvenanceRepositoryEncryptionKeyProperties() {
        // Filter all the property keys that define a key
        return getPropertyKeys().stream().filter(k ->
                k.startsWith(PROVENANCE_REPO_ENCRYPTION_KEY_ID + ".") || k.equalsIgnoreCase(PROVENANCE_REPO_ENCRYPTION_KEY)
        ).collect(Collectors.toList());
    }

    private List<String> getContentRepositoryEncryptionKeyProperties() {
        // Filter all the property keys that define a key
        return getPropertyKeys().stream().filter(k ->
                k.startsWith(CONTENT_REPOSITORY_ENCRYPTION_KEY_ID + ".") || k.equalsIgnoreCase(CONTENT_REPOSITORY_ENCRYPTION_KEY)
        ).collect(Collectors.toList());
    }

    public Long getDefaultBackPressureObjectThreshold() {
        long backPressureCount;
        try {
            String backPressureCountStr = getProperty(BACKPRESSURE_COUNT);
            if (backPressureCountStr == null || backPressureCountStr.trim().isEmpty()) {
                backPressureCount = DEFAULT_BACKPRESSURE_COUNT;
            } else {
                backPressureCount = Long.parseLong(backPressureCountStr);
            }
        } catch (NumberFormatException nfe) {
            backPressureCount = DEFAULT_BACKPRESSURE_COUNT;
        }
        return backPressureCount;
    }

    public String getDefaultBackPressureDataSizeThreshold() {
        return getProperty(BACKPRESSURE_SIZE, DEFAULT_BACKPRESSURE_SIZE);
    }

    /**
     * Returns the directory where the QuestDB based status repository is expected to work within.
     *
     * @return Path object pointing to the database's folder.
     */
    public Path getQuestDbStatusRepositoryPath() {
        return Paths.get(getProperty(STATUS_REPOSITORY_QUESTDB_PERSIST_LOCATION, DEFAULT_COMPONENT_STATUS_REPOSITORY_PERSIST_LOCATION));
    }

    /**
     * Returns all properties where the property key starts with the prefix.
     *
     * @param prefix The exact string the returned properties should start with. Dots are considered, thus prefix "item" will return both
     *               properties starting with "item." and "items". Properties with empty value will be included as well.
     *
     * @return A map of properties starting with the prefix.
     */
    public Map<String, String> getPropertiesWithPrefix(final String prefix) {
        return getPropertyKeys().stream().filter(key -> key.startsWith(prefix)).collect(Collectors.toMap(key -> key, key -> getProperty(key)));
    }

    /**
     * Returns with all the possible next "tokens" after the given prefix. An alphanumeric string between dots is considered as a "token".
     *
     * For example if there are "parent.sub1" and a "parent.sub2" properties are set, and the prefix is "parent", the method will return
     * with a set, consisting of "sub1" and "sub2. Only directly subsequent tokens are considered, so in case of "parent.sub1.subsub1", the
     * result will contain "sub1" as well.
     *
     * @param prefix The prefix of the request.
     *
     * @return A set of direct subsequent tokens.
     */
    public Set<String> getDirectSubsequentTokens(final String prefix) {
        final String fixedPrefix = prefix.endsWith(".") ? prefix : prefix + ".";

        return getPropertyKeys().stream()
                .filter(key -> key.startsWith(fixedPrefix))
                .map(key -> key.substring(fixedPrefix.length()))
                .map(key -> key.indexOf('.') == -1 ? key : key.substring(0, key.indexOf('.')))
                .collect(Collectors.toSet());
    }

    /**
     * Creates an instance of NiFiProperties. This should likely not be called
     * by any classes outside of the NiFi framework but can be useful by the
     * framework for default property loading behavior or helpful in tests
     * needing to create specific instances of NiFiProperties. If properties
     * file specified cannot be found/read a runtime exception will be thrown.
     * If one is not specified an empty object will be returned.
     *
     * @param propertiesFilePath   if provided properties will be loaded from
     *                             given file; else will be loaded from System property.
     *                             Can be null. Passing {@code ""} skips any attempt to load from the file system.
     * @return NiFiProperties
     */
    public static NiFiProperties createBasicNiFiProperties(final String propertiesFilePath) {
        return createBasicNiFiProperties(propertiesFilePath, new Properties());
    }

    /**
     * Creates an instance of NiFiProperties. This should likely not be called
     * by any classes outside of the NiFi framework but can be useful by the
     * framework for default property loading behavior or helpful in tests
     * needing to create specific instances of NiFiProperties. If properties
     * file specified cannot be found/read a runtime exception will be thrown.
     * If one is not specified, only the provided properties will be returned.
     *
     * @param propertiesFilePath   if provided properties will be loaded from
     *                             given file; else will be loaded from System property.
     *                             Can be null. Passing {@code ""} skips any attempt to load from the file system.
     * @param additionalProperties allows overriding of properties with the
     *                             supplied values. these will be applied after loading from any properties
     *                             file. Can be null or empty.
     * @return NiFiProperties
     */
    public static NiFiProperties createBasicNiFiProperties(final String propertiesFilePath, final Map<String, String> additionalProperties) {
        final Map<String, String> addProps = (additionalProperties == null) ? Collections.EMPTY_MAP : additionalProperties;
        final Properties properties = new Properties();
        addProps.forEach(properties::put);

        return createBasicNiFiProperties(propertiesFilePath, properties);
    }

    /**
     * Creates an instance of NiFiProperties. This should likely not be called
     * by any classes outside of the NiFi framework but can be useful by the
     * framework for default property loading behavior or helpful in tests
     * needing to create specific instances of NiFiProperties. If properties
     * file specified cannot be found/read a runtime exception will be thrown.
     * If one is not specified, only the provided properties will be returned.
     *
     * @param propertiesFilePath   if provided properties will be loaded from
     *                             given file; else will be loaded from System property.
     *                             Can be null. Passing {@code ""} skips any attempt to load from the file system.
     * @param additionalProperties allows overriding of properties with the
     *                             supplied values. these will be applied after loading from any properties
     *                             file. Can be null or empty.
     * @return NiFiProperties
     */
    public static NiFiProperties createBasicNiFiProperties(final String propertiesFilePath, final Properties additionalProperties) {
        final Properties properties = new Properties();

        // If the provided file path is null or provided, load from file. If it is "", skip this
        if (propertiesFilePath == null || StringUtils.isNotBlank(propertiesFilePath)) {
            readFromPropertiesFile(propertiesFilePath, properties);
        }

        // The Properties(Properties) constructor does NOT inherit the provided values, just uses them as default values
        if (additionalProperties != null) {
            additionalProperties.forEach(properties::put);
        }
        return new NiFiProperties() {
            @Override
            public String getProperty(String key) {
                return properties.getProperty(key);
            }

            @Override
            public Set<String> getPropertyKeys() {
                return properties.stringPropertyNames();
            }

            @Override
            public int size() {
                return getPropertyKeys().size();
            }
        };
    }

    private static void readFromPropertiesFile(String propertiesFilePath, Properties properties) {
        final String nfPropertiesFilePath = (propertiesFilePath == null)
                ? System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)
                : propertiesFilePath;
        if (nfPropertiesFilePath != null) {
            final File propertiesFile = new File(nfPropertiesFilePath.trim());
            if (!propertiesFile.exists()) {
                throw new RuntimeException("Properties file doesn't exist '"
                        + propertiesFile.getAbsolutePath() + "'");
            }
            if (!propertiesFile.canRead()) {
                throw new RuntimeException("Properties file exists but cannot be read '"
                        + propertiesFile.getAbsolutePath() + "'");
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

    @Override
    public String toString() {
        return "NiFiProperties instance with " + size() + " properties";
    }
}
