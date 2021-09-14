/**
 * Copyright 2016-2019 Cloudera, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import com.hortonworks.registries.auth.KerberosLogin;
import com.hortonworks.registries.auth.Login;
import com.hortonworks.registries.auth.NOOPLogin;
import com.hortonworks.registries.auth.util.JaasConfiguration;
import com.hortonworks.registries.common.SchemaRegistryServiceInfo;
import com.hortonworks.registries.common.SchemaRegistryVersion;
import com.hortonworks.registries.common.catalog.CatalogResponse;
import com.hortonworks.registries.common.util.ClassLoaderAwareInvocationHandler;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.ConfigEntry;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeResult;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaProviderInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionRetriever;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.cache.SchemaVersionInfoCache;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaBranchDeletionException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.exceptions.RegistryRetryableException;
import com.hortonworks.registries.schemaregistry.retry.RetryExecutor;
import com.hortonworks.registries.schemaregistry.retry.policy.BackoffPolicy;
import com.hortonworks.registries.schemaregistry.retry.policy.NOOPBackoffPolicy;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.serde.SnapshotDeserializer;
import com.hortonworks.registries.schemaregistry.serde.SnapshotSerializer;
import com.hortonworks.registries.schemaregistry.serde.pull.PullDeserializer;
import com.hortonworks.registries.schemaregistry.serde.pull.PullSerializer;
import com.hortonworks.registries.schemaregistry.serde.push.PushDeserializer;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachineInfo;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import com.hortonworks.registries.shaded.org.glassfish.jersey.SslConfigurator;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.ClientConfig;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.ClientProperties;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.JerseyClientBuilder;
import com.hortonworks.registries.shaded.org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import com.hortonworks.registries.shaded.org.glassfish.jersey.media.multipart.BodyPart;
import com.hortonworks.registries.shaded.org.glassfish.jersey.media.multipart.FormDataMultiPart;
import com.hortonworks.registries.shaded.org.glassfish.jersey.media.multipart.MultiPart;
import com.hortonworks.registries.shaded.org.glassfish.jersey.media.multipart.MultiPartFeature;
import com.hortonworks.registries.shaded.org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.security.auth.login.LoginException;
import com.hortonworks.registries.shaded.javax.ws.rs.BadRequestException;
import com.hortonworks.registries.shaded.javax.ws.rs.NotFoundException;
import com.hortonworks.registries.shaded.javax.ws.rs.ProcessingException;
import com.hortonworks.registries.shaded.javax.ws.rs.client.Client;
import com.hortonworks.registries.shaded.javax.ws.rs.client.ClientBuilder;
import com.hortonworks.registries.shaded.javax.ws.rs.client.Entity;
import com.hortonworks.registries.shaded.javax.ws.rs.client.WebTarget;
import com.hortonworks.registries.shaded.javax.ws.rs.core.MediaType;
import com.hortonworks.registries.shaded.javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.DEFAULT_CONNECTION_TIMEOUT;
import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.DEFAULT_READ_TIMEOUT;
import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL;

/**
 * NOTE: This class is a copy of the SchemaRegistryClient from https://github.com/hortonworks/registry.
 * The changes to this file are the following:
 *      - Making the 'private Login login' become protected for access by sub-classes
 *      - Making the 'private Configuration configuration' become protected for access by sub-classes
 *
 * This is the default implementation of {@link ISchemaRegistryClient} which connects to the given {@code rootCatalogURL}.
 * <p>
 * An instance of SchemaRegistryClient can be instantiated by passing configuration properties like below.
 * <pre>
 *     SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(config);
 * </pre>
 * <p>
 * There are different options available as mentioned in {@link Configuration} like
 * <pre>
 * - {@link Configuration#SCHEMA_REGISTRY_URL}.
 * - {@link Configuration#SCHEMA_METADATA_CACHE_SIZE}.
 * - {@link Configuration#SCHEMA_METADATA_CACHE_EXPIRY_INTERVAL_SECS}.
 * - {@link Configuration#SCHEMA_VERSION_CACHE_SIZE}.
 * - {@link Configuration#SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS}.
 * - {@link Configuration#SCHEMA_TEXT_CACHE_SIZE}.
 * - {@link Configuration#SCHEMA_TEXT_CACHE_EXPIRY_INTERVAL_SECS}.
 *
 * and many other properties like {@link ClientProperties}
 * </pre>
 * <pre>
 * This can be used to
 *      - register schema metadata
 *      - add new versions of a schema
 *      - fetch different versions of schema
 *      - fetch latest version of a schema
 *      - check whether the given schema text is compatible with a latest version of the schema
 *      - register serializer/deserializer for a schema
 *      - fetch serializer/deserializer for a schema
 * </pre>
 */
public class SchemaRegistryClient implements ISchemaRegistryClient {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryClient.class);

    private static final String SCHEMA_REGISTRY_PATH = "/schemaregistry";
    private static final String SCHEMAS_PATH = SCHEMA_REGISTRY_PATH + "/schemas/";
    private static final String SCHEMA_PROVIDERS_PATH = SCHEMA_REGISTRY_PATH + "/schemaproviders/";
    private static final String SCHEMAS_BY_ID_PATH = SCHEMA_REGISTRY_PATH + "/schemasById/";
    private static final String SCHEMA_VERSIONS_PATH = SCHEMAS_PATH + "versions/";
    private static final String FILES_PATH = SCHEMA_REGISTRY_PATH + "/files/";
    private static final String SERIALIZERS_PATH = SCHEMA_REGISTRY_PATH + "/serdes/";
    private static final String REGISTY_CLIENT_JAAS_SECTION = "RegistryClient";
    private static final Set<Class<?>> DESERIALIZER_INTERFACE_CLASSES = Sets.newHashSet(SnapshotDeserializer.class, PullDeserializer.class,
      PushDeserializer.class);
    private static final Set<Class<?>> SERIALIZER_INTERFACE_CLASSES = Sets.newHashSet(SnapshotSerializer.class, PullSerializer.class);
    private static final String SEARCH_FIELDS = SCHEMA_REGISTRY_PATH + "/search/schemas/fields";
    private static final String FIND_AGGREGATED_SCHEMAS = SCHEMA_REGISTRY_PATH + "/search/schemas/aggregated";
    private static final long KERBEROS_SYNCHRONIZATION_TIMEOUT_MS = 180000;

    private static final String SSL_KEY_PASSWORD = "keyPassword";
    private static final String SSL_KEY_STORE_PATH = "keyStorePath";

    private static final SchemaRegistryVersion CLIENT_VERSION = SchemaRegistryServiceInfo.get().version();

    protected Login login;
    private final Client client;
    private final UrlSelector urlSelector;
    private final Map<String, SchemaRegistryTargets> urlWithTargets;

    protected final Configuration configuration;
    private final ClassLoaderCache classLoaderCache;
    private final SchemaVersionInfoCache schemaVersionInfoCache;
    private final SchemaMetadataCache schemaMetadataCache;
    private final Cache<SchemaDigestEntry, SchemaIdVersion> schemaTextCache;

    private static final String SSL_CONFIGURATION_KEY = "schema.registry.client.ssl";
    private static final String SSL_PROTOCOL_KEY = "schema.registry.client.ssl.protocol";
    private static final String HOSTNAME_VERIFIER_CLASS_KEY = "hostnameVerifierClass";

    private static final String CLIENT_RETRY_POLICY_KEY = "schema.registry.client.retry.policy";
    private static final String RETRY_POLICY_CLASS_NAME_KEY = "className";
    private static final String RETRY_POLICY_CONFIG_KEY = "config";

    private static final String DEFAULT_RETRY_STRATEGY_CLASS = NOOPBackoffPolicy.class.getCanonicalName();
    private final RetryExecutor retryExecutor;

    /**
     * Creates {@link SchemaRegistryClient} instance with the given yaml config.
     *
     * @param confFile config file which contains the configuration entries.
     * @throws IOException when any IOException occurs while reading the given confFile
     */
    public SchemaRegistryClient(File confFile) throws IOException {
        this(buildConfFromFile(confFile));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> buildConfFromFile(File confFile) throws IOException {
        try (FileInputStream fis = new FileInputStream(confFile)) {
            return (Map<String, Object>) new Yaml().load(IOUtils.toString(fis, StandardCharsets.UTF_8));
        }
    }

    public SchemaRegistryClient(Map<String, ?> conf) {
        configuration = new Configuration(conf);
        initializeSecurityContext();   // configure kerberos

        ClientConfig config = createClientConfig(conf);
        ClientBuilder clientBuilder = JerseyClientBuilder.newBuilder()
          .withConfig(config)
          .property(ClientProperties.FOLLOW_REDIRECTS, Boolean.TRUE);

        if (conf.containsKey(SSL_CONFIGURATION_KEY) || conf.containsKey(SSL_PROTOCOL_KEY)) {
            configureClientForSsl(conf, clientBuilder);
        }

        client = clientBuilder.build();
        client.register(MultiPartFeature.class);
        configureClientForBasicAuth(client);

        // get list of urls and create given or default UrlSelector.
        urlSelector = createUrlSelector();
        urlWithTargets = new ConcurrentHashMap<>();

        retryExecutor = createRetryExecutor(conf);

        classLoaderCache = new ClassLoaderCache(this);

        schemaVersionInfoCache = createSchemaVersionInfoCache();

        schemaMetadataCache = createSchemaMetadataCache();

        schemaTextCache = createSchemaTextCache();
    }

    @SuppressWarnings("unchecked")
    private void configureClientForSsl(Map<String, ?> conf, ClientBuilder clientBuilder) {
        Map<String, String> sslConfigurations = (Map<String, String>) conf.get(SSL_CONFIGURATION_KEY);

        if (sslConfigurations == null) {
            sslConfigurations = conf.entrySet().stream()
              .filter(entry -> entry.getKey().startsWith(SSL_CONFIGURATION_KEY + "."))
              .collect(Collectors.toMap(entry -> entry.getKey().substring(SSL_CONFIGURATION_KEY.length() + 1),
                entry -> (String) entry.getValue()));
        }

        clientBuilder.sslContext(createSSLContext(sslConfigurations));
        if (sslConfigurations.containsKey(HOSTNAME_VERIFIER_CLASS_KEY)) {
            HostnameVerifier hostNameVerifier = null;
            String hostNameVerifierClassName = sslConfigurations.get(HOSTNAME_VERIFIER_CLASS_KEY);
            try {
                hostNameVerifier = (HostnameVerifier) Class.forName(hostNameVerifierClassName).newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate hostNameVerifierClass : " + hostNameVerifierClassName, e);
            }
            clientBuilder.hostnameVerifier(hostNameVerifier);
        }
    }

    private void configureClientForBasicAuth(Client client) {
        String userName = configuration.getValue(Configuration.AUTH_USERNAME.name());
        String password = configuration.getValue(Configuration.AUTH_PASSWORD.name());
        if (StringUtils.isNotEmpty(userName) && StringUtils.isNotEmpty(password)) {
            HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(userName, password);
            client.register(feature);
        }
    }

    @SuppressWarnings("unchecked")
    private RetryExecutor createRetryExecutor(Map<String, ?> conf) {
        String retryPolicyClass = DEFAULT_RETRY_STRATEGY_CLASS;
        Map<String, Object> retryPolicyProps = new HashMap<>();
        if (conf.containsKey(CLIENT_RETRY_POLICY_KEY)) {
            Map<String, Object> retryStrategyConfigurations = (Map<String, Object>) conf.get(CLIENT_RETRY_POLICY_KEY);
            retryPolicyClass = (String) retryStrategyConfigurations.getOrDefault(RETRY_POLICY_CLASS_NAME_KEY, DEFAULT_RETRY_STRATEGY_CLASS);
            if (retryStrategyConfigurations.containsKey(RETRY_POLICY_CONFIG_KEY)) {
                retryPolicyProps = (Map<String, Object>) retryStrategyConfigurations.get(RETRY_POLICY_CONFIG_KEY);
            }
        }

        BackoffPolicy backoffPolicy = createRetryPolicy(retryPolicyClass, retryPolicyProps);
        return new RetryExecutor.Builder()
          .backoffPolicy(backoffPolicy)
          .retryOnException(RegistryRetryableException.class)
          .build();
    }

    private Cache<SchemaDigestEntry, SchemaIdVersion> createSchemaTextCache() {
        long cacheSize = ((Number) configuration.getValue(Configuration.SCHEMA_TEXT_CACHE_SIZE.name())).longValue();
        long expiryInSecs = ((Number) configuration.getValue(Configuration.SCHEMA_TEXT_CACHE_EXPIRY_INTERVAL_SECS.name())).longValue();

        return CacheBuilder.newBuilder()
          .maximumSize(cacheSize)
          .expireAfterAccess(expiryInSecs, TimeUnit.SECONDS)
          .build();
    }

    private SchemaMetadataCache createSchemaMetadataCache() {
        SchemaMetadataCache.SchemaMetadataFetcher schemaMetadataFetcher = createSchemaMetadataFetcher();
        long cacheSize = ((Number) configuration.getValue(Configuration.SCHEMA_METADATA_CACHE_SIZE.name())).longValue();
        long expiryInSecs = ((Number) configuration.getValue(Configuration.SCHEMA_METADATA_CACHE_EXPIRY_INTERVAL_SECS.name())).longValue();

        return new SchemaMetadataCache(cacheSize, expiryInSecs, schemaMetadataFetcher);
    }

    private SchemaVersionInfoCache createSchemaVersionInfoCache() {
        return new SchemaVersionInfoCache(
          new SchemaVersionRetriever() {
              @Override
              public SchemaVersionInfo retrieveSchemaVersion(SchemaVersionKey key) throws SchemaNotFoundException {
                  return doGetSchemaVersionInfo(key);
              }

              @Override
              public SchemaVersionInfo retrieveSchemaVersion(SchemaIdVersion key) throws SchemaNotFoundException {
                  return doGetSchemaVersionInfo(key);
              }
          },
          ((Number) configuration.getValue(Configuration.SCHEMA_VERSION_CACHE_SIZE.name())).intValue(),
          ((Number) configuration.getValue(Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name())).longValue() * 1000L
        );
    }

    @SuppressWarnings("unchecked")
    private BackoffPolicy createRetryPolicy(String retryPolicyClass, Map<String, Object> retryPolicyProps) {
        ClassLoader classLoader = this.getClass().getClassLoader();
        BackoffPolicy backoffPolicy;
        Class<? extends BackoffPolicy> clazz;
        try {
            clazz = (Class<? extends BackoffPolicy>) Class.forName(retryPolicyClass, true, classLoader);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to initiate the retry policy class : " + retryPolicyClass, e);
        }
        try {
            backoffPolicy = clazz.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Failed to create an instance of retry policy class : " + retryPolicyClass, e);
        }
        backoffPolicy.init(retryPolicyProps);

        return backoffPolicy;
    }

    /** Login with kerberos */
    protected void initializeSecurityContext() {
        this.login = dynamicJaasLogin().orElse(staticJaasLogin());
    }

    /** Attempt to login with dynamic JAAS (from an in-memory string) */
    private Optional<Login> dynamicJaasLogin() {
        String saslJaasConfig = configuration.getValue(Configuration.SASL_JAAS_CONFIG.name());
        if (saslJaasConfig != null) {
            KerberosLogin kerberosLogin = new KerberosLogin(KERBEROS_SYNCHRONIZATION_TIMEOUT_MS);
            try {
                kerberosLogin.configure(new HashMap<>(), REGISTY_CLIENT_JAAS_SECTION,
                  new JaasConfiguration(REGISTY_CLIENT_JAAS_SECTION, saslJaasConfig));
                kerberosLogin.login();
                return Optional.of(kerberosLogin);
            } catch (LoginException e) {
                LOG.error("Failed to initialize the dynamic JAAS config: {}. Attempting static JAAS config.", saslJaasConfig);
            } catch (Exception e) {
                LOG.error("Failed to parse the dynamic JAAS config. Attempting static JAAS config.", e);
            }
        }
        return Optional.empty();
    }

    /** Attempt to login with static JAAS (from a file) */
    private Login staticJaasLogin() {
        String jaasConfigFile = System.getProperty(KerberosLogin.JAAS_CONFIG_SYSTEM_PROPERTY);
        if (jaasConfigFile != null && !jaasConfigFile.trim().isEmpty()) {
            KerberosLogin kerberosLogin = new KerberosLogin(KERBEROS_SYNCHRONIZATION_TIMEOUT_MS);
            kerberosLogin.configure(new HashMap<>(), REGISTY_CLIENT_JAAS_SECTION);
            try {
                kerberosLogin.login();
                return kerberosLogin;
            } catch (LoginException e) {
                LOG.error("Could not login using jaas config section {}", REGISTY_CLIENT_JAAS_SECTION);
                return new NOOPLogin();
            }
        } else {
            LOG.warn("System property for jaas config file is not defined. This is okay if schema registry is not running in secured mode");
            return new NOOPLogin();
        }
    }

    protected SSLContext createSSLContext(Map<String, String> sslConfigurations) {
        SslConfigurator sslConfigurator = SslConfigurator.newInstance();
        if (sslConfigurations.containsKey(SSL_KEY_STORE_PATH)) {
            sslConfigurator.keyStoreType(sslConfigurations.get("keyStoreType"))
              .keyStoreFile(sslConfigurations.get(SSL_KEY_STORE_PATH))
              .keyStorePassword(sslConfigurations.get("keyStorePassword"))
              .keyStoreProvider(sslConfigurations.get("keyStoreProvider"))
              .keyManagerFactoryAlgorithm(sslConfigurations.get("keyManagerFactoryAlgorithm"))
              .keyManagerFactoryProvider(sslConfigurations.get("keyManagerFactoryProvider"));
            if (sslConfigurations.containsKey(SSL_KEY_PASSWORD)) {
                sslConfigurator.keyPassword(sslConfigurations.get(SSL_KEY_PASSWORD));
            }
        }


        sslConfigurator.trustStoreType(sslConfigurations.get("trustStoreType"))
          .trustStoreFile(sslConfigurations.get("trustStorePath"))
          .trustStorePassword(sslConfigurations.get("trustStorePassword"))
          .trustStoreProvider(sslConfigurations.get("trustStoreProvider"))
          .trustManagerFactoryAlgorithm(sslConfigurations.get("trustManagerFactoryAlgorithm"))
          .trustManagerFactoryProvider(sslConfigurations.get("trustManagerFactoryProvider"));

        sslConfigurator.securityProtocol(sslConfigurations.get("protocol"));

        return sslConfigurator.createSSLContext();
    }

    private SchemaRegistryTargets currentSchemaRegistryTargets() {
        String url = urlSelector.select();
        urlWithTargets.computeIfAbsent(url, s -> new SchemaRegistryTargets(client.target(s)));
        return urlWithTargets.get(url);
    }

    private static class SchemaRegistryTargets {
        private final WebTarget schemaProvidersTarget;
        private final WebTarget schemasTarget;
        private final WebTarget schemasByIdTarget;
        private final WebTarget rootTarget;
        private final WebTarget searchFieldsTarget;
        private final WebTarget findAggregatedSchemasTarget;
        private final WebTarget serializersTarget;
        private final WebTarget filesTarget;
        private final WebTarget schemaVersionsTarget;
        private final WebTarget schemaVersionsByIdTarget;
        private final WebTarget schemaVersionsStatesMachineTarget;

        SchemaRegistryTargets(WebTarget rootTarget) {
            this.rootTarget = rootTarget;
            schemaProvidersTarget = rootTarget.path(SCHEMA_PROVIDERS_PATH);
            schemasTarget = rootTarget.path(SCHEMAS_PATH);
            schemasByIdTarget = rootTarget.path(SCHEMAS_BY_ID_PATH);
            schemaVersionsByIdTarget = schemasTarget.path("versionsById");
            schemaVersionsTarget = rootTarget.path(SCHEMA_VERSIONS_PATH);
            schemaVersionsStatesMachineTarget = schemaVersionsTarget.path("statemachine");
            searchFieldsTarget = rootTarget.path(SEARCH_FIELDS);
            findAggregatedSchemasTarget = rootTarget.path(FIND_AGGREGATED_SCHEMAS);
            serializersTarget = rootTarget.path(SERIALIZERS_PATH);
            filesTarget = rootTarget.path(FILES_PATH);
        }

    }

    private UrlSelector createUrlSelector() {
        UrlSelector urlSelector = null;
        String rootCatalogURL = configuration.getValue(SCHEMA_REGISTRY_URL.name());
        String urlSelectorClass = configuration.getValue(Configuration.URL_SELECTOR_CLASS.name());
        if (urlSelectorClass == null) {
            urlSelector = new LoadBalancedFailoverUrlSelector(rootCatalogURL);
        } else {
            try {
                urlSelector = (UrlSelector) Class.forName(urlSelectorClass)
                  .getConstructor(String.class)
                  .newInstance(rootCatalogURL);
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException
              | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }
        urlSelector.init(configuration.getConfig());

        return urlSelector;
    }

    private SchemaMetadataCache.SchemaMetadataFetcher createSchemaMetadataFetcher() {
        return new SchemaMetadataCache.SchemaMetadataFetcher() {
            @Override
            public SchemaMetadataInfo fetch(String name) throws SchemaNotFoundException {
                try {
                    return runRetryableBlock((SchemaRegistryTargets targets) -> {
                        return getEntity(targets.schemasTarget.path(name), SchemaMetadataInfo.class);
                    });
                } catch (NotFoundException e) {
                    throw new SchemaNotFoundException(e, name);
                }
            }

            @Override
            public SchemaMetadataInfo fetch(Long id) throws SchemaNotFoundException {
                try {
                    return runRetryableBlock((SchemaRegistryTargets targets) -> {
                        return getEntity(targets.schemasByIdTarget.path(id.toString()), SchemaMetadataInfo.class);
                    });
                } catch (NotFoundException e) {
                    throw new SchemaNotFoundException(e, String.valueOf(id));
                }
            }
        };
    }

    protected ClientConfig createClientConfig(Map<String, ?> conf) {
        ClientConfig config = new ClientConfig();
        config.property(ClientProperties.CONNECT_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
        config.property(ClientProperties.READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
        config.property(ClientProperties.FOLLOW_REDIRECTS, true);
        for (Map.Entry<String, ?> entry : conf.entrySet()) {
            config.property(entry.getKey(), entry.getValue());
        }
        return config;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public Collection<SchemaProviderInfo> getSupportedSchemaProviders() {
        return runRetryableBlock((SchemaRegistryTargets targets) -> {
            return getEntities(targets.schemaProvidersTarget, SchemaProviderInfo.class);
        });
    }

    @Override
    public Long registerSchemaMetadata(SchemaMetadata schemaMetadata) {
        return addSchemaMetadata(schemaMetadata);
    }

    @Override
    public Long addSchemaMetadata(SchemaMetadata schemaMetadata) {
        SchemaMetadataInfo schemaMetadataInfo = schemaMetadataCache.getIfPresent(SchemaMetadataCache.Key.of(schemaMetadata
          .getName()));
        if (schemaMetadataInfo == null) {
            return runRetryableBlock((SchemaRegistryTargets targets) -> {
                return doRegisterSchemaMetadata(schemaMetadata, targets.schemasTarget);
            });
        }

        return schemaMetadataInfo.getId();
    }

    @Override
    public SchemaMetadataInfo updateSchemaMetadata(String schemaName, SchemaMetadata schemaMetadata) {
        SchemaMetadataInfo schemaMetadataInfo = runRetryableBlock((SchemaRegistryTargets targets) -> {
            return postEntity(targets.schemasTarget.path(schemaName), schemaMetadata, SchemaMetadataInfo.class);
        });
        if (schemaMetadataInfo != null) {
            schemaMetadataCache.put(SchemaMetadataCache.Key.of(schemaName), schemaMetadataInfo);
        }
        return schemaMetadataInfo;
    }


    private Long doRegisterSchemaMetadata(SchemaMetadata schemaMetadata, WebTarget schemasTarget) {
        try {
            return postEntity(schemasTarget, schemaMetadata, Long.class);
        } catch (BadRequestException ex) {
            Response response = ex.getResponse();
            CatalogResponse catalogResponse = SchemaRegistryClient.readCatalogResponse(response.readEntity(String.class));
            if (catalogResponse.getResponseCode() == CatalogResponse.ResponseMessage.ENTITY_CONFLICT.getCode()) {
                SchemaMetadataInfo meta = checkNotNull(getSchemaMetadataInfo(schemaMetadata.getName()),
                  "Did not find schema " + schemaMetadata.getName());
                return meta.getId();
            } else {
                throw ex;
            }
        }
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        return schemaMetadataCache.get(SchemaMetadataCache.Key.of(schemaName));
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId) {
        return schemaMetadataCache.get(SchemaMetadataCache.Key.of(schemaMetadataId));
    }

    @Override
    public void deleteSchema(String schemaName) throws SchemaNotFoundException {
        Collection<SchemaVersionInfo> schemaVersionInfos = getAllVersions(schemaName);
        schemaMetadataCache.invalidateSchemaMetadata(SchemaMetadataCache.Key.of(schemaName));
        if (schemaVersionInfos != null) {
            for (SchemaVersionInfo schemaVersionInfo: schemaVersionInfos) {
                SchemaIdVersion schemaIdVersion = new SchemaIdVersion(schemaVersionInfo.getId());
                schemaVersionInfoCache.invalidateSchema(SchemaVersionInfoCache.Key.of(schemaIdVersion));
            }
        }

        Response response = runRetryableBlock((SchemaRegistryTargets targets) -> {
            WebTarget target = targets.schemasTarget.path(String.format("%s", schemaName));
            try {
                return login.doAction(new PrivilegedAction<Response>() {
                    @Override
                    public Response run() {
                        return target.request(MediaType.APPLICATION_JSON_TYPE).delete(Response.class);
                    }
                });
            } catch (LoginException | ProcessingException e) {
                throw new RegistryRetryableException(e);
            }
        });

        int status = response.getStatus();
        if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new SchemaNotFoundException(response.readEntity(String.class), schemaName);
        } else if (status == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
            throw new RuntimeException(response.readEntity(String.class));
        }
    }

    @Override
    public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata, SchemaVersion schemaVersion, boolean disableCanonicalCheck) throws
      InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        return addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaMetadata, schemaVersion, disableCanonicalCheck);
    }

    @Override
    public SchemaIdVersion addSchemaVersion(String schemaBranchName,
                                            SchemaMetadata schemaMetadata,
                                            SchemaVersion schemaVersion,
                                            boolean disableCanonicalCheck)
      throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        // get it, if it exists in cache
        SchemaDigestEntry schemaDigestEntry = buildSchemaTextEntry(schemaVersion, schemaMetadata.getName());
        SchemaIdVersion schemaIdVersion = schemaTextCache.getIfPresent(schemaDigestEntry);

        if (schemaIdVersion == null) {
            //register schema metadata if it does not exist
            Long metadataId = registerSchemaMetadata(schemaMetadata);
            if (metadataId == null) {
                LOG.error("Schema Metadata [{}] is not registered successfully", schemaMetadata);
                throw new RuntimeException("Given SchemaMetadata could not be registered: " + schemaMetadata);
            }

            // add schemaIdVersion
            schemaIdVersion = addSchemaVersion(schemaBranchName, schemaMetadata.getName(), schemaVersion, disableCanonicalCheck);
        }

        return schemaIdVersion;
    }

    @Override
    public SchemaIdVersion uploadSchemaVersion(String schemaName, String description, InputStream schemaVersionTextFile) throws
      InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        return uploadSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaName, description, schemaVersionTextFile);
    }

    public SchemaIdVersion uploadSchemaVersion(final String schemaBranchName,
                                               final String schemaName,
                                               final String description,
                                               final InputStream schemaVersionInputStream)
      throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {

        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("Schema with name " + schemaName + " not found", schemaName);
        }

        StreamDataBodyPart streamDataBodyPart = new StreamDataBodyPart("file", schemaVersionInputStream);

        Response response = runRetryableBlock((SchemaRegistryTargets targets) -> {
            WebTarget target = targets.schemasTarget.path(schemaName).path("/versions/upload").queryParam("branch", schemaBranchName);
            MultiPart multipartEntity =
              new FormDataMultiPart()
                .field("description", description, MediaType.APPLICATION_JSON_TYPE)
                .bodyPart(streamDataBodyPart);

            Entity<MultiPart> multiPartEntity = Entity.entity(multipartEntity, MediaType.MULTIPART_FORM_DATA);
            try {
                return login.doAction(new PrivilegedAction<Response>() {
                    @Override
                    public Response run() {
                        return target.request().post(multiPartEntity, Response.class);
                    }
                });
            } catch (LoginException | ProcessingException e) {
                throw new RegistryRetryableException(e);
            }
        });

        return handleSchemaIdVersionResponse(schemaMetadataInfo, response);
    }

    private String getHashFunction() {
        return configuration.getValue(Configuration.HASH_FUNCTION.name());
    }

    private SchemaDigestEntry buildSchemaTextEntry(SchemaVersion schemaVersion, String name) {
        byte[] digest;
        try {
            digest = MessageDigest.getInstance(getHashFunction()).digest(schemaVersion.getSchemaText().getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        // storing schema text string is expensive, so storing digest in cache's key.
        return new SchemaDigestEntry(name, digest);
    }

    @Override
    public SchemaIdVersion addSchemaVersion(final String schemaName, final SchemaVersion schemaVersion, boolean disableCanonicalCheck)
      throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
        return addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaName, schemaVersion, disableCanonicalCheck);
    }

    @Override
    public SchemaIdVersion addSchemaVersion(final String schemaBranchName,
                                            final String schemaName,
                                            final SchemaVersion schemaVersion,
                                            boolean disableCanonicalCheck)
      throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {

        try {
            return schemaTextCache.get(buildSchemaTextEntry(schemaVersion, schemaName),
              () -> doAddSchemaVersion(schemaBranchName, schemaName, schemaVersion, disableCanonicalCheck));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            LOG.error("Encountered error while adding new version [{}] of schema [{}] and error [{}]", schemaVersion, schemaName, e);
            if (cause != null) {
                if (cause instanceof InvalidSchemaException) {
                    throw (InvalidSchemaException) cause;
                } else if (cause instanceof IncompatibleSchemaException) {
                    throw (IncompatibleSchemaException) cause;
                } else if (cause instanceof SchemaNotFoundException) {
                    throw (SchemaNotFoundException) cause;
                } else {
                    throw new RuntimeException(cause.getMessage(), cause);
                }
            } else {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    @Override
    public void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException, SchemaLifecycleException {
        schemaVersionInfoCache.invalidateSchema(new SchemaVersionInfoCache.Key(schemaVersionKey));

        Response response = runRetryableBlock((SchemaRegistryTargets targets) -> {
            WebTarget target = targets.schemasTarget.path(String.format("%s/versions/%s", schemaVersionKey
              .getSchemaName(), schemaVersionKey.getVersion()));
            try {
                return login.doAction(new PrivilegedAction<Response>() {
                    @Override
                    public Response run() {
                        return target.request(MediaType.APPLICATION_JSON_TYPE).delete(Response.class);
                    }
                });
            } catch (LoginException | ProcessingException e) {
                throw new RegistryRetryableException(e);
            }
        });

        handleDeleteSchemaResponse(response);
    }

    private void handleDeleteSchemaResponse(Response response) throws SchemaNotFoundException, SchemaLifecycleException {
        String msg = response.readEntity(String.class);
        switch (Response.Status.fromStatusCode(response.getStatus())) {
            case NOT_FOUND:
                throw new SchemaNotFoundException(msg);
            case BAD_REQUEST:
                throw new SchemaLifecycleException(msg);
            case INTERNAL_SERVER_ERROR:
                throw new RuntimeException(msg);
            default:
                break;
        }
    }

    private SchemaIdVersion doAddSchemaVersion(String schemaBranchName, String schemaName,
                                               SchemaVersion schemaVersion, boolean disableCanonicalCheck)
      throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException {
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("Schema with name " + schemaName + " not found", schemaName);
        }


        Response response = runRetryableBlock((SchemaRegistryTargets targets) -> {
            try {
                WebTarget target = targets.schemasTarget.path(schemaName).path("/versions").queryParam("branch", schemaBranchName)
                  .queryParam("disableCanonicalCheck", disableCanonicalCheck);
                return login.doAction(new PrivilegedAction<Response>() {
                    @Override
                    public Response run() {
                        return target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(schemaVersion), Response.class);
                    }
                });
            } catch (LoginException | ProcessingException e) {
                throw new RegistryRetryableException(e);
            }
        });
        return handleSchemaIdVersionResponse(schemaMetadataInfo, response);
    }

    private SchemaIdVersion handleSchemaIdVersionResponse(SchemaMetadataInfo schemaMetadataInfo,
                                                          Response response) throws IncompatibleSchemaException, InvalidSchemaException {
        int status = response.getStatus();
        String msg = response.readEntity(String.class);
        if (status == Response.Status.BAD_REQUEST.getStatusCode() || status == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
            CatalogResponse catalogResponse = readCatalogResponse(msg);
            if (CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA.getCode() == catalogResponse.getResponseCode()) {
                throw new IncompatibleSchemaException(catalogResponse.getResponseMessage());
            } else if (CatalogResponse.ResponseMessage.INVALID_SCHEMA.getCode() == catalogResponse.getResponseCode()) {
                throw new InvalidSchemaException(catalogResponse.getResponseMessage());
            } else {
                throw new RuntimeException(catalogResponse.getResponseMessage());
            }

        }

        Integer version = readEntity(msg, Integer.class);

        SchemaVersionInfo schemaVersionInfo = doGetSchemaVersionInfo(new SchemaVersionKey(schemaMetadataInfo.getSchemaMetadata()
          .getName(), version));

        return new SchemaIdVersion(schemaMetadataInfo.getId(), version, schemaVersionInfo.getId());
    }

    public static CatalogResponse readCatalogResponse(String msg) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode node = objectMapper.readTree(msg);

            return objectMapper.treeToValue(node, CatalogResponse.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SchemaVersionInfo getSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
        try {
            return schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaIdVersion));
        } catch (SchemaNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        return getLatestSchemaVersionInfo(SchemaBranch.MASTER_BRANCH, schemaName);
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        try {
            return schemaVersionInfoCache.getSchema(SchemaVersionInfoCache.Key.of(schemaVersionKey));
        } catch (SchemaNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private SchemaVersionInfo doGetSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
        if (schemaIdVersion.getSchemaVersionId() != null) {
            LOG.info("Getting schema version from target registry by its id [{}]", schemaIdVersion.getSchemaVersionId());
            return doGetSchemaIdVersionInfo(schemaIdVersion.getSchemaVersionId());
        } else if (schemaIdVersion.getSchemaMetadataId() != null) {
            SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaIdVersion.getSchemaMetadataId());
            final String schemaName = schemaMetadataInfo.getSchemaMetadata().getName();
            SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName, schemaIdVersion.getVersion());
            LOG.info("Getting schema version for \"{}\" from target registry for key [{}]", schemaName, schemaVersionKey);
            return doGetSchemaVersionInfo(schemaVersionKey);
        }

        throw new IllegalArgumentException("Given argument not valid: " + schemaIdVersion);
    }

    private SchemaVersionInfo doGetSchemaIdVersionInfo(Long versionId) {
        return runRetryableBlock((SchemaRegistryTargets targets) ->
          getEntity(targets.schemaVersionsByIdTarget.path(versionId.toString()), SchemaVersionInfo.class));
    }

    private SchemaVersionInfo doGetSchemaVersionInfo(SchemaVersionKey schemaVersionKey) {
        LOG.info("Getting schema version from target registry for [{}]", schemaVersionKey);
        String schemaName = schemaVersionKey.getSchemaName();
        return runRetryableBlock((SchemaRegistryTargets targets) ->
          getEntity(targets.schemasTarget.path(String.format("%s/versions/%d", schemaName, schemaVersionKey.getVersion())),
            SchemaVersionInfo.class));
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaBranchName, String schemaName) throws SchemaNotFoundException {
        return runRetryableBlock((SchemaRegistryTargets targets) -> {
            WebTarget webTarget = targets.schemasTarget.path(encode(schemaName) + "/versions/latest").queryParam("branch", schemaBranchName);
            return getEntity(webTarget, SchemaVersionInfo.class);
        });
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaName) throws SchemaNotFoundException {
        return getAllVersions(SchemaBranch.MASTER_BRANCH, schemaName);
    }

    private static String encode(String schemaName) {
        try {
            return URLEncoder.encode(schemaName, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void enableSchemaVersion(Long schemaVersionId)
      throws SchemaNotFoundException, SchemaLifecycleException, IncompatibleSchemaException {
        try {
            transitionSchemaVersionState(schemaVersionId, "enable", null);
        } catch (SchemaLifecycleException e) {
            Throwable cause = e.getCause();
            if (cause != null && cause instanceof IncompatibleSchemaException) {
                throw (IncompatibleSchemaException) cause;
            }
            throw e;
        }
    }

    @Override
    public void disableSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        transitionSchemaVersionState(schemaVersionId, "disable", null);
    }

    @Override
    public void deleteSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        transitionSchemaVersionState(schemaVersionId, "delete", null);
    }

    @Override
    public void archiveSchemaVersion(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        transitionSchemaVersionState(schemaVersionId, "archive", null);
    }

    @Override
    public void startSchemaVersionReview(Long schemaVersionId) throws SchemaNotFoundException, SchemaLifecycleException {
        transitionSchemaVersionState(schemaVersionId, "startReview", null);
    }

    @Override
    public SchemaVersionMergeResult mergeSchemaVersion(Long schemaVersionId, boolean disableCanonicalCheck)
      throws SchemaNotFoundException, IncompatibleSchemaException {
        Response response = runRetryableBlock((SchemaRegistryTargets targets) -> {
            try {
                WebTarget target = targets.schemasTarget.path(schemaVersionId + "/merge").queryParam("disableCanonicalCheck", disableCanonicalCheck);
                return login.doAction(new PrivilegedAction<Response>() {
                    @Override
                    public Response run() {
                        return target.request().post(null);
                    }
                });
            } catch (LoginException | ProcessingException e) {
                throw new RegistryRetryableException(e);
            }
        });

        int status = response.getStatus();
        if (status == Response.Status.OK.getStatusCode()) {
            String msg = response.readEntity(String.class);
            return readEntity(msg, SchemaVersionMergeResult.class);
        } else if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new SchemaNotFoundException(response.readEntity(String.class), String.valueOf(schemaVersionId));
        } else if (status == Response.Status.BAD_REQUEST.getStatusCode()) {
            throw new IncompatibleSchemaException(response.readEntity(String.class));
        } else {
            throw new RuntimeException(response.readEntity(String.class));
        }
    }

    @Override
    public void transitionState(Long schemaVersionId,
                                Byte targetStateId,
                                byte[] transitionDetails) throws SchemaNotFoundException, SchemaLifecycleException {
        boolean result = transitionSchemaVersionState(schemaVersionId, targetStateId.toString(), transitionDetails);
    }

    @Override
    public SchemaVersionLifecycleStateMachineInfo getSchemaVersionLifecycleStateMachineInfo() {
        return runRetryableBlock((SchemaRegistryTargets targets) -> {
            return getEntity(targets.schemaVersionsStatesMachineTarget,
              SchemaVersionLifecycleStateMachineInfo.class);
        });
    }

    @Override
    public SchemaBranch createSchemaBranch(Long schemaVersionId, SchemaBranch schemaBranch)
      throws SchemaBranchAlreadyExistsException, SchemaNotFoundException {
        Response response = runRetryableBlock((SchemaRegistryTargets targets) -> {
            WebTarget target = targets.schemasTarget.path("versionsById/" + schemaVersionId + "/branch");
            try {
                return login.doAction(new PrivilegedAction<Response>() {
                    @Override
                    public Response run() {
                        return target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(schemaBranch), Response.class);
                    }
                });
            } catch (LoginException | ProcessingException e) {
                throw new RegistryRetryableException(e);
            }
        });

        int status = response.getStatus();
        if (status == Response.Status.OK.getStatusCode()) {
            String msg = response.readEntity(String.class);
            SchemaBranch returnedSchemaBranch = readEntity(msg, SchemaBranch.class);
            return returnedSchemaBranch;
        } else if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new SchemaNotFoundException(response.readEntity(String.class));
        } else if (status == Response.Status.CONFLICT.getStatusCode()) {
            throw new SchemaBranchAlreadyExistsException(response.readEntity(String.class));
        } else {
            throw new RuntimeException(response.readEntity(String.class));
        }
    }

    @Override
    public Collection<SchemaBranch> getSchemaBranches(String schemaName) throws SchemaNotFoundException {
        Response response = runRetryableBlock((SchemaRegistryTargets targets) -> {
            WebTarget target = targets.schemasTarget.path(encode(schemaName) + "/branches");
            try {
                return login.doAction(new PrivilegedAction<Response>() {
                    @Override
                    public Response run() {
                        return target.request().get();
                    }
                });
            } catch (LoginException | ProcessingException e) {
                throw new RegistryRetryableException(e);
            }
        });

        int status = response.getStatus();
        if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new SchemaNotFoundException(response.readEntity(String.class), schemaName);
        } else if (status != Response.Status.OK.getStatusCode()) {
            throw new RuntimeException(response.readEntity(String.class));
        }

        return parseResponseAsEntities(response.readEntity(String.class), SchemaBranch.class);
    }

    @Override
    public void deleteSchemaBranch(Long schemaBranchId) throws SchemaBranchNotFoundException, InvalidSchemaBranchDeletionException {
        Response response = runRetryableBlock((SchemaRegistryTargets targets) -> {
            WebTarget target = targets.schemasTarget.path("branch/" + schemaBranchId);
            try {
                return login.doAction(new PrivilegedAction<Response>() {
                    @Override
                    public Response run() {
                        return target.request().delete();
                    }
                });
            } catch (LoginException | ProcessingException e) {
                throw new RegistryRetryableException(e);
            }
        });

        int status = response.getStatus();
        if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new SchemaBranchNotFoundException(response.readEntity(String.class));
        } else if (status == Response.Status.BAD_REQUEST.getStatusCode()) {
            throw new InvalidSchemaBranchDeletionException(response.readEntity(String.class));
        } else if (status != Response.Status.OK.getStatusCode()) {
            throw new RuntimeException(response.readEntity(String.class));
        }

    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaBranchName, String schemaName, List<Byte> stateIds)
      throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return runRetryableBlock((SchemaRegistryTargets targets) -> {
            WebTarget webTarget = targets.schemasTarget
              .path(encode(schemaName) + "/versions")
              .queryParam("branch", schemaBranchName)
              .queryParam("states", stateIds.toArray());
            return getEntities(webTarget, SchemaVersionInfo.class);
        });
    }

    private boolean transitionSchemaVersionState(Long schemaVersionId,
                                                 String operationOrTargetState,
                                                 byte[] transitionDetails) throws SchemaNotFoundException, SchemaLifecycleException {

        Response response = runRetryableBlock((SchemaRegistryTargets targets) -> {
            WebTarget webTarget = targets.schemaVersionsTarget.path(schemaVersionId + "/state/" + operationOrTargetState);
            try {
                return login.doAction(new PrivilegedAction<Response>() {
                    @Override
                    public Response run() {
                        return webTarget.request().post(Entity.text(transitionDetails));
                    }
                });
            } catch (LoginException | ProcessingException e) {
                throw new RegistryRetryableException(e);
            }
        });

        boolean result = handleSchemaLifeCycleResponse(response);

        // invalidate this entry from cache.
        schemaVersionInfoCache.invalidateSchema(SchemaVersionInfoCache.Key.of(new SchemaIdVersion(schemaVersionId)));

        return result;
    }

    private boolean handleSchemaLifeCycleResponse(Response response) throws SchemaNotFoundException, SchemaLifecycleException {
        boolean result;
        int status = response.getStatus();
        if (status == Response.Status.OK.getStatusCode()) {
            result = response.readEntity(Boolean.class);
        } else if (status == Response.Status.NOT_FOUND.getStatusCode()) {
            throw new SchemaNotFoundException(response.readEntity(String.class));
        } else if (status == Response.Status.BAD_REQUEST.getStatusCode()) {
            CatalogResponse catalogResponse = readCatalogResponse(response.readEntity(String.class));
            if (catalogResponse.getResponseCode() == CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA.getCode()) {
                throw new SchemaLifecycleException(new IncompatibleSchemaException(catalogResponse.getResponseMessage()));
            }
            throw new SchemaLifecycleException(catalogResponse.getResponseMessage());

        } else {
            throw new RuntimeException(response.readEntity(String.class));
        }

        return result;
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaBranchName, String schemaName) throws SchemaNotFoundException {
        return runRetryableBlock((SchemaRegistryTargets targets) -> {
            WebTarget webTarget = targets.schemasTarget.path(encode(schemaName) + "/versions").queryParam("branch", schemaBranchName);
            return getEntities(webTarget, SchemaVersionInfo.class);
        });
    }

    @Override
    public List<AggregatedSchemaMetadataInfo> findAggregatedSchemas(String schemaName, String schemaDescription, String orderByFields) {
        if (StringUtils.isBlank(schemaName)) {
            schemaName = "";
        }
        if (StringUtils.isBlank(schemaDescription)) {
            schemaDescription = null;
        }
        if (StringUtils.isBlank(orderByFields)) {
            orderByFields = "timestamp,d";
        }
        final String schemaNameParam = schemaName;
        final String schemaDescriptionParam = schemaDescription;
        final String orderByFieldsParam = orderByFields;

        return runRetryableBlock((SchemaRegistryTargets targets) -> {
            WebTarget target = targets.findAggregatedSchemasTarget;
            target = target.queryParam("name", schemaNameParam);
            if (schemaDescriptionParam != null) {
                target = target.queryParam("description", schemaDescriptionParam);
            }
            target = target.queryParam("_orderByFields", orderByFieldsParam);

            return getEntities(target, AggregatedSchemaMetadataInfo.class);
        });
    }

    @Override
    public CompatibilityResult checkCompatibility(String schemaName, String toSchemaText)
      throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return checkCompatibility(SchemaBranch.MASTER_BRANCH, schemaName, toSchemaText);
    }

    @Override
    public CompatibilityResult checkCompatibility(String schemaBranchName, String schemaName,
                                                  String toSchemaText) throws SchemaNotFoundException {
        String response = runRetryableBlock((SchemaRegistryTargets targets) -> {
            try {
                WebTarget webTarget = targets.schemasTarget.path(encode(schemaName) + "/compatibility").queryParam("branch", schemaBranchName);
                return login.doAction(new PrivilegedAction<String>() {
                    @Override
                    public String run() {
                        return webTarget.request().post(Entity.text(toSchemaText), String.class);
                    }
                });
            } catch (LoginException | ProcessingException e) {
                throw new RegistryRetryableException(e);
            }
        });
        return readEntity(response, CompatibilityResult.class);
    }

    @Override
    public boolean isCompatibleWithAllVersions(String schemaName, String toSchemaText) throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return isCompatibleWithAllVersions(SchemaBranch.MASTER_BRANCH, schemaName, toSchemaText);
    }

    @Override
    public boolean isCompatibleWithAllVersions(String schemaBranchName, String schemaName, String toSchemaText)
      throws SchemaNotFoundException, SchemaBranchNotFoundException {
        return checkCompatibility(schemaBranchName, schemaName, toSchemaText).isCompatible();
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasByFields(SchemaFieldQuery schemaFieldQuery) {
        return runRetryableBlock((SchemaRegistryTargets targets) -> {
            WebTarget target = targets.searchFieldsTarget;
            for (Map.Entry<String, String> entry : schemaFieldQuery.toQueryMap().entrySet()) {
                target = target.queryParam(entry.getKey(), entry.getValue());
            }
            return getEntities(target, SchemaVersionKey.class);
        });
    }

    @Override
    public String uploadFile(InputStream inputStream) {
        MultiPart multiPart = new MultiPart();
        BodyPart filePart = new StreamDataBodyPart("file", inputStream, "file");
        multiPart.bodyPart(filePart);
        return runRetryableBlock((SchemaRegistryTargets targets) -> {
            try {
                return login.doAction(new PrivilegedAction<String>() {
                    @Override
                    public String run() {
                        return targets.filesTarget.request().accept(MediaType.TEXT_PLAIN)
                          .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA), String.class);
                    }
                });
            } catch (LoginException | ProcessingException e) {
                throw new RegistryRetryableException(e);
            }
        });
    }

    @Override
    public InputStream downloadFile(String fileId) {
        return runRetryableBlock((SchemaRegistryTargets targets) -> {
            try {
                return login.doAction(new PrivilegedAction<InputStream>() {
                    @Override
                    public InputStream run() {
                        return targets.filesTarget.path("download/" + encode(fileId))
                          .request()
                          .get(InputStream.class);
                    }
                });
            } catch (LoginException | ProcessingException e) {
                urlSelector.urlWithError(targets.rootTarget.getUri().toString(), e);
                throw new RegistryRetryableException(e);
            }
        });
    }

    @Override
    public Long addSerDes(SerDesPair serDesPair) {
        return runRetryableBlock((SchemaRegistryTargets targets) -> {
            return postEntity(targets.serializersTarget, serDesPair, Long.class);
        });
    }

    @Override
    public void mapSchemaWithSerDes(String schemaName, Long serDesId) {
        String path = String.format("%s/mapping/%s", encode(schemaName), serDesId.toString());

        Boolean success = runRetryableBlock((SchemaRegistryTargets targets) -> {
            return postEntity(targets.schemasTarget.path(path), null, Boolean.class);
        });
        LOG.info("Received response while mapping schema [{}] with serialzer/deserializer [{}] : [{}]", schemaName, serDesId, success);
    }

    @Override
    public <T> T getDefaultSerializer(String type) throws SerDesException {
        Collection<SchemaProviderInfo> supportedSchemaProviders = getSupportedSchemaProviders();
        for (SchemaProviderInfo schemaProvider : supportedSchemaProviders) {
            if (schemaProvider.getType().equals(type)) {
                try {
                    return (T) Class.forName(schemaProvider.getDefaultSerializerClassName()).newInstance();
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new SerDesException(e);
                }
            }
        }

        throw new IllegalArgumentException("No schema provider registered for the given type " + type);
    }

    @Override
    public <T> T getDefaultDeserializer(String type) throws SerDesException {
        Collection<SchemaProviderInfo> supportedSchemaProviders = getSupportedSchemaProviders();
        for (SchemaProviderInfo schemaProvider : supportedSchemaProviders) {
            if (schemaProvider.getType().equals(type)) {
                try {
                    return (T) Class.forName(schemaProvider.getDefaultDeserializerClassName()).newInstance();
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new SerDesException(e);
                }
            }
        }

        throw new IllegalArgumentException("No schema provider registered for the given type " + type);
    }

    @Override
    public Collection<SerDesInfo> getSerDes(String schemaName) {
        return runRetryableBlock((SchemaRegistryTargets targets) -> {
            String path = encode(schemaName) + "/serdes/";
            return getEntities(targets.schemasTarget.path(path), SerDesInfo.class);
        });
    }

    public <T> T createSerializerInstance(SerDesInfo serDesInfo) {
        return createInstance(serDesInfo, true);
    }

    @Override
    public <T> T createDeserializerInstance(SerDesInfo serDesInfo) {
        return createInstance(serDesInfo, false);
    }

    /**
     * <p>Closes the client and all open connections.</p>
     * If Kerberos was enabled this will also close the Kerberos ticket management thread. We will wait up to 1 second
     * for the thread to close. In case the thread doesn't shut down in time, you will see a warning in the logs.
     */
    @Override
    public void close() {
        if (login != null) {
            try {
                login.close();
            } catch (Exception ex) {
                LOG.debug("Exception thrown while closing the kerberos login.", ex);
            }
        }
        client.close();
    }

    public SchemaRegistryVersion clientVersion() {
        return CLIENT_VERSION;
    }

    private <T> T createInstance(SerDesInfo serDesInfo, boolean isSerializer) {
        Set<Class<?>> interfaceClasses = isSerializer ? SERIALIZER_INTERFACE_CLASSES : DESERIALIZER_INTERFACE_CLASSES;

        if (interfaceClasses == null || interfaceClasses.isEmpty()) {
            throw new IllegalArgumentException("interfaceClasses array must be neither null nor empty.");
        }

        // loading serializer, create a class loader and and keep them in cache.
        final SerDesPair serDesPair = serDesInfo.getSerDesPair();
        String fileId = serDesPair.getFileId();
        // get class loader for this file ID
        ClassLoader classLoader = classLoaderCache.getClassLoader(fileId);

        T t;
        try {
            String className =
              isSerializer ? serDesPair.getSerializerClassName() : serDesPair.getDeserializerClassName();

            Class<T> clazz = (Class<T>) Class.forName(className, true, classLoader);
            t = clazz.newInstance();
            List<Class<?>> classes = new ArrayList<>();
            for (Class<?> interfaceClass : interfaceClasses) {
                if (interfaceClass.isAssignableFrom(clazz)) {
                    classes.add(interfaceClass);
                }
            }

            if (classes.isEmpty()) {
                throw new RuntimeException("Given Serialize/Deserializer " + className + " class does not implement any " +
                  "one of the registered interfaces: " + interfaceClasses);
            }

            Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
              classes.toArray(new Class[classes.size()]),
              new ClassLoaderAwareInvocationHandler(classLoader, t));
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new SerDesException(e);
        }

        return t;
    }

    private <T> List<T> getEntities(WebTarget target, Class<T> clazz) {
        String response = null;
        try {
            response = login.doAction(new PrivilegedAction<String>() {
                @Override
                public String run() {
                    return target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
                }
            });
        } catch (LoginException | ProcessingException e) {
            throw new RegistryRetryableException(e);
        }
        return parseResponseAsEntities(response, clazz);
    }

    private <T> List<T> parseResponseAsEntities(String response, Class<T> clazz) {
        List<T> entities = new ArrayList<>();
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(response);
            Iterator<JsonNode> it = node.get("entities").elements();
            while (it.hasNext()) {
                entities.add(mapper.treeToValue(it.next(), clazz));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return entities;
    }

    private <T> T postEntity(WebTarget target, Object json, Class<T> responseType) {
        String response = null;
        try {
            response = login.doAction(new PrivilegedAction<String>() {
                @Override
                public String run() {
                    return target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(json), String.class);
                }
            });
        } catch (LoginException | ProcessingException e) {
            throw new RegistryRetryableException(e);
        }
        return readEntity(response, responseType);
    }

    private <T> T readEntity(String response, Class<T> clazz) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(response, clazz);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private <T> T getEntity(WebTarget target, Class<T> clazz) {
        String response = null;
        try {
            response = login.doAction(new PrivilegedAction<String>() {
                @Override
                public String run() {
                    return target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
                }
            });
        } catch (LoginException | ProcessingException e) {
            throw new RegistryRetryableException(e);
        }

        return readEntity(response, clazz);
    }

    /**
     *   If schema registry client is configured with URL ensemble eg: url1,url2,url3 and ExponentialBackoffPolicy is
     *   configured as a retry mechanism, then retry is done in following manner
     *      1) Try url1, if not reachable try url2 and if url2 is not reachable try url3
     *         if none of the urls are reachable, then
     *            proceed to step 2)
     *         else
     *            return
     *      2) sleep for sleepMs which is defined according to the backoff policy configured.
     *      3) Go to step 1)
     *   Retry attempts are made as long as they don't exceed the max attempts and are with in the timeoutMs configured
     *   with the back off policy. In case no more attempts can be carried out due breach of number of attempts or exceeding timeout,
     *   exception thrown with in the {@link RegistryRetryableBlock} is resurfaced.
     *
     * @param registryRetryableBlock Block of code on which retry attempts should be made in case of failures
     * @param <T> return type of registryRetryableBlock
     * @return
     */
    private <T> T runRetryableBlock(RegistryRetryableBlock<T> registryRetryableBlock) {
        return retryExecutor.execute(() -> {
            WebTarget initialWebTarget = null;
            RegistryRetryableException retryableException = null;
            while (true) {
                SchemaRegistryClient.SchemaRegistryTargets targets = currentSchemaRegistryTargets();
                if (initialWebTarget == null) {
                    initialWebTarget = targets.rootTarget;
                } else if (initialWebTarget.equals(targets.rootTarget)) {
                    throw retryableException;
                }
                try {
                    LOG.debug("Using '{}' to make request", targets.rootTarget);
                    return registryRetryableBlock.run(targets);
                } catch (RegistryRetryableException e) {
                    urlSelector.urlWithError(targets.rootTarget.getUri().toString(), e);
                    retryableException = e;
                }
            }
        });
    }

    public static final class Configuration {
        // we may want to remove schema.registry prefix from configuration properties as these are all properties
        // given by client.
        /**
         * URL of schema registry to which this client connects to. For ex: http://localhost:9090/api/v1
         */
        public static final ConfigEntry<String> SCHEMA_REGISTRY_URL =
          ConfigEntry.mandatory("schema.registry.url",
            String.class,
            "URL of schema registry to which this client connects to. For ex: http://localhost:9090/api/v1",
            "http://localhost:9090/api/v1",
            ConfigEntry.StringConverter.get(),
            ConfigEntry.NonEmptyStringValidator.get());

        /**
         * Default path for downloaded jars to be stored.
         */
        public static final String DEFAULT_LOCAL_JARS_PATH = "/tmp/schema-registry/local-jars";

        /**
         * Local directory path to which downloaded jars should be copied to. For ex: /tmp/schema-registry/local-jars
         */
        public static final ConfigEntry<String> LOCAL_JAR_PATH =
          ConfigEntry.optional("schema.registry.client.local.jars.path",
            String.class,
            "URL of schema registry to which this client connects to. For ex: http://localhost:9090/api/v1",
            DEFAULT_LOCAL_JARS_PATH,
            ConfigEntry.StringConverter.get(),
            ConfigEntry.NonEmptyStringValidator.get());

        /**
         * Default value for classloader cache size.
         */
        public static final long DEFAULT_CLASSLOADER_CACHE_SIZE = 1024;

        /**
         * Default value for cache expiry interval in seconds.
         */
        public static final long DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS = 60 * 60;

        /**
         * Maximum size of classloader cache. Default value is {@link #DEFAULT_CLASSLOADER_CACHE_SIZE}
         * Classloaders are created for serializer/deserializer jars downloaded from schema registry and they will be locally cached.
         */
        public static final ConfigEntry<Number> CLASSLOADER_CACHE_SIZE =
          ConfigEntry.optional("schema.registry.client.class.loader.cache.size",
            Integer.class,
            "Maximum size of classloader cache",
            DEFAULT_CLASSLOADER_CACHE_SIZE,
            ConfigEntry.IntegerConverter.get(),
            ConfigEntry.PositiveNumberValidator.get());

        /**
         * Expiry interval(in seconds) of an entry in classloader cache. Default value is {@link #DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS}
         * Classloaders are created for serializer/deserializer jars downloaded from schema registry and they will be locally cached.
         */
        public static final ConfigEntry<Number> CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS =
          ConfigEntry.optional("schema.registry.client.class.loader.cache.expiry.interval.secs",
            Integer.class,
            "Expiry interval(in seconds) of an entry in classloader cache",
            DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS,
            ConfigEntry.IntegerConverter.get(),
            ConfigEntry.PositiveNumberValidator.get());

        public static final long DEFAULT_SCHEMA_CACHE_SIZE = 1024;
        public static final long DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS = 5 * 60;

        /**
         * Maximum size of schema version cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_SIZE}
         */
        public static final ConfigEntry<Number> SCHEMA_VERSION_CACHE_SIZE =
          ConfigEntry.optional("schema.registry.client.schema.version.cache.size",
            Integer.class,
            "Maximum size of schema version cache",
            DEFAULT_SCHEMA_CACHE_SIZE,
            ConfigEntry.IntegerConverter.get(),
            ConfigEntry.PositiveNumberValidator.get());

        /**
         * Expiry interval(in seconds) of an entry in schema version cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS}
         */
        public static final ConfigEntry<Number> SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS =
          ConfigEntry.optional("schema.registry.client.schema.version.cache.expiry.interval.secs",
            Integer.class,
            "Expiry interval(in seconds) of an entry in schema version cache",
            DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS,
            ConfigEntry.IntegerConverter.get(),
            ConfigEntry.PositiveNumberValidator.get());

        /**
         * Maximum size of schema metadata cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_SIZE}
         */
        public static final ConfigEntry<Number> SCHEMA_METADATA_CACHE_SIZE =
          ConfigEntry.optional("schema.registry.client.schema.metadata.cache.size",
            Integer.class,
            "Maximum size of schema metadata cache",
            DEFAULT_SCHEMA_CACHE_SIZE,
            ConfigEntry.IntegerConverter.get(),
            ConfigEntry.PositiveNumberValidator.get());

        /**
         * Expiry interval(in seconds) of an entry in schema metadata cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS}
         */
        public static final ConfigEntry<Number> SCHEMA_METADATA_CACHE_EXPIRY_INTERVAL_SECS =
          ConfigEntry.optional("schema.registry.client.schema.metadata.cache.expiry.interval.secs",
            Integer.class,
            "Expiry interval(in seconds) of an entry in schema metadata cache",
            DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS,
            ConfigEntry.IntegerConverter.get(),
            ConfigEntry.PositiveNumberValidator.get());

        /**
         * Maximum size of schema text cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_SIZE}.
         * This cache has ability to store/get entries with same schema name and schema text.
         */
        public static final ConfigEntry<Number> SCHEMA_TEXT_CACHE_SIZE =
          ConfigEntry.optional("schema.registry.client.schema.text.cache.size",
            Integer.class,
            "Maximum size of schema text cache",
            DEFAULT_SCHEMA_CACHE_SIZE,
            ConfigEntry.IntegerConverter.get(),
            ConfigEntry.PositiveNumberValidator.get());

        /**
         * Expiry interval(in seconds) of an entry in schema text cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS}
         */
        public static final ConfigEntry<Number> SCHEMA_TEXT_CACHE_EXPIRY_INTERVAL_SECS =
          ConfigEntry.optional("schema.registry.client.schema.text.cache.expiry.interval.secs",
            Integer.class,
            "Expiry interval(in seconds) of an entry in schema text cache.",
            DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS,
            ConfigEntry.IntegerConverter.get(),
            ConfigEntry.PositiveNumberValidator.get());

        /**
         *
         */
        public static final ConfigEntry<String> URL_SELECTOR_CLASS =
          ConfigEntry.optional("schema.registry.client.url.selector",
            String.class,
            "Schema Registry URL selector class.",
            FailoverUrlSelector.class.getName(),
            ConfigEntry.StringConverter.get(),
            ConfigEntry.NonEmptyStringValidator.get());

        /**
         *
         */
        public static final ConfigEntry<String> SASL_JAAS_CONFIG =
          ConfigEntry.optional("sasl.jaas.config",
            String.class,
            "Schema Registry Dynamic JAAS config for SASL connection.",
            null,
            ConfigEntry.StringConverter.get(),
            ConfigEntry.NonEmptyStringValidator.get());

        // connection properties
        /**
         * Default connection timeout on connections created while connecting to schema registry.
         */
        public static final int DEFAULT_CONNECTION_TIMEOUT = 30 * 1000;

        /**
         * Default read timeout on connections created while connecting to schema registry.
         */
        public static final int DEFAULT_READ_TIMEOUT = 30 * 1000;

        /**
         * Username for basic authentication.
         */
        public static final ConfigEntry<String> AUTH_USERNAME =
          ConfigEntry.optional("schema.registry.auth.username",
            String.class,
            "Username for basic authentication",
            null,
            ConfigEntry.StringConverter.get(),
            ConfigEntry.NonEmptyStringValidator.get());

        /**
         * Password for basic authentication.
         */
        public static final ConfigEntry<String> AUTH_PASSWORD =
          ConfigEntry.optional("schema.registry.auth.password",
            String.class,
            "Password for basic authentication",
            null,
            ConfigEntry.StringConverter.get(),
            ConfigEntry.NonEmptyStringValidator.get());

        public static final ConfigEntry<String> HASH_FUNCTION =
          ConfigEntry.optional("schema.registry.hash.function",
            String.class,
            "Hashing algorithm for generating schema fingerprints",
            "MD5",
            ConfigEntry.StringConverter.get(),
            ConfigEntry.NonEmptyStringValidator.get());

        private final Map<String, ?> config;
        private final Map<String, ConfigEntry<?>> options;

        public Configuration(Map<String, ?> config) {
            Field[] fields = this.getClass().getDeclaredFields();
            this.options = Collections.unmodifiableMap(buildOptions(fields));
            this.config = buildConfig(config);
        }

        Map<String, ?> buildConfig(Map<String, ?> config) {
            Map<String, Object> result = new HashMap<>();
            for (Map.Entry<String, ?> entry : config.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                Object finalValue = value;

                ConfigEntry configEntry = options.get(key);
                if (configEntry != null) {
                    if (value != null) {
                        finalValue = configEntry.converter().convert(value);
                        configEntry.validator().validate(key, finalValue);
                    } else {
                        finalValue = configEntry.defaultValue();
                    }
                }
                result.put(key, finalValue);
            }

            return result;
        }

        private Map<String, ConfigEntry<?>> buildOptions(Field[] fields) {
            Map<String, ConfigEntry<?>> options = new HashMap<>();
            for (Field field : fields) {
                Class<?> type = field.getType();

                if (type.isAssignableFrom(ConfigEntry.class)) {
                    field.setAccessible(true);
                    try {
                        ConfigEntry configEntry = (ConfigEntry) field.get(this);
                        options.put(configEntry.name(), configEntry);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            return options;
        }

        public <T> T getValue(String propertyKey) {
            return (T) (config.containsKey(propertyKey) ? config.get(propertyKey)
              : options.get(propertyKey).defaultValue());
        }

        public Map<String, Object> getConfig() {
            return Collections.unmodifiableMap(config);
        }

        public Collection<ConfigEntry<?>> getAvailableConfigEntries() {
            return options.values();
        }

    }

    private static class SchemaDigestEntry {
        private final String name;
        private final byte[] schemaDigest;

        SchemaDigestEntry(String name, byte[] schemaDigest) {
            checkNotNull(name, "name can not be null");
            checkNotNull(schemaDigest, "schema digest can not be null");

            this.name = name;
            this.schemaDigest = schemaDigest;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SchemaDigestEntry that = (SchemaDigestEntry) o;

            if (name != null ? !name.equals(that.name) : that.name != null) {
                return false;
            }
            return Arrays.equals(schemaDigest, that.schemaDigest);

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + Arrays.hashCode(schemaDigest);
            return result;
        }
    }

    private interface RegistryRetryableBlock<T> {
        T run(SchemaRegistryTargets targets) throws RegistryRetryableException;
    }
}
