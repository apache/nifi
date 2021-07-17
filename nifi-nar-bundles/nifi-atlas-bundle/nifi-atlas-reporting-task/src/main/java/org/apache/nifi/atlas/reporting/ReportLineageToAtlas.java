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
package org.apache.nifi.atlas.reporting;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.security.SecurityProperties;
import org.apache.atlas.utils.AtlasPathExtractorUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.atlas.NiFiAtlasClient;
import org.apache.nifi.atlas.NiFiFlow;
import org.apache.nifi.atlas.NiFiFlowAnalyzer;
import org.apache.nifi.atlas.hook.NiFiAtlasHook;
import org.apache.nifi.atlas.provenance.AnalysisContext;
import org.apache.nifi.atlas.provenance.FilesystemPathsLevel;
import org.apache.nifi.atlas.provenance.StandardAnalysisContext;
import org.apache.nifi.atlas.provenance.lineage.CompleteFlowPathLineage;
import org.apache.nifi.atlas.provenance.lineage.LineageStrategy;
import org.apache.nifi.atlas.provenance.lineage.SimpleFlowPathLineage;
import org.apache.nifi.atlas.resolver.NamespaceResolver;
import org.apache.nifi.atlas.resolver.NamespaceResolvers;
import org.apache.nifi.atlas.resolver.RegexNamespaceResolver;
import org.apache.nifi.atlas.security.AtlasAuthN;
import org.apache.nifi.atlas.security.Basic;
import org.apache.nifi.atlas.security.Kerberos;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringSelector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer.PROVENANCE_BATCH_SIZE;
import static org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer.PROVENANCE_START_POSITION;

@Tags({"atlas", "lineage"})
@CapabilityDescription("Report NiFi flow data set level lineage to Apache Atlas." +
        " End-to-end lineages across NiFi environments and other systems can be reported if those are" +
        " connected by different protocols and data set, such as NiFi Site-to-Site, Kafka topic or Hive tables ... etc." +
        " Atlas lineage reported by this reporting task can be useful to grasp the high level relationships between processes and data sets," +
        " in addition to NiFi provenance events providing detailed event level lineage." +
        " See 'Additional Details' for further description and limitations.")
@Stateful(scopes = Scope.LOCAL, description = "Stores the Reporting Task's last event Id so that on restart the task knows where it left off.")
@DynamicProperty(name = "hostnamePattern.<namespace>", value = "hostname Regex patterns",
                 description = RegexNamespaceResolver.PATTERN_PROPERTY_PREFIX_DESC, expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
// In order for each reporting task instance to have its own static objects such as KafkaNotification.
@RequiresInstanceClassLoading
public class ReportLineageToAtlas extends AbstractReportingTask {

    private static final String ATLAS_URL_DELIMITER = ",";
    static final PropertyDescriptor ATLAS_URLS = new PropertyDescriptor.Builder()
            .name("atlas-urls")
            .displayName("Atlas URLs")
            .description("Comma separated URL of Atlas Servers" +
                    " (e.g. http://atlas-server-hostname:21000 or https://atlas-server-hostname:21443)." +
                    " For accessing Atlas behind Knox gateway, specify Knox gateway URL" +
                    " (e.g. https://knox-hostname:8443/gateway/{topology-name}/atlas)." +
                    " If not specified, 'atlas.rest.address' in Atlas Configuration File is used.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATLAS_CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("atlas-connect-timeout")
            .displayName("Atlas Connect Timeout")
            .description("Max wait time for connection to Atlas.")
            .required(true)
            .defaultValue("60 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATLAS_READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("atlas-read-timeout")
            .displayName("Atlas Read Timeout")
            .description("Max wait time for response from Atlas.")
            .required(true)
            .defaultValue("60 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final AllowableValue ATLAS_AUTHN_BASIC = new AllowableValue("basic", "Basic", "Use username and password.");
    static final AllowableValue ATLAS_AUTHN_KERBEROS = new AllowableValue("kerberos", "Kerberos", "Use Kerberos keytab file.");
    static final PropertyDescriptor ATLAS_AUTHN_METHOD = new PropertyDescriptor.Builder()
            .name("atlas-authentication-method")
            .displayName("Atlas Authentication Method")
            .description("Specify how to authenticate this reporting task to Atlas server.")
            .required(true)
            .allowableValues(ATLAS_AUTHN_BASIC, ATLAS_AUTHN_KERBEROS)
            .defaultValue(ATLAS_AUTHN_BASIC.getValue())
            .build();

    public static final PropertyDescriptor ATLAS_USER = new PropertyDescriptor.Builder()
            .name("atlas-username")
            .displayName("Atlas Username")
            .description("User name to communicate with Atlas.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATLAS_PASSWORD = new PropertyDescriptor.Builder()
            .name("atlas-password")
            .displayName("Atlas Password")
            .description("Password to communicate with Atlas.")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ATLAS_CONF_DIR = new PropertyDescriptor.Builder()
            .name("atlas-conf-dir")
            .displayName("Atlas Configuration Directory")
            .description("Directory path that contains 'atlas-application.properties' file." +
                    " If not specified and 'Create Atlas Configuration File' is disabled," +
                    " then, 'atlas-application.properties' file under root classpath is used.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.DIRECTORY)
            // Atlas generates ssl-client.xml in this directory and then loads it from classpath
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor ATLAS_NIFI_URL = new PropertyDescriptor.Builder()
            .name("atlas-nifi-url")
            .displayName("NiFi URL for Atlas")
            .description("NiFi URL is used in Atlas to represent this NiFi cluster (or standalone instance)." +
                    " It is recommended to use one that can be accessible remotely instead of using 'localhost'.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ATLAS_DEFAULT_CLUSTER_NAME = new PropertyDescriptor.Builder()
            .name("atlas-default-cluster-name")
            .displayName("Atlas Default Metadata Namespace")
            .description("Namespace for Atlas entities reported by this ReportingTask." +
                    " If not specified, 'atlas.metadata.namespace' or 'atlas.cluster.name' (the former having priority) in Atlas Configuration File is used." +
                    " Multiple mappings can be configured by user defined properties." +
                    " See 'Additional Details...' for more.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ATLAS_CONF_CREATE = new PropertyDescriptor.Builder()
            .name("atlas-conf-create")
            .displayName("Create Atlas Configuration File")
            .description("If enabled, 'atlas-application.properties' file will be created in 'Atlas Configuration Directory'" +
                    " automatically when this Reporting Task starts." +
                    " Note that the existing configuration file will be overwritten.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("Specifies the SSL Context Service to use for communicating with Atlas and Kafka.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    static final PropertyDescriptor KAFKA_BOOTSTRAP_SERVERS = new PropertyDescriptor.Builder()
            .name("kafka-bootstrap-servers")
            .displayName("Kafka Bootstrap Servers")
            .description("Kafka Bootstrap Servers to send Atlas hook notification messages based on NiFi provenance events." +
                    " E.g. 'localhost:9092'" +
                    " NOTE: Once this reporting task has started, restarting NiFi is required to changed this property" +
                    " as Atlas library holds a unmodifiable static reference to Kafka client.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final AllowableValue SEC_PLAINTEXT = new AllowableValue("PLAINTEXT", "PLAINTEXT", "PLAINTEXT");
    static final AllowableValue SEC_SSL = new AllowableValue("SSL", "SSL", "SSL");
    static final AllowableValue SEC_SASL_PLAINTEXT = new AllowableValue("SASL_PLAINTEXT", "SASL_PLAINTEXT", "SASL_PLAINTEXT");
    static final AllowableValue SEC_SASL_SSL = new AllowableValue("SASL_SSL", "SASL_SSL", "SASL_SSL");
    static final PropertyDescriptor KAFKA_SECURITY_PROTOCOL = new PropertyDescriptor.Builder()
            .name("kafka-security-protocol")
            .displayName("Kafka Security Protocol")
            .description("Protocol used to communicate with Kafka brokers to send Atlas hook notification messages." +
                    " Corresponds to Kafka's 'security.protocol' property.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(SEC_PLAINTEXT, SEC_SSL, SEC_SASL_PLAINTEXT, SEC_SASL_SSL)
            .defaultValue(SEC_PLAINTEXT.getValue())
            .build();

    public static final PropertyDescriptor KERBEROS_PRINCIPAL = new PropertyDescriptor.Builder()
            .name("nifi-kerberos-principal")
            .displayName("Kerberos Principal")
            .description("The Kerberos principal for this NiFi instance to access Atlas API and Kafka brokers." +
                    " If not set, it is expected to set a JAAS configuration file in the JVM properties defined in the bootstrap.conf file." +
                    " This principal will be set into 'sasl.jaas.config' Kafka's property.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor KERBEROS_KEYTAB = new PropertyDescriptor.Builder()
            .name("nifi-kerberos-keytab")
            .displayName("Kerberos Keytab")
            .description("The Kerberos keytab for this NiFi instance to access Atlas API and Kafka brokers." +
                    " If not set, it is expected to set a JAAS configuration file in the JVM properties defined in the bootstrap.conf file." +
                    " This principal will be set into 'sasl.jaas.config' Kafka's property.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
        .name("kerberos-credentials-service")
        .displayName("Kerberos Credentials Service")
        .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
        .identifiesControllerService(KerberosCredentialsService.class)
        .required(false)
        .build();


    static final PropertyDescriptor KAFKA_KERBEROS_SERVICE_NAME = new PropertyDescriptor.Builder()
            .name("kafka-kerberos-service-name")
            .displayName("Kafka Kerberos Service Name")
            .description("The service name that matches the primary name of the Kafka server configured in the broker JAAS file." +
                    " This can be defined either in Kafka's JAAS config or in Kafka's config." +
                    " Corresponds to Kafka's 'security.protocol' property." +
                    " It is ignored unless one of the SASL options of the <Security Protocol> are selected.")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("kafka")
            .build();

    static final AllowableValue LINEAGE_STRATEGY_SIMPLE_PATH = new AllowableValue("SimplePath", "Simple Path",
            "Map NiFi provenance events and target Atlas DataSets to statically created 'nifi_flow_path' Atlas Processes." +
                    " See also 'Additional Details'.");
    static final AllowableValue LINEAGE_STRATEGY_COMPLETE_PATH = new AllowableValue("CompletePath", "Complete Path",
            "Create separate 'nifi_flow_path' Atlas Processes for each distinct input and output DataSet combinations" +
                    " by looking at the complete route for a given FlowFile. See also 'Additional Details.");

    static final PropertyDescriptor LINEAGE_STRATEGY = new PropertyDescriptor.Builder()
            .name("nifi-lineage-strategy")
            .displayName("Lineage Strategy")
            .description("Specifies granularity on how NiFi data flow should be reported to Atlas." +
                    " NOTE: It is strongly recommended to keep using the same strategy once this reporting task started to keep Atlas data clean." +
                    " Switching strategies will not delete Atlas entities created by the old strategy." +
                    " Having mixed entities created by different strategies makes Atlas lineage graph noisy." +
                    " For more detailed description on each strategy and differences, refer 'NiFi Lineage Strategy' section in Additional Details.")
            .required(true)
            .allowableValues(LINEAGE_STRATEGY_SIMPLE_PATH, LINEAGE_STRATEGY_COMPLETE_PATH)
            .defaultValue(LINEAGE_STRATEGY_SIMPLE_PATH.getValue())
            .build();

    static final AllowableValue AWS_S3_MODEL_VERSION_V1 = new AllowableValue("v1", "v1",
            "Creates AWS S3 directory entities version 1 (aws_s3_pseudo_dir).");
    static final AllowableValue AWS_S3_MODEL_VERSION_V2 = new AllowableValue(AtlasPathExtractorUtil.AWS_S3_ATLAS_MODEL_VERSION_V2, "v2",
            "Creates AWS S3 directory entities version 2 (aws_s3_v2_directory).");

    static final PropertyDescriptor AWS_S3_MODEL_VERSION = new PropertyDescriptor.Builder()
            .name("aws-s3-model-version")
            .displayName("AWS S3 Model Version")
            .description("Specifies what type of AWS S3 directory entities will be created in Atlas for s3a:// transit URIs (eg. PutHDFS with S3 integration)." +
                    " NOTE: It is strongly recommended to keep using the same AWS S3 entity model version once this reporting task started to keep Atlas data clean." +
                    " Switching versions will not delete existing Atlas entities created by the old version, nor migrate them to the new version.")
            .required(true)
            .allowableValues(AWS_S3_MODEL_VERSION_V1, AWS_S3_MODEL_VERSION_V2)
            .defaultValue(AWS_S3_MODEL_VERSION_V2.getValue())
            .build();

    static final AllowableValue FILESYSTEM_PATHS_LEVEL_FILE = new AllowableValue(FilesystemPathsLevel.FILE.name(), FilesystemPathsLevel.FILE.getDisplayName(),
            "Creates File level paths.");
    static final AllowableValue FILESYSTEM_PATHS_LEVEL_DIRECTORY = new AllowableValue(FilesystemPathsLevel.DIRECTORY.name(), FilesystemPathsLevel.DIRECTORY.getDisplayName(),
            "Creates Directory level paths.");

    static final PropertyDescriptor FILESYSTEM_PATHS_LEVEL = new PropertyDescriptor.Builder()
            .name("filesystem-paths-level")
            .displayName("Filesystem Path Entities Level")
            .description("Specifies how the filesystem path entities (fs_path and hdfs_path) will be logged in Atlas: File or Directory level. In case of File level, each individual file entity " +
                    "will be sent to Atlas as a separate entity with the full path including the filename. Directory level only logs the path of the parent directory without the filename. " +
                    "This setting affects processors working with files, like GetFile or PutHDFS. NOTE: Although the default value is File level for backward compatibility reasons, " +
                    "it is highly recommended to set it to Directory level because File level logging can generate a huge number of entities in Atlas.")
            .required(true)
            .allowableValues(FILESYSTEM_PATHS_LEVEL_FILE, FILESYSTEM_PATHS_LEVEL_DIRECTORY)
            .defaultValue(FILESYSTEM_PATHS_LEVEL_FILE.getValue())
            .build();

    private static final String ATLAS_PROPERTIES_FILENAME = "atlas-application.properties";
    private static final String ATLAS_PROPERTY_CLIENT_CONNECT_TIMEOUT_MS = "atlas.client.connectTimeoutMSecs";
    private static final String ATLAS_PROPERTY_CLIENT_READ_TIMEOUT_MS = "atlas.client.readTimeoutMSecs";
    private static final String ATLAS_PROPERTY_METADATA_NAMESPACE = "atlas.metadata.namespace";
    private static final String ATLAS_PROPERTY_CLUSTER_NAME = "atlas.cluster.name";
    private static final String ATLAS_PROPERTY_REST_ADDRESS = "atlas.rest.address";
    private static final String ATLAS_PROPERTY_ENABLE_TLS = SecurityProperties.TLS_ENABLED;
    private static final String ATLAS_KAFKA_PREFIX = "atlas.kafka.";
    private static final String ATLAS_PROPERTY_KAFKA_BOOTSTRAP_SERVERS = ATLAS_KAFKA_PREFIX + "bootstrap.servers";
    private static final String ATLAS_PROPERTY_KAFKA_CLIENT_ID = ATLAS_KAFKA_PREFIX + ProducerConfig.CLIENT_ID_CONFIG;

    private static final String SSL_CLIENT_XML_FILENAME = SecurityProperties.SSL_CLIENT_PROPERTIES;
    private static final String SSL_CLIENT_XML_TRUSTSTORE_LOCATION = "ssl.client.truststore.location";
    private static final String SSL_CLIENT_XML_TRUSTSTORE_PASSWORD = "ssl.client.truststore.password";
    private static final String SSL_CLIENT_XML_TRUSTSTORE_TYPE = "ssl.client.truststore.type";

    private final ServiceLoader<NamespaceResolver> namespaceResolverLoader = ServiceLoader.load(NamespaceResolver.class);
    private volatile AtlasAuthN atlasAuthN;
    private volatile Properties atlasProperties;
    private volatile boolean isTypeDefCreated = false;
    private volatile String defaultMetadataNamespace;

    private volatile ProvenanceEventConsumer consumer;
    private volatile NamespaceResolvers namespaceResolvers;
    private volatile NiFiAtlasHook nifiAtlasHook;
    private volatile LineageStrategy lineageStrategy;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        // Basic atlas config
        properties.add(ATLAS_URLS);
        properties.add(ATLAS_CONF_DIR);
        properties.add(ATLAS_CONF_CREATE);
        properties.add(ATLAS_DEFAULT_CLUSTER_NAME);

        // General config used by the processor
        properties.add(LINEAGE_STRATEGY);
        properties.add(PROVENANCE_START_POSITION);
        properties.add(PROVENANCE_BATCH_SIZE);
        properties.add(ATLAS_NIFI_URL);
        properties.add(ATLAS_AUTHN_METHOD);
        properties.add(ATLAS_USER);
        properties.add(ATLAS_PASSWORD);

        // Following properties are required if ATLAS_CONF_CREATE is enabled.
        // Otherwise should be left blank.
        // Will be used by the atlas client by reading the values from the atlas config file
        properties.add(KERBEROS_CREDENTIALS_SERVICE);
        properties.add(KERBEROS_PRINCIPAL);
        properties.add(KERBEROS_KEYTAB);
        properties.add(SSL_CONTEXT_SERVICE);
        properties.add(KAFKA_BOOTSTRAP_SERVERS);
        properties.add(KAFKA_SECURITY_PROTOCOL);
        properties.add(KAFKA_KERBEROS_SERVICE_NAME);
        properties.add(ATLAS_CONNECT_TIMEOUT);
        properties.add(ATLAS_READ_TIMEOUT);

        // Provenance event analyzer specific properties
        properties.add(AWS_S3_MODEL_VERSION);
        properties.add(FILESYSTEM_PATHS_LEVEL);

        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        for (NamespaceResolver resolver : namespaceResolverLoader) {
            final PropertyDescriptor propertyDescriptor = resolver.getSupportedDynamicPropertyDescriptor(propertyDescriptorName);
            if(propertyDescriptor != null) {
                return propertyDescriptor;
            }
        }
        return null;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final Set<String> schemes = new HashSet<>();
        String atlasUrls = context.getProperty(ATLAS_URLS).evaluateAttributeExpressions().getValue();
        if (!StringUtils.isEmpty(atlasUrls)) {
            Arrays.stream(atlasUrls.split(ATLAS_URL_DELIMITER))
                .map(String::trim)
                .forEach(input -> {
                    try {
                        final URL url = new URL(input);
                        schemes.add(url.toURI().getScheme());
                    } catch (Exception e) {
                        results.add(new ValidationResult.Builder().subject(ATLAS_URLS.getDisplayName()).input(input)
                                .explanation("contains invalid URI: " + e).valid(false).build());
                    }
                });
        }

        if (schemes.size() > 1) {
            results.add(new ValidationResult.Builder().subject(ATLAS_URLS.getDisplayName())
                    .explanation("URLs with multiple schemes have been specified").valid(false).build());
        }

        final String atlasAuthNMethod = context.getProperty(ATLAS_AUTHN_METHOD).getValue();
        final AtlasAuthN atlasAuthN = getAtlasAuthN(atlasAuthNMethod);
        results.addAll(atlasAuthN.validate(context));

        synchronized (namespaceResolverLoader) {
            // ServiceLoader is not thread-safe and customValidate() may be executed on multiple threads in parallel,
            // especially if the component has a property with dynamicallyModifiesClasspath(true)
            // and the component gets reloaded due to this when the property has been modified
            namespaceResolverLoader.forEach(resolver -> results.addAll(resolver.validate(context)));
        }

        if (context.getProperty(ATLAS_CONF_CREATE).asBoolean()) {

            Stream.of(ATLAS_URLS, ATLAS_CONF_DIR, ATLAS_DEFAULT_CLUSTER_NAME, KAFKA_BOOTSTRAP_SERVERS)
                    .filter(p -> !context.getProperty(p).isSet())
                    .forEach(p -> results.add(new ValidationResult.Builder()
                            .subject(p.getDisplayName())
                            .explanation("required to create Atlas configuration file.")
                            .valid(false).build()));

            validateKafkaProperties(context, results);
        }

        return results;
    }

    private void validateKafkaProperties(ValidationContext context, Collection<ValidationResult> results) {
        final String kafkaSecurityProtocol = context.getProperty(KAFKA_SECURITY_PROTOCOL).getValue();

        if ((SEC_SSL.equals(kafkaSecurityProtocol) || SEC_SASL_SSL.equals(kafkaSecurityProtocol))
                && !context.getProperty(SSL_CONTEXT_SERVICE).isSet()) {
            results.add(new ValidationResult.Builder()
                    .subject(SSL_CONTEXT_SERVICE.getDisplayName())
                    .explanation("required by SSL Kafka connection")
                    .valid(false)
                    .build());
        }

        if (SEC_SASL_PLAINTEXT.equals(kafkaSecurityProtocol) || SEC_SASL_SSL.equals(kafkaSecurityProtocol)) {
            final KerberosCredentialsService credentialsService = context.getProperty(ReportLineageToAtlas.KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

            if (credentialsService == null || context.getControllerServiceLookup().isControllerServiceEnabled(credentialsService)) {
                String principal;
                String keytab;
                if (credentialsService == null) {
                    principal = context.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue();
                    keytab = context.getProperty(KERBEROS_KEYTAB).evaluateAttributeExpressions().getValue();
                } else {
                    principal = credentialsService.getPrincipal();
                    keytab = credentialsService.getKeytab();
                }

                if (keytab == null || principal == null) {
                    results.add(new ValidationResult.Builder()
                            .subject("Kerberos Authentication")
                            .explanation("Keytab and Principal are required for Kerberos authentication with Apache Kafka.")
                            .valid(false)
                            .build());
                }
            }

            if (!context.getProperty(KAFKA_KERBEROS_SERVICE_NAME).isSet()) {
                results.add(new ValidationResult.Builder()
                        .subject(KAFKA_KERBEROS_SERVICE_NAME.getDisplayName())
                        .explanation("Required by Kafka SASL authentication.")
                        .valid(false)
                        .build());
            }
        }
    }

    @OnScheduled
    public void setup(ConfigurationContext context) throws Exception {
        // initAtlasClient has to be done first as it loads AtlasProperty.
        initAtlasProperties(context);
        initLineageStrategy(context);
        initNamespaceResolvers(context);
    }

    private void initLineageStrategy(ConfigurationContext context) throws IOException {
        nifiAtlasHook = new NiFiAtlasHook();

        final String strategy = context.getProperty(LINEAGE_STRATEGY).getValue();
        if (LINEAGE_STRATEGY_SIMPLE_PATH.equals(strategy)) {
            lineageStrategy = new SimpleFlowPathLineage();
        } else if (LINEAGE_STRATEGY_COMPLETE_PATH.equals(strategy)) {
            lineageStrategy = new CompleteFlowPathLineage();
        }

        lineageStrategy.setLineageContext(nifiAtlasHook);
        initProvenanceConsumer(context);
    }

    private void initNamespaceResolvers(ConfigurationContext context) {
        final Set<NamespaceResolver> loadedNamespaceResolvers = new LinkedHashSet<>();
        namespaceResolverLoader.forEach(resolver -> {
            resolver.configure(context);
            loadedNamespaceResolvers.add(resolver);
        });
        namespaceResolvers = new NamespaceResolvers(Collections.unmodifiableSet(loadedNamespaceResolvers), defaultMetadataNamespace);
    }


    private void initAtlasProperties(ConfigurationContext context) throws Exception {
        final String atlasAuthNMethod = context.getProperty(ATLAS_AUTHN_METHOD).getValue();

        final String confDirStr = context.getProperty(ATLAS_CONF_DIR).evaluateAttributeExpressions().getValue();
        final File confDir = confDirStr != null && !confDirStr.isEmpty() ? new File(confDirStr) : null;

        atlasProperties = new Properties();
        final File atlasPropertiesFile = new File(confDir, ATLAS_PROPERTIES_FILENAME);

        final Boolean createAtlasConf = context.getProperty(ATLAS_CONF_CREATE).asBoolean();
        if (!createAtlasConf) {
            // Load existing properties file.
            if (atlasPropertiesFile.isFile()) {
                getLogger().info("Loading {}", new Object[]{atlasPropertiesFile});
                try (InputStream in = new FileInputStream(atlasPropertiesFile)) {
                    atlasProperties.load(in);
                }
            } else {
                final String fileInClasspath = "/" + ATLAS_PROPERTIES_FILENAME;
                try (InputStream in = ReportLineageToAtlas.class.getResourceAsStream(fileInClasspath)) {
                    getLogger().info("Loading {} from classpath", new Object[]{fileInClasspath});
                    if (in == null) {
                        throw new ProcessException(String.format("Could not find %s in classpath." +
                                " Please add it to classpath," +
                                " or specify %s a directory containing Atlas properties file," +
                                " or enable %s to generate it.",
                                fileInClasspath, ATLAS_CONF_DIR.getDisplayName(), ATLAS_CONF_CREATE.getDisplayName()));
                    }
                    atlasProperties.load(in);
                }
            }
        }

        List<String> urls = parseAtlasUrls(context.getProperty(ATLAS_URLS));

        setValue(
            value -> defaultMetadataNamespace = value,
            () -> {
                throw new ProcessException("Default metadata namespace (or cluster name) is not defined.");
            },
            context.getProperty(ATLAS_DEFAULT_CLUSTER_NAME),
            atlasProperties.getProperty(ATLAS_PROPERTY_METADATA_NAMESPACE),
            atlasProperties.getProperty(ATLAS_PROPERTY_CLUSTER_NAME)
        );

        String atlasConnectTimeoutMs = context.getProperty(ATLAS_CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue() + "";
        String atlasReadTimeoutMs = context.getProperty(ATLAS_READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue() + "";

        atlasAuthN = getAtlasAuthN(atlasAuthNMethod);
        atlasAuthN.configure(context);

        // Create Atlas configuration file if necessary.
        if (createAtlasConf) {
            // enforce synchronous notification sending (needed for the checkpointing in ProvenanceEventConsumer)
            atlasProperties.setProperty(AtlasHook.ATLAS_NOTIFICATION_ASYNCHRONOUS, "false");

            atlasProperties.put(ATLAS_PROPERTY_REST_ADDRESS, urls.stream().collect(Collectors.joining(ATLAS_URL_DELIMITER)));
            atlasProperties.put(ATLAS_PROPERTY_CLIENT_CONNECT_TIMEOUT_MS, atlasConnectTimeoutMs);
            atlasProperties.put(ATLAS_PROPERTY_CLIENT_READ_TIMEOUT_MS, atlasReadTimeoutMs);
            atlasProperties.put(ATLAS_PROPERTY_METADATA_NAMESPACE, defaultMetadataNamespace);
            atlasProperties.put(ATLAS_PROPERTY_CLUSTER_NAME, defaultMetadataNamespace);

            setAtlasSSLConfig(atlasProperties, context, urls, confDir);

            setKafkaConfig(atlasProperties, context);

            atlasAuthN.populateProperties(atlasProperties);

            try (FileOutputStream fos = new FileOutputStream(atlasPropertiesFile)) {
                String ts = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX")
                        .withZone(ZoneOffset.UTC)
                        .format(Instant.now());
                atlasProperties.store(fos, "Generated by Apache NiFi ReportLineageToAtlas ReportingTask at " + ts);
            }
        } else {
            // check if synchronous notification sending has been set (needed for the checkpointing in ProvenanceEventConsumer)
            String isAsync = atlasProperties.getProperty(AtlasHook.ATLAS_NOTIFICATION_ASYNCHRONOUS);
            if (isAsync == null || !isAsync.equalsIgnoreCase("false")) {
                throw new ProcessException("Atlas property '" + AtlasHook.ATLAS_NOTIFICATION_ASYNCHRONOUS + "' must be set to 'false' in " + ATLAS_PROPERTIES_FILENAME + "." +
                        " Sending notifications asynchronously is not supported by the reporting task.");
            }
        }

        getLogger().debug("Force reloading Atlas application properties.");
        ApplicationProperties.forceReload();

        if (confDir != null) {
            // If atlasConfDir is not set, atlas-application.properties will be searched under classpath.
            Properties props = System.getProperties();
            final String atlasConfProp = ApplicationProperties.ATLAS_CONFIGURATION_DIRECTORY_PROPERTY;
            props.setProperty(atlasConfProp, confDir.getAbsolutePath());
            getLogger().debug("{} has been set to: {}", new Object[]{atlasConfProp, props.getProperty(atlasConfProp)});
        }
    }

    private List<String> parseAtlasUrls(final PropertyValue atlasUrlsProp) {
        List<String> atlasUrls = new ArrayList<>();

        setValue(
            value -> Arrays.stream(value.split(ATLAS_URL_DELIMITER))
                .map(String::trim)
                .forEach(urlString -> {
                        try {
                            new URL(urlString);
                        } catch (Exception e) {
                            throw new ProcessException(e);
                        }
                        atlasUrls.add(urlString);
                    }
                ),
            () -> {
                throw new ProcessException("No Atlas URL has been specified! Set either the '" + ATLAS_URLS.getDisplayName() + "' " +
                    "property on the processor or the 'atlas.rest.address' property in the atlas configuration file.");
            },
            atlasUrlsProp,
            atlasProperties.getProperty(ATLAS_PROPERTY_REST_ADDRESS)
        );

        return atlasUrls;
    }

    private void setValue(Consumer<String> setter, Runnable emptyHandler, PropertyValue elEnabledPropertyValue, String... properties) {
        StringSelector valueSelector = StringSelector
            .of(elEnabledPropertyValue.evaluateAttributeExpressions().getValue())
            .or(properties);

        if (valueSelector.found()) {
            setter.accept(valueSelector.toString());
        } else {
            emptyHandler.run();
        }
    }

    private void setAtlasSSLConfig(Properties atlasProperties, ConfigurationContext context, List<String> urls, File confDir) throws Exception {
        boolean isAtlasApiSecure = urls.stream().anyMatch(url -> url.toLowerCase().startsWith("https"));
        atlasProperties.put(ATLAS_PROPERTY_ENABLE_TLS, String.valueOf(isAtlasApiSecure));

        deleteSslClientXml(confDir);

        if (isAtlasApiSecure) {
            SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            if (sslContextService == null) {
                getLogger().warn("No SSLContextService configured, the system default truststore will be used.");
            } else if (!sslContextService.isTrustStoreConfigured()) {
                getLogger().warn("No truststore configured on SSLContextService, the system default truststore will be used.");
            } else {
                // create ssl-client.xml config file for Hadoop Security used by Atlas REST client,
                // Atlas would generate this file with hardcoded JKS keystore type,
                // in order to support other keystore types, we generate it ourselves
                createSslClientXml(confDir, sslContextService);
            }
        }
    }

    private void deleteSslClientXml(File confDir) throws Exception {
        Path sslClientXmlPath = new File(confDir, SSL_CLIENT_XML_FILENAME).toPath();
        try {
            Files.deleteIfExists(sslClientXmlPath);
        } catch (Exception e) {
            getLogger().error("Unable to delete SSL Client Configuration File {}", sslClientXmlPath, e);
            throw e;
        }
    }

    private void createSslClientXml(File confDir, SSLContextService sslContextService) throws Exception {
        File sslClientXmlFile = new File(confDir, SSL_CLIENT_XML_FILENAME);

        Configuration configuration = new Configuration(false);

        configuration.set(SSL_CLIENT_XML_TRUSTSTORE_LOCATION, sslContextService.getTrustStoreFile());
        configuration.set(SSL_CLIENT_XML_TRUSTSTORE_PASSWORD, sslContextService.getTrustStorePassword());
        configuration.set(SSL_CLIENT_XML_TRUSTSTORE_TYPE, sslContextService.getTrustStoreType());

        try (FileWriter fileWriter = new FileWriter(sslClientXmlFile)) {
            configuration.writeXml(fileWriter);
        } catch (Exception e) {
            getLogger().error("Unable to create SSL Client Configuration File {}", sslClientXmlFile, e);
            throw e;
        }
    }

    /**
     * In order to avoid authentication expiration issues (i.e. Kerberos ticket and DelegationToken expiration),
     * create Atlas client instance at every onTrigger execution.
     */
    protected NiFiAtlasClient createNiFiAtlasClient(ReportingContext context) {
        List<String> urls = parseAtlasUrls(context.getProperty(ATLAS_URLS));
        try {
            return new NiFiAtlasClient(atlasAuthN.createClient(urls.toArray(new String[]{})));
        } catch (final NullPointerException e) {
            throw new ProcessException(String.format("Failed to initialize Atlas client due to %s." +
                    " Make sure 'atlas-application.properties' is in the directory specified with %s" +
                    " or under root classpath if not specified.", e, ATLAS_CONF_DIR.getDisplayName()), e);
        }
    }

    private AtlasAuthN getAtlasAuthN(String atlasAuthNMethod) {
        final AtlasAuthN atlasAuthN;
        switch (atlasAuthNMethod) {
            case "basic" :
                atlasAuthN = new Basic();
                break;
            case "kerberos" :
                atlasAuthN = new Kerberos();
                break;
            default:
                throw new IllegalArgumentException(atlasAuthNMethod + " is not supported as an Atlas authentication method.");
        }
        return atlasAuthN;
    }

    private void initProvenanceConsumer(final ConfigurationContext context) throws IOException {
        consumer = new ProvenanceEventConsumer();
        consumer.setStartPositionValue(context.getProperty(PROVENANCE_START_POSITION).getValue());
        consumer.setBatchSize(context.getProperty(PROVENANCE_BATCH_SIZE).asInteger());
        consumer.addTargetEventType(lineageStrategy.getTargetEventTypes());
        consumer.setLogger(getLogger());
        consumer.setScheduled(true);
    }

    @OnUnscheduled
    public void onUnscheduled() {
        if (consumer != null) {
            // Tell provenance consumer to stop pulling more provenance events.
            // This should be called from @OnUnscheduled to stop the loop in the thread called from onTrigger.
            consumer.setScheduled(false);
        }
    }

    @OnStopped
    public void onStopped() {
        if (nifiAtlasHook != null) {
            nifiAtlasHook.close();
            nifiAtlasHook = null;
        }
    }

    @Override
    public void onTrigger(ReportingContext context) {

        final String clusterNodeId = context.getClusterNodeIdentifier();
        final boolean isClustered = context.isClustered();
        if (isClustered && isEmpty(clusterNodeId)) {
            // Clustered, but this node's ID is unknown. Not ready for processing yet.
            return;
        }

        // If standalone or being primary node in a NiFi cluster, this node is responsible for doing primary tasks.
        final boolean isResponsibleForPrimaryTasks = !isClustered || getNodeTypeProvider().isPrimary();

        try (final NiFiAtlasClient atlasClient = createNiFiAtlasClient(context)) {

            // Create Entity defs in Atlas if there's none yet.
            if (!isTypeDefCreated) {
                try {
                    if (isResponsibleForPrimaryTasks) {
                        // Create NiFi type definitions in Atlas type system.
                        atlasClient.registerNiFiTypeDefs(false);
                    } else {
                        // Otherwise, just check existence of NiFi type definitions.
                        if (!atlasClient.isNiFiTypeDefsRegistered()) {
                            getLogger().debug("NiFi type definitions are not ready in Atlas type system yet.");
                            return;
                        }
                    }
                    isTypeDefCreated = true;
                } catch (AtlasServiceException e) {
                    throw new RuntimeException("Failed to check and create NiFi flow type definitions in Atlas due to " + e, e);
                }
            }

            // Regardless of whether being a primary task node, each node has to analyse NiFiFlow.
            // Assuming each node has the same flow definition, that is guaranteed by NiFi cluster management mechanism.
            final NiFiFlow nifiFlow = createNiFiFlow(context, atlasClient);


            if (isResponsibleForPrimaryTasks) {
                try {
                    atlasClient.registerNiFiFlow(nifiFlow);
                } catch (AtlasServiceException e) {
                    throw new RuntimeException("Failed to register NiFI flow. " + e, e);
                }
            }

            // NOTE: There is a race condition between the primary node and other nodes.
            // If a node notifies an event related to a NiFi component which is not yet created by NiFi primary node,
            // then the notification message will fail due to having a reference to a non-existing entity.
            nifiAtlasHook.setAtlasClient(atlasClient);
            consumeNiFiProvenanceEvents(context, nifiFlow);
        }
    }

    private NiFiFlow createNiFiFlow(ReportingContext context, NiFiAtlasClient atlasClient) {
        final ProcessGroupStatus rootProcessGroup = context.getEventAccess().getGroupStatus("root");
        final String flowName = rootProcessGroup.getName();
        final String nifiUrl = context.getProperty(ATLAS_NIFI_URL).evaluateAttributeExpressions().getValue();


        final String namespace;
        try {
            final String nifiHostName = new URL(nifiUrl).getHost();
            namespace = namespaceResolvers.fromHostNames(nifiHostName);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Failed to parse NiFi URL, " + e.getMessage(), e);
        }

        NiFiFlow existingNiFiFlow = null;
        try {
            // Retrieve Existing NiFiFlow from Atlas.
            existingNiFiFlow = atlasClient.fetchNiFiFlow(rootProcessGroup.getId(), namespace);
        } catch (AtlasServiceException e) {
            if (ClientResponse.Status.NOT_FOUND.equals(e.getStatus())){
                getLogger().debug("Existing flow was not found for {}@{}", new Object[]{rootProcessGroup.getId(), namespace});
            } else {
                throw new RuntimeException("Failed to fetch existing NiFI flow. " + e, e);
            }
        }

        final NiFiFlow nifiFlow = existingNiFiFlow != null ? existingNiFiFlow : new NiFiFlow(rootProcessGroup.getId());
        nifiFlow.setFlowName(flowName);
        nifiFlow.setUrl(nifiUrl);
        nifiFlow.setNamespace(namespace);

        final NiFiFlowAnalyzer flowAnalyzer = new NiFiFlowAnalyzer();

        flowAnalyzer.analyzeProcessGroup(nifiFlow, rootProcessGroup);
        flowAnalyzer.analyzePaths(nifiFlow);

        return nifiFlow;
    }

    private void consumeNiFiProvenanceEvents(ReportingContext context, NiFiFlow nifiFlow) {
        final EventAccess eventAccess = context.getEventAccess();
        final String awsS3ModelVersion = context.getProperty(AWS_S3_MODEL_VERSION).getValue();
        final FilesystemPathsLevel filesystemPathsLevel = FilesystemPathsLevel.valueOf(context.getProperty(FILESYSTEM_PATHS_LEVEL).getValue());
        final AnalysisContext analysisContext = new StandardAnalysisContext(nifiFlow, namespaceResolvers,
                // FIXME: This class cast shouldn't be necessary to query lineage. Possible refactor target in next major update.
                (ProvenanceRepository)eventAccess.getProvenanceRepository(), awsS3ModelVersion, filesystemPathsLevel);
        consumer.consumeEvents(context, (componentMapHolder, events) -> {
            for (ProvenanceEventRecord event : events) {
                try {
                    lineageStrategy.processEvent(analysisContext, nifiFlow, event);
                } catch (Exception e) {
                    // If something went wrong, log it and continue with other records.
                    getLogger().error("Skipping failed analyzing event {} due to {}.", new Object[]{event, e, e});
                }
            }
            nifiAtlasHook.commitMessages();
        });
    }

    private void setKafkaConfig(Map<Object, Object> mapToPopulate, PropertyContext context) {

        final String kafkaBootStrapServers = context.getProperty(KAFKA_BOOTSTRAP_SERVERS).evaluateAttributeExpressions().getValue();
        mapToPopulate.put(ATLAS_PROPERTY_KAFKA_BOOTSTRAP_SERVERS, kafkaBootStrapServers);
        mapToPopulate.put(ATLAS_PROPERTY_KAFKA_CLIENT_ID, String.format("%s.%s", getName(), getIdentifier()));

        final String kafkaSecurityProtocol = context.getProperty(KAFKA_SECURITY_PROTOCOL).getValue();
        mapToPopulate.put(ATLAS_KAFKA_PREFIX + "security.protocol", kafkaSecurityProtocol);

        // Translate SSLContext Service configuration into Kafka properties
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService != null && sslContextService.isKeyStoreConfigured()) {
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslContextService.getKeyStoreFile());
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslContextService.getKeyStorePassword());
            final String keyPass = sslContextService.getKeyPassword() == null ? sslContextService.getKeyStorePassword() : sslContextService.getKeyPassword();
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyPass);
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, sslContextService.getKeyStoreType());
        }

        if (sslContextService != null && sslContextService.isTrustStoreConfigured()) {
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslContextService.getTrustStoreFile());
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslContextService.getTrustStorePassword());
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, sslContextService.getTrustStoreType());
        }

        if (SEC_SASL_PLAINTEXT.equals(kafkaSecurityProtocol) || SEC_SASL_SSL.equals(kafkaSecurityProtocol)) {
            setKafkaJaasConfig(mapToPopulate, context);
        }

    }

    /**
     * Populate Kafka JAAS properties for Atlas notification.
     * Since Atlas 0.8.1 uses Kafka client 0.10.0.0, we can not use 'sasl.jaas.config' property
     * as it is available since 0.10.2, implemented by KAFKA-4259.
     * Instead, this method uses old property names.
     * @param mapToPopulate Map of configuration properties
     * @param context Context
     */
    private void setKafkaJaasConfig(Map<Object, Object> mapToPopulate, PropertyContext context) {
        String keytab;
        String principal;
        final String explicitPrincipal = context.getProperty(KERBEROS_PRINCIPAL).evaluateAttributeExpressions().getValue();
        final String explicitKeytab = context.getProperty(KERBEROS_KEYTAB).evaluateAttributeExpressions().getValue();

        final KerberosCredentialsService credentialsService = context.getProperty(ReportLineageToAtlas.KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        if (credentialsService == null) {
            principal = explicitPrincipal;
            keytab = explicitKeytab;
        } else {
            principal = credentialsService.getPrincipal();
            keytab = credentialsService.getKeytab();
        }

        String serviceName = context.getProperty(KAFKA_KERBEROS_SERVICE_NAME).evaluateAttributeExpressions().getValue();
        if(StringUtils.isNotBlank(keytab) && StringUtils.isNotBlank(principal) && StringUtils.isNotBlank(serviceName)) {
            mapToPopulate.put("atlas.jaas.KafkaClient.loginModuleControlFlag", "required");
            mapToPopulate.put("atlas.jaas.KafkaClient.loginModuleName", "com.sun.security.auth.module.Krb5LoginModule");
            mapToPopulate.put("atlas.jaas.KafkaClient.option.keyTab", keytab);
            mapToPopulate.put("atlas.jaas.KafkaClient.option.principal", principal);
            mapToPopulate.put("atlas.jaas.KafkaClient.option.serviceName", serviceName);
            mapToPopulate.put("atlas.jaas.KafkaClient.option.storeKey", "True");
            mapToPopulate.put("atlas.jaas.KafkaClient.option.useKeyTab", "True");
            mapToPopulate.put("atlas.jaas.ticketBased-KafkaClient.loginModuleControlFlag", "required");
            mapToPopulate.put("atlas.jaas.ticketBased-KafkaClient.loginModuleName", "com.sun.security.auth.module.Krb5LoginModule");
            mapToPopulate.put("atlas.jaas.ticketBased-KafkaClient.option.useTicketCache", "true");
            mapToPopulate.put(ATLAS_KAFKA_PREFIX + "sasl.kerberos.service.name", serviceName);
        }
    }

}
