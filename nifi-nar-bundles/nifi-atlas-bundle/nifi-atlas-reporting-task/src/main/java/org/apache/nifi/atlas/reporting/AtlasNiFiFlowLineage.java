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

import org.apache.atlas.AtlasServiceException;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.atlas.AtlasVariables;
import org.apache.nifi.atlas.NiFiApiClient;
import org.apache.nifi.atlas.NiFiAtlasClient;
import org.apache.nifi.atlas.NiFiFlow;
import org.apache.nifi.atlas.NiFiFlowAnalyzer;
import org.apache.nifi.atlas.NiFiFlowPath;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.web.api.entity.ClusterEntity;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.commons.lang3.StringUtils.isEmpty;

@Tags({"atlas", "lineage"})
@CapabilityDescription("Publishes NiFi flow data set level lineage to Apache Atlas." +
        " By reporting flow information to Atlas, an end-to-end Process and DataSet lineage such as across NiFi environments and other systems" +
        " connected by technologies, for example NiFi Site-to-Site, Kafka topic or Hive tables." +
        " There are limitations and required configurations for both NiFi and Atlas. See 'Additional Details' for further description.")
public class AtlasNiFiFlowLineage extends AbstractReportingTask {

    static final PropertyDescriptor ATLAS_URLS = new PropertyDescriptor.Builder()
            .name("atlas-urls")
            .displayName("Atlas URLs")
            .description("Comma separated URLs of the Atlas Server (e.g. http://atlas-server-hostname:21000).")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ATLAS_USER = new PropertyDescriptor.Builder()
            .name("atlas-username")
            .displayName("Atlas Username")
            .description("User name to communicate with Atlas.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ATLAS_PASSWORD = new PropertyDescriptor.Builder()
            .name("atlas-password")
            .displayName("Atlas Password")
            .description("Password to communicate with Atlas.")
            .required(true)
            .sensitive(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ATLAS_CONF_DIR = new PropertyDescriptor.Builder()
            .name("atlas-conf-dir")
            .displayName("Atlas Configuration Directory")
            .description("Directory path that contains 'atlas-application.properties' file." +
                    " If not specified, 'atlas-application.properties' file under root classpath is used.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor ATLAS_NIFI_URL = new PropertyDescriptor.Builder()
            .name("atlas-nifi-url")
            .displayName("NiFi URL for Atlas")
            .description("NiFi URL is used in Atlas to represent this NiFi cluster (or standalone instance)." +
                    " It is recommended to use one that can be accessible remotely instead of using 'localhost'.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    static final PropertyDescriptor LOCAL_HOSTNAME = new PropertyDescriptor.Builder()
            .name("local-address")
            .displayName("Local Hostname")
            .description("This reporting task uses NiFi REST API against itself to retrieve NiFI flow data." +
                    " This hostname is used when making HTTP(s) requests to the REST API.")
            .required(true)
            .defaultValue("localhost")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor NIFI_API_PORT = new PropertyDescriptor.Builder()
            .name("nifi-api-port")
            .displayName("NiFi API Port")
            .description("Same as 'Local Hostname', this port number is used to specify a port number of this NiFi instance.")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("8080")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor NIFI_API_SECURE = new PropertyDescriptor.Builder()
            .name("nifi-api-secure")
            .displayName("NiFi API Secure")
            .description("Specify if this NiFi instance is secured and requires HTTPS. If true, 'SSL Context Service' needs to be set, too." +
                    " Also, NiFi security policy should be configured for this NiFi instance to read certain resources. See 'Additional Details' for further description.")
            .required(true)
            .expressionLanguageSupported(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service to use when communicating with a secured NiFi node.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    private static final String ATLAS_PROPERTIES_FILENAME = "atlas-application.properties";
    private volatile NiFiAtlasClient atlasClient;
    private volatile Properties atlasProperties;
    private volatile boolean isTypeDefCreated = false;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATLAS_URLS);
        properties.add(ATLAS_USER);
        properties.add(ATLAS_PASSWORD);
        properties.add(ATLAS_CONF_DIR);
        properties.add(ATLAS_NIFI_URL);
        properties.add(LOCAL_HOSTNAME);
        properties.add(NIFI_API_PORT);
        properties.add(NIFI_API_SECURE);
        properties.add(SSL_CONTEXT);
        return properties;
    }

    private void parseAtlasUrls(final PropertyValue atlasUrlsProp, final Consumer<String> urlStrConsumer) {
        final String atlasUrlsStr = atlasUrlsProp.evaluateAttributeExpressions().getValue();
        if (atlasUrlsStr != null && !atlasUrlsStr.isEmpty()) {
            Arrays.stream(atlasUrlsStr.split(","))
                    .map(s -> s.trim())
                    .forEach(input -> urlStrConsumer.accept(input));
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new ArrayList<>();

        parseAtlasUrls(validationContext.getProperty(ATLAS_URLS), input -> {
            final ValidationResult.Builder builder = new ValidationResult.Builder().subject(ATLAS_URLS.getDisplayName()).input(input);
            try {
                new URL(input);
                results.add(builder.explanation("Valid URI").valid(true).build());
            } catch (Exception e) {
                results.add(builder.explanation("Contains invalid URI").valid(false).build());
            }
        });

        return results;
    }

    @OnScheduled
    public void initAtlasClient(ConfigurationContext context) throws IOException {
        List<String> urls = new ArrayList<>();
        parseAtlasUrls(context.getProperty(ATLAS_URLS), url -> urls.add(url));

        final String user = context.getProperty(ATLAS_USER).getValue();
        final String password = context.getProperty(ATLAS_PASSWORD).getValue();
        final String confDirStr = context.getProperty(ATLAS_CONF_DIR).getValue();
        final File confDir = confDirStr != null && !confDirStr.isEmpty() ? new File(confDirStr) : null;

        atlasProperties = new Properties();
        final File atlasPropertiesFile = new File(confDir, ATLAS_PROPERTIES_FILENAME);
        if (atlasPropertiesFile.isFile()) {
            getLogger().info("Loading {}", new Object[]{confDir});
            try (InputStream in = new FileInputStream(atlasPropertiesFile)) {
                atlasProperties.load(in);
            }
        } else {
            final String fileInClasspath = "/" + ATLAS_PROPERTIES_FILENAME;
            try (InputStream in = AtlasNiFiFlowLineage.class.getResourceAsStream(fileInClasspath)) {
                getLogger().info("Loading {} from classpath", new Object[]{fileInClasspath});
                atlasProperties.load(in);
            }
        }

        atlasClient = NiFiAtlasClient.getInstance();
        try {
            atlasClient.initialize(true, urls.toArray(new String[]{}), user, password, confDir);
        } catch (final NullPointerException e) {
            throw new ProcessException(String.format("Failed to initialize Atlas client due to %s." +
                    " Make sure 'atlas-application.properties' is in the directory specified with %s" +
                    " or under root classpath if not specified.", e, ATLAS_CONF_DIR.getDisplayName()), e);
        }

    }

    @Override
    public void onTrigger(ReportingContext context) {

        final Boolean isNiFiApiSecure = context.getProperty(NIFI_API_SECURE).evaluateAttributeExpressions().asBoolean();
        final Integer nifiApiPort = context.getProperty(NIFI_API_PORT).evaluateAttributeExpressions().asInteger();
        final String localhost = context.getProperty(LOCAL_HOSTNAME).evaluateAttributeExpressions().getValue();
        final String nifiBaseUrl = (isNiFiApiSecure ? "https" : "http") + "://" + localhost + ":" + nifiApiPort + "/";
        final NiFiApiClient nifiClient = new NiFiApiClient(nifiBaseUrl);

        if (isNiFiApiSecure) {
            final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
            final SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
            nifiClient.setSslContext(sslContext);
        }

        final String clusterNodeId = context.getClusterNodeIdentifier();
        if (context.isClustered()) {
            if (isEmpty(clusterNodeId)) {
                // Clustered, but this node's ID is unknown. Not ready for processing yet.
                return;
            }

            try {
                final ClusterEntity clusterEntity = nifiClient.getClusterEntity();
                if (clusterEntity.getCluster().getNodes().stream()
                        .noneMatch(node -> clusterNodeId.equals(node.getNodeId())
                                && node.getRoles().contains("Primary Node"))) {
                    // In a cluster, only primary node can report to Atlas.
                    // TODO: This should be done by NiFi scheduler like processor. But not supported at this moment.
                    return;
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to get cluster entity, due to " + e, e);
            }
        }

        // Create Entity defs in Atlas if there's none yet.
        if (!isTypeDefCreated) {
            try {
                atlasClient.registerNiFiTypeDefs(false);
                isTypeDefCreated = true;
            } catch (AtlasServiceException e) {
                throw new RuntimeException("Failed to check and create NiFi flow type definitions in Atlas due to " + e, e);
            }
        }

        final NiFiFlowAnalyzer flowAnalyzer = new NiFiFlowAnalyzer(nifiClient);

        final NiFiFlow niFiFlow;
        try {
            final AtlasVariables atlasVariables = new AtlasVariables();
            atlasVariables.setNifiUrl(context.getProperty(ATLAS_NIFI_URL).evaluateAttributeExpressions().getValue());
            atlasVariables.setAtlasProperties(atlasProperties);
            niFiFlow = flowAnalyzer.analyzeProcessGroup(atlasVariables);
        } catch (IOException e) {
            throw new RuntimeException("Failed to analyze NiFi flow. " + e, e);
        }

        try {
            final List<NiFiFlowPath> niFiFlowPaths = flowAnalyzer.analyzePaths(niFiFlow);
            atlasClient.registerNiFiFlow(niFiFlow, niFiFlowPaths);
        } catch (AtlasServiceException e) {
            throw new RuntimeException("Failed to register NiFI flow. " + e, e);
        }
    }


}
