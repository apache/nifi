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
package org.apache.nifi.registry.web.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.PostConstruct;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * A base class to simplify creating integration tests against an API application running with an embedded server and volatile DB.
 */
public abstract class IntegrationTestBase {

    private static final String H2 = "h2";
    private static final String MYSQL = "mysql";
    private static final String MARIADB = "mariadb";
    private static final String POSTGRES = "postgres";
    private static final String ORACLE = "oracle";

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        final String activeProfiles = System.getProperty("spring.profiles.active", H2);
        registry.add("current.database.is.h2", () -> activeProfiles.contains(H2));
        registry.add("current.database.is.mysql", () -> activeProfiles.contains(MYSQL));
        registry.add("current.database.is.mariadb", () -> activeProfiles.contains(MARIADB));
        registry.add("current.database.is.postgres", () -> activeProfiles.contains(POSTGRES));
        registry.add("current.database.is.oracle", () -> activeProfiles.contains(ORACLE));
        registry.add("current.database.is.not.h2", () -> !activeProfiles.contains(H2));
        registry.add("current.database.is.not.mysql", () -> !activeProfiles.contains(MYSQL));
        registry.add("current.database.is.not.mariadb", () -> !activeProfiles.contains(MARIADB));
        registry.add("current.database.is.not.postgres", () -> !activeProfiles.contains(POSTGRES));
        registry.add("current.database.is.not.oracle", () -> !activeProfiles.contains(ORACLE));
    }

    private static final String CONTEXT_PATH = "/nifi-registry-api";

    @TestConfiguration
    public static class TestConfigurationClass {

        /* REQUIRED: Any subclass extending IntegrationTestBase must add a Spring profile that defines a
         * property value for this key containing the path to the nifi-registy.properties file to use to
         * create a NiFiRegistryProperties Bean in the ApplicationContext.  */
        @Value("${nifi.registry.properties.file}")
        private String propertiesFileLocation;

        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        private final Lock readLock = lock.readLock();
        private NiFiRegistryProperties testProperties;

        @Bean
        public JettyServletWebServerFactory jettyEmbeddedServletContainerFactory() {
            JettyServletWebServerFactory jettyContainerFactory = new JettyServletWebServerFactory();
            jettyContainerFactory.setContextPath(CONTEXT_PATH);
            return jettyContainerFactory;
        }

        @Bean
        public NiFiRegistryProperties getNiFiRegistryProperties() {
            readLock.lock();
            try {
                if (testProperties == null) {
                    testProperties = loadNiFiRegistryProperties(propertiesFileLocation);
                }
            } finally {
                readLock.unlock();
            }
            return testProperties;
        }

    }

    @Autowired
    private NiFiRegistryProperties properties;

    /* OPTIONAL: Any subclass that extends this base class MAY provide or specify a @TestConfiguration that provides a
     * NiFiRegistryClientConfig @Bean. The properties specified should correspond with the integration test cases in
     * the concrete subclass. See SecureFileIT for an example. */
    @Autowired(required = false)
    private NiFiRegistryClientConfig clientConfig;

    /* This will be injected with the random port assigned to the embedded Jetty container. */
    @LocalServerPort
    private int port;

    /**
     * Subclasses can access this auto-configured JAX-RS client to communicate to the NiFi Registry Server
     */
    protected Client client;

    @PostConstruct
    void initialize() {
        if (this.clientConfig != null) {
            this.client = createClientFromConfig(this.clientConfig);
        } else {
            this.client = ClientBuilder.newClient();
        }

    }

    /**
     * Subclasses can utilize this method to build a URL that has the correct protocol, hostname, and port
     * for a given path.
     *
     * @param relativeResourcePath the path component of the resource you wish to access, relative to the
     *                             base API URL, where the base includes the servlet context path.
     *
     * @return a String containing the absolute URL of the resource.
     */
    String createURL(String relativeResourcePath) {
        if (relativeResourcePath == null) {
            throw new IllegalArgumentException("Resource path cannot be null");
        }

        final StringBuilder baseUriBuilder = new StringBuilder(createBaseURL()).append(CONTEXT_PATH);

        if (!relativeResourcePath.startsWith("/")) {
            baseUriBuilder.append('/');
        }
        baseUriBuilder.append(relativeResourcePath);

        return baseUriBuilder.toString();
    }

    /**
     * Sub-classes can utilize this method to obtain the base-url for a client.
     *
     * @return a string containing the base url which includes the scheme, host, and port
     */
    public String createBaseURL() {
        final boolean isSecure = this.properties.getSslPort() != null;
        final String protocolSchema = isSecure ? "https" : "http";
        return protocolSchema + "://localhost:" + port;
    }

    NiFiRegistryClientConfig createClientConfig(String baseUrl) {
        final NiFiRegistryClientConfig.Builder builder = new NiFiRegistryClientConfig.Builder();
        builder.baseUrl(baseUrl);

        if (this.clientConfig != null) {
            if (this.clientConfig.getSslContext() != null) {
                builder.sslContext(this.clientConfig.getSslContext());
            }

            if (this.clientConfig.getHostnameVerifier() != null) {
                builder.hostnameVerifier(this.clientConfig.getHostnameVerifier());
            }
        }

        return builder.build();
    }

    /**
     * A helper method for loading NiFiRegistryProperties by reading *.properties files from disk.
     *
     * @param propertiesFilePath The location of the properties file
     * @return A NiFIRegistryProperties instance based on the properties file contents
     */
    static NiFiRegistryProperties loadNiFiRegistryProperties(final String propertiesFilePath) {
        final Properties props = new Properties();
        try (final FileReader reader = new FileReader(propertiesFilePath)) {
            props.load(reader);
            return new NiFiRegistryProperties(props);
        } catch (final IOException ioe) {
            throw new RuntimeException("Unable to load properties: " + ioe, ioe);
        }
    }

    private static Client createClientFromConfig(final NiFiRegistryClientConfig registryClientConfig) {

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.register(jacksonJaxbJsonProvider());

        final ClientBuilder clientBuilder = ClientBuilder.newBuilder().withConfig(clientConfig);

        final SSLContext sslContext = registryClientConfig.getSslContext();
        if (sslContext != null) {
            clientBuilder.sslContext(sslContext);
        }

        final HostnameVerifier hostnameVerifier = registryClientConfig.getHostnameVerifier();
        if (hostnameVerifier != null) {
            clientBuilder.hostnameVerifier(hostnameVerifier);
        }

        return clientBuilder.build();
    }

    private static JacksonJaxbJsonProvider jacksonJaxbJsonProvider() {
        JacksonJaxbJsonProvider jacksonJaxbJsonProvider = new JacksonJaxbJsonProvider();

        ObjectMapper mapper = new ObjectMapper();
        mapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.NON_NULL));
        mapper.setAnnotationIntrospector(new JaxbAnnotationIntrospector(mapper.getTypeFactory()));
        // Ignore unknown properties so that deployed client remain compatible with future versions of NiFi Registry that add new fields
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        jacksonJaxbJsonProvider.setMapper(mapper);
        return jacksonJaxbJsonProvider;
    }

}
