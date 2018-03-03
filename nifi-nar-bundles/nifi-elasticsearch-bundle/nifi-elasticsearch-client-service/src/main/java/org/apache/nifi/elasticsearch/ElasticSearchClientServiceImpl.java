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

package org.apache.nifi.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ElasticSearchClientServiceImpl extends AbstractControllerService implements ElasticSearchClientService {
    private ObjectMapper mapper = new ObjectMapper();

    private List<PropertyDescriptor> properties;

    private RestClient client;

    private String url;

    @Override
    protected void init(ControllerServiceInitializationContext config) {
        properties = new ArrayList<>();
        properties.add(ElasticSearchClientService.HTTP_HOSTS);
        properties.add(ElasticSearchClientService.USERNAME);
        properties.add(ElasticSearchClientService.PASSWORD);
        properties.add(ElasticSearchClientService.PROP_SSL_CONTEXT_SERVICE);
        properties.add(ElasticSearchClientService.CONNECT_TIMEOUT);
        properties.add(ElasticSearchClientService.SOCKET_TIMEOUT);
        properties.add(ElasticSearchClientService.RETRY_TIMEOUT);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        try {
            setupClient(context);
        } catch (Exception ex) {
            getLogger().error("Could not initialize ElasticSearch client.", ex);
            throw new InitializationException(ex);
        }
    }

    @OnDisabled
    public void onDisabled() throws IOException {
        this.client.close();
        this.url = null;
    }

    private void setupClient(ConfigurationContext context) throws Exception {
        final String hosts = context.getProperty(HTTP_HOSTS).evaluateAttributeExpressions().getValue();
        String[] hostsSplit = hosts.split(",[\\s]*");
        this.url = hostsSplit[0];
        final SSLContextService sslService =
                context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();

        final Integer connectTimeout = context.getProperty(CONNECT_TIMEOUT).asInteger();
        final Integer readTimeout    = context.getProperty(SOCKET_TIMEOUT).asInteger();
        final Integer retryTimeout   = context.getProperty(RETRY_TIMEOUT).asInteger();

        HttpHost[] hh = new HttpHost[hostsSplit.length];
        for (int x = 0; x < hh.length; x++) {
            URL u = new URL(hostsSplit[x]);
            hh[x] = new HttpHost(u.getHost(), u.getPort(), u.getProtocol());
        }

        RestClientBuilder builder = RestClient.builder(hh)
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    if (sslService != null) {
                        try {
                            KeyStore keyStore = KeyStore.getInstance(sslService.getKeyStoreType());
                            KeyStore trustStore = KeyStore.getInstance("JKS");

                            try (final InputStream is = new FileInputStream(sslService.getKeyStoreFile())) {
                                keyStore.load(is, sslService.getKeyStorePassword().toCharArray());
                            }

                            try (final InputStream is = new FileInputStream(sslService.getTrustStoreFile())) {
                                trustStore.load(is, sslService.getTrustStorePassword().toCharArray());
                            }

                            final KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory
                                    .getDefaultAlgorithm());
                            kmf.init(keyStore, sslService.getKeyStorePassword().toCharArray());
                            final TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory
                                    .getDefaultAlgorithm());
                            tmf.init(keyStore);
                            SSLContext context1 = SSLContext.getInstance(sslService.getSslAlgorithm());
                            context1.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
                            httpClientBuilder = httpClientBuilder.setSSLContext(context1);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }

                    if (username != null && password != null) {
                        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(AuthScope.ANY,
                                new UsernamePasswordCredentials(username, password));
                        httpClientBuilder = httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }

                    return httpClientBuilder;
                })
                .setRequestConfigCallback(requestConfigBuilder -> {
                    try {
                        requestConfigBuilder.setConnectTimeout(connectTimeout);
                        requestConfigBuilder.setSocketTimeout(readTimeout);
                        return requestConfigBuilder;
                    } catch (Exception ex) {
                        try {
                            PrintWriter writer = new PrintWriter(new FileWriter("/tmp/out.log"));
                            writer.println(requestConfigBuilder == null);
                            writer.println(readTimeout == null);
                            ex.printStackTrace(writer);
                            writer.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        throw new RuntimeException(ex);
                    }
                })
                .setMaxRetryTimeoutMillis(retryTimeout);

        this.client = builder.build();
    }

    private Response runQuery(String query, String index, String type) throws Exception {
        StringBuilder sb = new StringBuilder()
                .append("/")
                .append(index);
        if (type != null && !type.equals("")) {
            sb.append("/")
                    .append(type);
        }

        sb.append("/_search");

        HttpEntity queryEntity = new NStringEntity(query, ContentType.APPLICATION_JSON);

        return client.performRequest("POST", sb.toString(), Collections.emptyMap(), queryEntity);
    }

    @Override
    public Optional<SearchResponse> search(String query, String index, String type) {
        try {
            Response response = runQuery(query, index, type);
            Map<String, Object> parsed = mapper.readValue(IOUtils.toString(response.getEntity().getContent()), Map.class);

            int took = (Integer)parsed.get("took");
            boolean timedOut = (Boolean)parsed.get("timed_out");
            Map<String, Object> aggregations = parsed.get("aggregations") != null
                    ? (Map<String, Object>)parsed.get("aggregations") : new HashMap<>();
            Map<String, Object> hitsParent = (Map<String, Object>)parsed.get("hits");
            int count = (Integer)hitsParent.get("total");
            List<Map<String, Object>> hits = (List<Map<String, Object>>)hitsParent.get("hits");

            SearchResponse esr = new SearchResponse(hits, aggregations, count, took, timedOut);

            if (getLogger().isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("******************");
                sb.append(String.format("Took: %d", took));
                sb.append(String.format("Timed out: %s", timedOut));
                sb.append(String.format("Aggregation count: %d", aggregations.size()));
                sb.append(String.format("Hit count: %d", hits.size()));
                sb.append(String.format("Total found: %d", count));
                sb.append("******************");

                getLogger().debug(sb.toString());
            }

            return Optional.of(esr);
        } catch (Exception ex) {
            getLogger().error("Error running search.", ex);
            return Optional.empty();
        }
    }

    @Override
    public Optional<SearchResponse> search(Map<String, Object> query, String index, String type) {
        try {
            return search(mapper.writeValueAsString(query), index, type);
        } catch (JsonProcessingException e) {
            getLogger().error("Error trying to turn parameters into JSON string", new Object[]{ query }, e);
            return Optional.empty();
        }
    }

    @Override
    public String getTransitUrl(String index, String type) {
        return new StringBuilder()
            .append(this.url)
            .append(index != null && !index.equals("") ? "/" : "")
            .append(index != null ? index : "")
            .append(type != null && !type.equals("") ? "/" : "")
            .append(type != null ? type : "")
            .toString();
    }
}
