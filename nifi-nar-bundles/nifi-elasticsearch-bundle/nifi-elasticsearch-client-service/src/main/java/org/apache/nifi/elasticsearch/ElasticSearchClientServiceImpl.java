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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
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
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSearchClientServiceImpl extends AbstractControllerService implements ElasticSearchClientService {
    private ObjectMapper mapper = new ObjectMapper();

    static final private List<PropertyDescriptor> properties;

    private RestClient client;
    private RestHighLevelClient highLevelClient;

    private String url;
    private Charset charset;

    static {
        List<PropertyDescriptor> _props = new ArrayList();
        _props.add(ElasticSearchClientService.HTTP_HOSTS);
        _props.add(ElasticSearchClientService.USERNAME);
        _props.add(ElasticSearchClientService.PASSWORD);
        _props.add(ElasticSearchClientService.PROP_SSL_CONTEXT_SERVICE);
        _props.add(ElasticSearchClientService.CONNECT_TIMEOUT);
        _props.add(ElasticSearchClientService.SOCKET_TIMEOUT);
        _props.add(ElasticSearchClientService.RETRY_TIMEOUT);
        _props.add(ElasticSearchClientService.CHARSET);

        properties = Collections.unmodifiableList(_props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        try {
            setupClient(context);
            charset = Charset.forName(context.getProperty(CHARSET).getValue());
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

    private SSLContext buildSslContext(SSLContextService sslService) throws IOException, CertificateException,
            NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException, KeyManagementException {
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
        return context1;
    }

    private void setupClient(ConfigurationContext context) throws MalformedURLException, InitializationException {
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

        final SSLContext sslContext;
        try {
            sslContext = (sslService != null && sslService.isKeyStoreConfigured() && sslService.isTrustStoreConfigured())
                ? buildSslContext(sslService) : null;
        } catch (IOException | CertificateException | NoSuchAlgorithmException | UnrecoverableKeyException
                | KeyStoreException | KeyManagementException e) {
            getLogger().error("Error building up SSL Context from the supplied configuration.", e);
            throw new InitializationException(e);
        }

        RestClientBuilder builder = RestClient.builder(hh)
            .setHttpClientConfigCallback(httpClientBuilder -> {
                if (sslContext != null) {
                    httpClientBuilder = httpClientBuilder.setSSLContext(sslContext);
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
                requestConfigBuilder.setConnectTimeout(connectTimeout);
                requestConfigBuilder.setSocketTimeout(readTimeout);
                return requestConfigBuilder;
            })
            .setMaxRetryTimeoutMillis(retryTimeout);

        this.client = builder.build();
        this.highLevelClient = new RestHighLevelClient(client);
    }

    private Response runQuery(String endpoint, String query, String index, String type) throws IOException {
        StringBuilder sb = new StringBuilder()
            .append("/")
            .append(index);
        if (type != null && !type.equals("")) {
            sb.append("/")
            .append(type);
        }

        sb.append(String.format("/%s", endpoint));

        HttpEntity queryEntity = new NStringEntity(query, ContentType.APPLICATION_JSON);

        return client.performRequest("POST", sb.toString(), Collections.emptyMap(), queryEntity);
    }

    private Map<String, Object> parseResponse(Response response) throws IOException {
        final int code = response.getStatusLine().getStatusCode();

        if (code >= 200 & code < 300) {
            InputStream inputStream = response.getEntity().getContent();
            byte[] result = IOUtils.toByteArray(inputStream);
            inputStream.close();
            return mapper.readValue(new String(result, charset), Map.class);
        } else {
            String errorMessage = String.format("ElasticSearch reported an error while trying to run the query: %s",
                response.getStatusLine().getReasonPhrase());
            throw new IOException(errorMessage);
        }
    }

    @Override
    public IndexOperationResponse add(IndexOperationRequest operation) throws IOException {
        return add(Arrays.asList(operation));
    }

    @Override
    public IndexOperationResponse add(List<IndexOperationRequest> operations) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        for (int index = 0; index < operations.size(); index++) {
            IndexOperationRequest or = operations.get(index);
            IndexRequest indexRequest = new IndexRequest(or.getIndex(), or.getType(), or.getId())
                .source(or.getFields());
            bulkRequest.add(indexRequest);
        }

        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        BulkResponse response = highLevelClient.bulk(bulkRequest);
        IndexOperationResponse retVal = new IndexOperationResponse(response.getTookInMillis(), response.getIngestTookInMillis());

        return retVal;
    }

    @Override
    public DeleteOperationResponse deleteById(String index, String type, String id) throws IOException {
        return deleteById(index, type, Arrays.asList(id));
    }

    @Override
    public DeleteOperationResponse deleteById(String index, String type, List<String> ids) throws IOException {
        BulkRequest bulk = new BulkRequest();
        for (int idx = 0; idx < ids.size(); idx++) {
            DeleteRequest request = new DeleteRequest(index, type, ids.get(idx));
            bulk.add(request);
        }
        BulkResponse response = highLevelClient.bulk(bulk);

        DeleteOperationResponse dor = new DeleteOperationResponse(response.getTookInMillis());

        return dor;
    }

    @Override
    public DeleteOperationResponse deleteByQuery(String query, String index, String type) throws IOException {
        long start = System.currentTimeMillis();
        Response response = runQuery("_delete_by_query", query, index, type);
        long end   = System.currentTimeMillis();
        Map<String, Object> parsed = parseResponse(response);

        return new DeleteOperationResponse(end - start);
    }

    @Override
    public Map<String, Object> get(String index, String type, String id) throws IOException {
        GetRequest get = new GetRequest(index, type, id);
        GetResponse resp = highLevelClient.get(get, new Header[]{});
        return resp.getSource();
    }

    @Override
    public SearchResponse search(String query, String index, String type) throws IOException {
        Response response = runQuery("_search", query, index, type);
        Map<String, Object> parsed = parseResponse(response);

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

        return esr;
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
