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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.StringUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ElasticSearchClientServiceImpl extends AbstractControllerService implements ElasticSearchClientService {
    private ObjectMapper mapper;

    private static final List<PropertyDescriptor> properties;

    private RestClient client;

    private String url;
    private Charset responseCharset;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ElasticSearchClientService.HTTP_HOSTS);
        props.add(ElasticSearchClientService.USERNAME);
        props.add(ElasticSearchClientService.PASSWORD);
        props.add(ElasticSearchClientService.PROP_SSL_CONTEXT_SERVICE);
        props.add(ElasticSearchClientService.PROXY_CONFIGURATION_SERVICE);
        props.add(ElasticSearchClientService.CONNECT_TIMEOUT);
        props.add(ElasticSearchClientService.SOCKET_TIMEOUT);
        props.add(ElasticSearchClientService.RETRY_TIMEOUT);
        props.add(ElasticSearchClientService.CHARSET);
        props.add(ElasticSearchClientService.SUPPRESS_NULLS);

        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        try {
            setupClient(context);
            responseCharset = Charset.forName(context.getProperty(CHARSET).getValue());

            // re-create the ObjectMapper in case the SUPPRESS_NULLS property has changed - the JsonInclude settings aren't dynamic
            mapper = new ObjectMapper();
            if (ALWAYS_SUPPRESS.getValue().equals(context.getProperty(SUPPRESS_NULLS).getValue())) {
                mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
            }
        } catch (final Exception ex) {
            getLogger().error("Could not initialize ElasticSearch client.", ex);
            throw new InitializationException(ex);
        }
    }

    @OnDisabled
    public void onDisabled() throws IOException {
        this.client.close();
        this.url = null;
    }

    private void setupClient(final ConfigurationContext context) throws MalformedURLException, InitializationException {
        final String hosts = context.getProperty(HTTP_HOSTS).evaluateAttributeExpressions().getValue();
        final String[] hostsSplit = hosts.split(",[\\s]*");
        this.url = hostsSplit[0];
        final SSLContextService sslService =
                context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();

        final Integer connectTimeout = context.getProperty(CONNECT_TIMEOUT).asInteger();
        final Integer readTimeout    = context.getProperty(SOCKET_TIMEOUT).asInteger();

        final ProxyConfigurationService proxyConfigurationService = context.getProperty(PROXY_CONFIGURATION_SERVICE).asControllerService(ProxyConfigurationService.class);

        final HttpHost[] hh = new HttpHost[hostsSplit.length];
        for (int x = 0; x < hh.length; x++) {
            final URL u = new URL(hostsSplit[x]);
            hh[x] = new HttpHost(u.getHost(), u.getPort(), u.getProtocol());
        }

        final SSLContext sslContext;
        try {
            sslContext = (sslService != null && (sslService.isKeyStoreConfigured() || sslService.isTrustStoreConfigured()))
                    ? sslService.createContext() : null;
        } catch (final Exception e) {
            getLogger().error("Error building up SSL Context from the supplied configuration.", e);
            throw new InitializationException(e);
        }

        final RestClientBuilder builder = RestClient.builder(hh)
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    if (sslContext != null) {
                        httpClientBuilder.setSSLContext(sslContext);
                    }

                    CredentialsProvider credentialsProvider = null;
                    if (username != null && password != null) {
                        credentialsProvider = addCredentials(null, AuthScope.ANY, username, password);
                    }

                    if (proxyConfigurationService != null) {
                        final ProxyConfiguration proxyConfiguration = proxyConfigurationService.getConfiguration();
                        if (Proxy.Type.HTTP == proxyConfiguration.getProxyType()) {
                            final HttpHost proxy = new HttpHost(proxyConfiguration.getProxyServerHost(), proxyConfiguration.getProxyServerPort(), "http");
                            httpClientBuilder.setProxy(proxy);

                            credentialsProvider = addCredentials(credentialsProvider, new AuthScope(proxy), proxyConfiguration.getProxyUserName(), proxyConfiguration.getProxyUserPassword());
                        }
                    }

                    if (credentialsProvider != null) {
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }

                    return httpClientBuilder;
                })
                .setRequestConfigCallback(requestConfigBuilder -> {
                    requestConfigBuilder.setConnectTimeout(connectTimeout);
                    requestConfigBuilder.setSocketTimeout(readTimeout);
                    return requestConfigBuilder;
                });

        this.client = builder.build();
    }

    private CredentialsProvider addCredentials(final CredentialsProvider credentialsProvider, final AuthScope authScope, final String username, final String password) {
        final CredentialsProvider cp = credentialsProvider != null ? credentialsProvider : new BasicCredentialsProvider();

        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            cp.setCredentials(
                    authScope == null ? AuthScope.ANY : authScope,
                    new UsernamePasswordCredentials(username, password)
            );
        }

        return cp;
    }

    private Response runQuery(final String endpoint, final String query, final String index, final String type, final Map<String, String> requestParameters) {
        final StringBuilder sb = new StringBuilder();
        if (StringUtils.isNotBlank(index)) {
            sb.append("/").append(index);
        }
        if (StringUtils.isNotBlank(type)) {
            sb.append("/").append(type);
        }

        sb.append(String.format("/%s", endpoint));

        final HttpEntity queryEntity = new NStringEntity(query, ContentType.APPLICATION_JSON);
        try {
            return performRequest("POST", sb.toString(), requestParameters, queryEntity);
        } catch (final Exception e) {
            throw new ElasticsearchException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseResponse(final Response response) {
        final int code = response.getStatusLine().getStatusCode();

        try {
            if (code >= 200 && code < 300) {
                final InputStream inputStream = response.getEntity().getContent();
                final byte[] result = IOUtils.toByteArray(inputStream);
                inputStream.close();
                return mapper.readValue(new String(result, responseCharset), Map.class);
            } else {
                final String errorMessage = String.format("ElasticSearch reported an error while trying to run the query: %s",
                        response.getStatusLine().getReasonPhrase());
                throw new IOException(errorMessage);
            }
        } catch (final Exception ex) {
            throw new ElasticsearchException(ex);
        }
    }

    private List<String> parseResponseWarningHeaders(final Response response) {
        return Arrays.stream(response.getHeaders())
                .filter(h -> "Warning".equalsIgnoreCase(h.getName()))
                .map(Header::getValue)
                .peek(h -> getLogger().warn("Elasticsearch Warning: {}", h))
                .collect(Collectors.toList());
    }

    @Override
    public IndexOperationResponse add(final IndexOperationRequest operation, final Map<String, String> requestParameters) {
        return bulk(Collections.singletonList(operation), requestParameters);
    }

    private String flatten(final String str) {
        return str.replaceAll("[\\n\\r]", "\\\\n");
    }

    private String buildBulkHeader(final IndexOperationRequest request) throws JsonProcessingException {
        final String operation = request.getOperation().equals(IndexOperationRequest.Operation.Upsert)
                ? "update"
                : request.getOperation().getValue();
        return buildBulkHeader(operation, request.getIndex(), request.getType(), request.getId());
    }

    private String buildBulkHeader(final String operation, final String index, final String type, final String id) throws JsonProcessingException {
        final Map<String, Object> header = new HashMap<String, Object>() {{
            put(operation, new HashMap<String, Object>() {{
                put("_index", index);
                if (StringUtils.isNotBlank(id)) {
                    put("_id", id);
                }
                if (StringUtils.isNotBlank(type)) {
                    put("_type", type);
                }
            }});
        }};

        return flatten(mapper.writeValueAsString(header));
    }

    protected void buildRequest(final IndexOperationRequest request, final StringBuilder builder) throws JsonProcessingException {
        final String header = buildBulkHeader(request);
        builder.append(header).append("\n");
        switch (request.getOperation()) {
            case Index:
            case Create:
                final String indexDocument = mapper.writeValueAsString(request.getFields());
                builder.append(indexDocument).append("\n");
                break;
            case Update:
            case Upsert:
                final Map<String, Object> doc = new HashMap<String, Object>() {{
                    put("doc", request.getFields());
                    if (request.getOperation().equals(IndexOperationRequest.Operation.Upsert)) {
                        put("doc_as_upsert", true);
                    }
                }};
                final String update = flatten(mapper.writeValueAsString(doc)).trim();
                builder.append(update).append("\n");
                break;
            case Delete:
                // nothing to do for Delete operations, it just needs the header
                break;
            default:
                throw new IllegalArgumentException(String.format("Unhandled Index Operation type: %s", request.getOperation().name()));
        }
    }

    @Override
    public IndexOperationResponse bulk(final List<IndexOperationRequest> operations, final Map<String, String> requestParameters) {
        try {
            final StringBuilder payload = new StringBuilder();
            for (final IndexOperationRequest or : operations) {
                buildRequest(or, payload);
            }

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(payload.toString());
            }
            final HttpEntity entity = new NStringEntity(payload.toString(), ContentType.APPLICATION_JSON);
            final StopWatch watch = new StopWatch();
            watch.start();
            final Response response = performRequest("POST", "/_bulk", requestParameters, entity);
            watch.stop();

            final String rawResponse = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
            parseResponseWarningHeaders(response);

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Response was: %s", rawResponse));
            }

            return IndexOperationResponse.fromJsonResponse(rawResponse);
        } catch (final Exception ex) {
            throw new ElasticsearchException(ex);
        }
    }

    @Override
    public Long count(final String query, final String index, final String type, final Map<String, String> requestParameters) {
        final Response response = runQuery("_count", query, index, type, requestParameters);
        final Map<String, Object> parsed = parseResponse(response);

        return ((Integer)parsed.get("count")).longValue();
    }

    @Override
    public DeleteOperationResponse deleteById(final String index, final String type, final String id, final Map<String, String> requestParameters) {
        return deleteById(index, type, Collections.singletonList(id), requestParameters);
    }

    @Override
    public DeleteOperationResponse deleteById(final String index, final String type, final List<String> ids, final Map<String, String> requestParameters) {
        try {
            final StringBuilder sb = new StringBuilder();
            for (final String id : ids) {
                final String header = buildBulkHeader("delete", index, type, id);
                sb.append(header).append("\n");
            }
            final HttpEntity entity = new NStringEntity(sb.toString(), ContentType.APPLICATION_JSON);
            final StopWatch watch = new StopWatch();
            watch.start();
            final Response response = performRequest("POST", "/_bulk", requestParameters, entity);
            watch.stop();

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Response for bulk delete: %s",
                        IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8)));
            }

            parseResponseWarningHeaders(response);
            return new DeleteOperationResponse(watch.getDuration(TimeUnit.MILLISECONDS));
        } catch (final Exception ex) {
            throw new ElasticsearchException(ex);
        }
    }

    @Override
    public DeleteOperationResponse deleteByQuery(final String query, final String index, final String type, final Map<String, String> requestParameters) {
        final StopWatch watch = new StopWatch();
        watch.start();
        final Response response = runQuery("_delete_by_query", query, index, type, requestParameters);
        watch.stop();

        // check for errors in response
        parseResponse(response);
        parseResponseWarningHeaders(response);

        return new DeleteOperationResponse(watch.getDuration(TimeUnit.MILLISECONDS));
    }

    @Override
    public UpdateOperationResponse updateByQuery(final String query, final String index, final String type, final Map<String, String> requestParameters) {
        final long start = System.currentTimeMillis();
        final Response response = runQuery("_update_by_query", query, index, type, requestParameters);
        final long end = System.currentTimeMillis();

        // check for errors in response
        parseResponse(response);

        return new UpdateOperationResponse(end - start);
    }

    @Override
    public void refresh(final String index, final Map<String, String> requestParameters) {
        try {
            final StringBuilder endpoint = new StringBuilder();
            if (StringUtils.isNotBlank(index) && !"/".equals(index)) {
                endpoint.append("/").append(index);
            }
            endpoint.append("/_refresh");
            final Response response = performRequest("POST", endpoint.toString(), requestParameters, null);
            parseResponseWarningHeaders(response);
        } catch (final Exception ex) {
            throw new ElasticsearchException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> get(final String index, final String type, final String id, final Map<String, String> requestParameters) {
        try {
            final StringBuilder endpoint = new StringBuilder();
            endpoint.append("/").append(index);
            if (StringUtils.isNotBlank(type)) {
                endpoint.append("/").append(type);
            } else {
                endpoint.append("/_doc");
            }
            endpoint.append("/").append(id);

            final Response response = performRequest("GET", endpoint.toString(), requestParameters, null);
            final String body = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
            parseResponseWarningHeaders(response);

            return (Map<String, Object>) mapper.readValue(body, Map.class).get("_source");
        } catch (final Exception ex) {
            throw new ElasticsearchException(ex);
        }
    }

    /*
     * In pre-7.X ElasticSearch, it should return just a number. 7.X and after they are returning a map.
     */
    @SuppressWarnings("unchecked")
    private int handleSearchCount(final Object raw) {
        if (raw instanceof Number) {
            return Integer.parseInt(raw.toString());
        } else if (raw instanceof Map) {
            return (Integer)((Map<String, Object>)raw).get("value");
        } else {
            throw new ProcessException("Unknown type for hit count.");
        }
    }

    @Override
    public SearchResponse search(final String query, final String index, final String type, final Map<String, String> requestParameters) {
        try {
            final Response response = runQuery("_search", query, index, type, requestParameters);
            return buildSearchResponse(response);
        } catch (final Exception ex) {
            throw new ElasticsearchException(ex);
        }
    }

    @Override
    public SearchResponse scroll(final String scroll) {
        try {
            final HttpEntity scrollEntity = new NStringEntity(scroll, ContentType.APPLICATION_JSON);
            final Response response = performRequest("POST", "/_search/scroll", Collections.emptyMap(), scrollEntity);
            return buildSearchResponse(response);
        } catch (final Exception ex) {
            throw new ElasticsearchException(ex);
        }
    }

    @Override
    public String initialisePointInTime(final String index, final String keepAlive) {
        try {
            final Map<String, String> params = new HashMap<String, String>() {{
                if (StringUtils.isNotBlank(keepAlive)) {
                    put("keep_alive", keepAlive);
                }
            }};
            final Response response = performRequest("POST", "/" + index + "/_pit", params, null);
            final String body = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
            parseResponseWarningHeaders(response);

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Response for initialising Point in Time: %s", body));
            }

            return (String) mapper.readValue(body, Map.class).get("id");
        } catch (final Exception ex) {
            throw new ElasticsearchException(ex);
        }
    }

    @Override
    public DeleteOperationResponse deletePointInTime(final String pitId) {
        try {
            final HttpEntity pitEntity = new NStringEntity(String.format("{\"id\": \"%s\"}", pitId), ContentType.APPLICATION_JSON);

            final StopWatch watch = new StopWatch(true);
            final Response response = performRequest("DELETE", "/_pit", Collections.emptyMap(), pitEntity);
            watch.stop();

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Response for deleting Point in Time: %s",
                        IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8))
                );
            }

            parseResponseWarningHeaders(response);
            return new DeleteOperationResponse(watch.getDuration(TimeUnit.MILLISECONDS));
        } catch (final ResponseException re) {
            if (404 == re.getResponse().getStatusLine().getStatusCode()) {
                getLogger().debug("Point in Time {} not found in Elasticsearch for deletion, ignoring", pitId);
                return new DeleteOperationResponse(0);
            }
            throw new ElasticsearchException(re);
        } catch (final Exception ex) {
            throw new ElasticsearchException(ex);
        }
    }

    @Override
    public DeleteOperationResponse deleteScroll(final String scrollId) {
        try {
            final HttpEntity scrollBody = new NStringEntity(String.format("{\"scroll_id\": \"%s\"}", scrollId), ContentType.APPLICATION_JSON);

            final StopWatch watch = new StopWatch(true);
            final Response response = performRequest("DELETE", "/_search/scroll", Collections.emptyMap(), scrollBody);
            watch.stop();

            if (getLogger().isDebugEnabled()) {
                getLogger().debug(String.format("Response for deleting Scroll: %s",
                        IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8))
                );
            }

            parseResponseWarningHeaders(response);
            return new DeleteOperationResponse(watch.getDuration(TimeUnit.MILLISECONDS));
        } catch (final ResponseException re) {
            if (404 == re.getResponse().getStatusLine().getStatusCode()) {
                getLogger().debug("Scroll Id {} not found in Elasticsearch for deletion, ignoring", scrollId);
                return new DeleteOperationResponse(0);
            }
            throw new ElasticsearchException(re);
        } catch (final Exception ex) {
            throw new ElasticsearchException(ex);
        }
    }

    @SuppressWarnings("unchecked")
    private SearchResponse buildSearchResponse(final Response response) throws JsonProcessingException {
        final Map<String, Object> parsed = parseResponse(response);
        final List<String> warnings = parseResponseWarningHeaders(response);

        final int took = (Integer)parsed.get("took");
        final boolean timedOut = (Boolean)parsed.get("timed_out");
        final String pitId = parsed.get("pit_id") != null ? (String)parsed.get("pit_id") : null;
        final String scrollId = parsed.get("_scroll_id") != null ? (String)parsed.get("_scroll_id") : null;
        final Map<String, Object> aggregations = parsed.get("aggregations") != null
                ? (Map<String, Object>)parsed.get("aggregations") : new HashMap<>();
        final Map<String, Object> hitsParent = (Map<String, Object>)parsed.get("hits");
        final int count = handleSearchCount(hitsParent.get("total"));
        final List<Map<String, Object>> hits = (List<Map<String, Object>>)hitsParent.get("hits");
        final String searchAfter = getSearchAfter(hits);

        final SearchResponse esr = new SearchResponse(hits, aggregations, pitId, scrollId, searchAfter, count, took, timedOut, warnings);

        if (getLogger().isDebugEnabled()) {
            final String searchSummary = "******************" +
                    String.format(Locale.getDefault(), "Took: %d", took) +
                    String.format(Locale.getDefault(), "Timed out: %s", timedOut) +
                    String.format(Locale.getDefault(), "Aggregation count: %d", aggregations.size()) +
                    String.format(Locale.getDefault(), "Hit count: %d", hits.size()) +
                    String.format(Locale.getDefault(), "PIT Id: %s", pitId) +
                    String.format(Locale.getDefault(), "Scroll Id: %s", scrollId) +
                    String.format(Locale.getDefault(), "Search After: %s", searchAfter) +
                    String.format(Locale.getDefault(), "Total found: %d", count) +
                    String.format(Locale.getDefault(), "Warnings: %s", warnings) +
                    "******************";
            getLogger().debug(searchSummary);
        }

        return esr;
    }

    private String getSearchAfter(final List<Map<String, Object>> hits) throws JsonProcessingException {
        String searchAfter = null;
        if (!hits.isEmpty()) {
            final Object lastHitSort = hits.get(hits.size() - 1).get("sort");
            if (lastHitSort != null && !"null".equalsIgnoreCase(lastHitSort.toString())) {
                searchAfter = mapper.writeValueAsString(lastHitSort);
            }
        }
        return searchAfter;
    }

    @Override
    public String getTransitUrl(final String index, final String type) {
        return this.url +
                (StringUtils.isNotBlank(index) ? "/" : "") +
                (StringUtils.isNotBlank(index) ? index : "") +
                (StringUtils.isNotBlank(type) ? "/" : "") +
                (StringUtils.isNotBlank(type) ? type : "");
    }

    private Response performRequest(final String method, final String endpoint, final Map<String, String> parameters, final HttpEntity entity) throws IOException {
        final Request request = new Request(method, endpoint);
        if (parameters != null && !parameters.isEmpty()) {
            request.addParameters(parameters);
        }
        if (entity != null) {
            request.setEntity(entity);
        }
        return client.performRequest(request);
    }
}
