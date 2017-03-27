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
package org.apache.nifi.atlas;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.nifi.web.api.entity.ClusterEntity;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class NiFiApiClient {

    private static Logger logger = LoggerFactory.getLogger(NiFiApiClient.class);

    private static final Gson gson = new GsonBuilder().registerTypeAdapter(Date.class, new DateSerializer()).create();


    private final String baseUri;
    private RequestConfig requestConfig;
    private SSLContext sslContext;


    public NiFiApiClient(String baseUri) {
        this.baseUri = baseUri.endsWith("/") ? baseUri : baseUri + "/";

        final int connectTimeoutMillis = 3000;
        final int readTimeoutMillis = 3000;
        final RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                .setConnectionRequestTimeout(connectTimeoutMillis)
                .setConnectTimeout(connectTimeoutMillis)
                .setSocketTimeout(readTimeoutMillis);

        requestConfig = requestConfigBuilder.build();
    }

    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public String getBaseUri() {
        return baseUri;
    }

    public ProcessGroupFlowEntity getProcessGroupFlow() throws IOException {
        return getProcessGroupFlow("root");
    }

    public ProcessGroupFlowEntity getProcessGroupFlow(String processGroupId) throws IOException {
        final String path = "nifi-api/flow/process-groups/" + processGroupId;
        return getEntity(path, ProcessGroupFlowEntity.class);
    }

    public ProcessGroupEntity getProcessGroupEntity() throws IOException {
        final String path = "nifi-api/process-groups/root";
        return getEntity(path, ProcessGroupEntity.class);
    }

    public ClusterEntity getClusterEntity() throws IOException {
        final String path = "nifi-api/controller/cluster";
        return getEntity(path, ClusterEntity.class);
    }

    private static class DateSerializer implements JsonDeserializer<Date> {

        private static final String[] FORMATS = {
                "HH:mm:ss Z",
                "MM/dd/yyyy HH:mm:ss Z"
        };

        @Override
        public Date deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context)
                throws com.google.gson.JsonParseException {
            for (String format : FORMATS) {
                try {
                    return new SimpleDateFormat(format).parse(jsonElement.getAsString());
                } catch (ParseException e) {
                    // Try next format.
                }
            }
            throw new JsonParseException("Failed to parse " + jsonElement + " to Date.");
        }
    }

    public <T extends Entity> T getEntity(String path, Type type) throws IOException {
        final URI url = toUri(path);
        return getEntity(url, type);
    }

    public URI toUri(String path) {
        final URI url;
        try {
            url = new URI(baseUri + path);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Failed to parse URI due to " + e, e);
        }
        return url;
    }

    public <T extends Entity> T getEntity(URI url, Type type) throws IOException {
        final String responseMessage = getEntity(url);

        try {
            return gson.fromJson(responseMessage, type);
        } catch (JsonSyntaxException e) {
            final String msg = String.format("Failed to parse response Json as %s. uri=%s", type, url);
            logger.warn("{} response={}", msg, url, responseMessage);
            throw new IOException(msg, e);
        }
    }

    private String getEntity(URI url) throws IOException {
        final HttpGet get = new HttpGet(url);
        get.setConfig(requestConfig);
        get.setHeader("Accept", "application/json");

        HttpClientBuilder clientBuilder = HttpClients.custom();
        if (sslContext != null) {
            clientBuilder.setSslcontext(sslContext);
        }

        try (CloseableHttpClient httpClient = clientBuilder.build()) {

            try (CloseableHttpResponse response = httpClient.execute(get)) {

                final StatusLine statusLine = response.getStatusLine();
                final int statusCode = statusLine.getStatusCode();
                if (200 != statusCode) {
                    final String msg = String.format("Failed to get entity from %s, response=%d:%s",
                            get.getURI(), statusCode, statusLine.getReasonPhrase());
                    throw new RuntimeException(msg);
                }
                final HttpEntity entity = response.getEntity();
                return EntityUtils.toString(entity);
            }
        }
    }
}
