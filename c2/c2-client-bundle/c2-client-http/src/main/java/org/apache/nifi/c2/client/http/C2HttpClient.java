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

package org.apache.nifi.c2.client.http;

import static java.lang.String.format;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static okhttp3.MultipartBody.FORM;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.nifi.c2.client.C2ClientConfig;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.http.url.C2UrlProvider;
import org.apache.nifi.c2.client.http.url.C2UrlProviderFactory;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.serializer.C2Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C2HttpClient implements C2Client {

    static final MediaType MEDIA_TYPE_APPLICATION_JSON = MediaType.parse("application/json");
    private static final Logger logger = LoggerFactory.getLogger(C2HttpClient.class);
    private static final String MULTIPART_FORM_FILE_FIELD_NAME = "file";
    private static final String BUNDLE_FILE_NAME = "debug.tar.gz";
    private static final MediaType BUNDLE_MIME_TYPE = MediaType.parse("application/gzip");

    private final C2ClientConfig clientConfig;
    private final C2Serializer serializer;
    private final C2UrlProvider c2UrlProvider;
    private final AtomicReference<OkHttpClient> httpClientReference = new AtomicReference<>();

    public static C2HttpClient create(C2ClientConfig clientConfig, C2Serializer serializer) {
        OkHttpClient okHttpClient = new OkHttpClientProvider(clientConfig).okHttpClient();
        C2UrlProvider c2UrlProvider = new C2UrlProviderFactory(clientConfig).create();
        return new C2HttpClient(clientConfig, serializer, c2UrlProvider, okHttpClient);
    }

    C2HttpClient(C2ClientConfig clientConfig, C2Serializer serializer,
                 C2UrlProvider c2UrlProvider, OkHttpClient okHttpClient) {
        this.clientConfig = clientConfig;
        this.serializer = serializer;
        this.c2UrlProvider = c2UrlProvider;
        this.httpClientReference.set(okHttpClient);
    }

    @Override
    public Optional<C2HeartbeatResponse> publishHeartbeat(C2Heartbeat heartbeat) {
        return serializer.serialize(heartbeat).flatMap(this::sendHeartbeat);
    }

    @Override
    public void acknowledgeOperation(C2OperationAck operationAck) {
        String c2AcknowledgeUrl = c2UrlProvider.getAcknowledgeUrl();
        logger.info("Acknowledging Operation {} to C2 server {}", operationAck.getOperationId(), c2AcknowledgeUrl);
        serializer.serialize(operationAck)
            .map(operationAckBody -> RequestBody.create(operationAckBody, MEDIA_TYPE_APPLICATION_JSON))
            .map(requestBody -> new Request.Builder().post(requestBody).url(c2AcknowledgeUrl).build())
            .map(C2RequestCompression.forType(clientConfig.getC2RequestCompression())::compress)
            .ifPresent(this::sendAck);
    }

    @Override
    public Optional<byte[]> retrieveUpdateConfigurationContent(String callbackUrl) {
        return retrieveContent(callbackUrl, clientConfig.getHttpHeaders());
    }

    @Override
    public Optional<byte[]> retrieveUpdateAssetContent(String callbackUrl) {
        return retrieveContent(callbackUrl, Map.of());
    }

    @Override
    public Optional<Path> retrieveResourceItem(String callbackUrl, Function<InputStream, Optional<Path>> resourceConsumer) {
        Request request = new Request.Builder()
            .get()
            .url(callbackUrl)
            .build();

        try (Response response = httpClientReference.get().newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new C2ServerException("Resource content retrieval failed with HTTP return code " + response.code());
            }
            return ofNullable(response.body())
                .map(ResponseBody::byteStream)
                .map(resourceConsumer::apply)
                .orElseThrow(() -> new C2ServerException("Resource content retrieval failed with empty body"));
        } catch (Exception e) {
            logger.warn("Resource item retrieval failed", e);
            return empty();
        }
    }

    @Override
    public Optional<String> uploadBundle(String callbackUrl, byte[] bundle) {
        Request request = new Request.Builder()
            .url(callbackUrl)
            .post(new MultipartBody.Builder()
                .setType(FORM)
                .addFormDataPart(MULTIPART_FORM_FILE_FIELD_NAME, BUNDLE_FILE_NAME, RequestBody.create(bundle, BUNDLE_MIME_TYPE))
                .build())
            .build();

        logger.info("Uploading bundle to C2 server {} with size {}", callbackUrl, bundle.length);
        try (Response response = httpClientReference.get().newCall(request).execute()) {
            if (!response.isSuccessful()) {
                logger.error("Upload bundle failed to C2 server {} with status code {}", callbackUrl, response.code());
                return Optional.of("Upload bundle failed to C2 server");
            }
        } catch (IOException e) {
            logger.error("Could not upload bundle to C2 server {}", callbackUrl, e);
            return Optional.of("Could not upload bundle to C2 server");
        }
        return empty();
    }

    @Override
    public String getCallbackUrl(String absoluteUrl, String relativeUrl) {
        return c2UrlProvider.getCallbackUrl(absoluteUrl, relativeUrl);
    }

    private Optional<C2HeartbeatResponse> sendHeartbeat(String heartbeat) {
        Optional<C2HeartbeatResponse> c2HeartbeatResponse = empty();
        Request request = new Request.Builder()
            .post(RequestBody.create(heartbeat, MEDIA_TYPE_APPLICATION_JSON))
            .url(c2UrlProvider.getHeartbeatUrl())
            .build();

        Request decoratedRequest = C2RequestCompression.forType(clientConfig.getC2RequestCompression()).compress(request);

        try (Response heartbeatResponse = httpClientReference.get().newCall(decoratedRequest).execute()) {
            c2HeartbeatResponse = getResponseBody(heartbeatResponse).flatMap(response -> serializer.deserialize(response, C2HeartbeatResponse.class));
        } catch (IOException ce) {
            logger.error("Send Heartbeat failed to C2 server {}", c2UrlProvider.getHeartbeatUrl(), ce);
        }

        return c2HeartbeatResponse;
    }

    private Optional<String> getResponseBody(Response response) {
        String responseBody = null;

        try {
            responseBody = response.body().string();
            logger.debug("Received response body {}", responseBody);
        } catch (IOException e) {
            logger.error("HTTP Request failed", e);
        }

        return ofNullable(responseBody);
    }

    private void sendAck(Request request) {
        try (Response heartbeatResponse = httpClientReference.get().newCall(request).execute()) {
            if (!heartbeatResponse.isSuccessful()) {
                logger.warn("Acknowledgement was not successful with C2 server {} with status code {}", c2UrlProvider.getAcknowledgeUrl(), heartbeatResponse.code());
            }
        } catch (IOException e) {
            logger.error("Could not transmit ack to C2 server {}", c2UrlProvider.getAcknowledgeUrl(), e);
        }
    }

    private Optional<byte[]> retrieveContent(String callbackUrl, Map<String, String> httpHeaders) {
        Optional<byte[]> content = empty();

        Request.Builder requestBuilder = new Request.Builder()
            .get()
            .headers(Headers.of(httpHeaders))
            .url(callbackUrl);
        Request request = requestBuilder.build();

        try (Response response = httpClientReference.get().newCall(request).execute()) {
            Optional<ResponseBody> body = ofNullable(response.body());

            if (!response.isSuccessful()) {
                StringBuilder messageBuilder = new StringBuilder(format("Update content retrieval failed: HTTP %d", response.code()));
                body.map(Object::toString).ifPresent(messageBuilder::append);
                throw new C2ServerException(messageBuilder.toString());
            }

            if (body.isPresent()) {
                content = Optional.of(body.get().bytes());
            } else {
                logger.warn("No body returned when pulling new content");
            }
        } catch (Exception e) {
            logger.warn("Update content retrieval failed", e);
        }

        return content;
    }
}
