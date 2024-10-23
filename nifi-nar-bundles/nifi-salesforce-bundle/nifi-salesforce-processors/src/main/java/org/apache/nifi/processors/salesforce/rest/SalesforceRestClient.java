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
package org.apache.nifi.processors.salesforce.rest;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;

public class SalesforceRestClient {

    private static final MediaType JSON_MEDIA_TYPE = MediaType.parse("application/json; charset=utf-8");

    private final SalesforceConfiguration configuration;
    private final OkHttpClient httpClient;

    public SalesforceRestClient(SalesforceConfiguration configuration) {
        this.configuration = configuration;
        httpClient = new OkHttpClient.Builder()
                .readTimeout(configuration.getResponseTimeoutMillis(), TimeUnit.MILLISECONDS)
                .build();
    }

    public InputStream describeSObject(String sObject) {
        String url = getUrl("/sobjects/" + sObject + "/describe?maxRecords=1");
        Request request = buildGetRequest(url);
        return executeRequest(request);
    }

    public InputStream query(String query) {
        HttpUrl httpUrl = HttpUrl.get(getUrl("/query")).newBuilder()
                .addQueryParameter("q", query)
                .build();
        Request request = buildGetRequest(httpUrl.toString());
        return executeRequest(request);
    }

    public InputStream queryAll(String query) {
        HttpUrl httpUrl = HttpUrl.get(getUrl("/queryAll")).newBuilder()
                .addQueryParameter("q", query)
                .build();
        Request request = buildGetRequest(httpUrl.toString());
        return executeRequest(request);
    }

    public InputStream getNextRecords(String nextRecordsUrl) {
        HttpUrl httpUrl = HttpUrl.get(configuration.getInstanceUrl() + nextRecordsUrl).newBuilder().build();
        Request request = buildGetRequest(httpUrl.toString());
        return executeRequest(request);
    }

    public void postRecord(String sObjectApiName, String body) {
        HttpUrl httpUrl = HttpUrl.get(getUrl("/composite/tree/" + sObjectApiName)).newBuilder().build();
        RequestBody requestBody = RequestBody.create(body, JSON_MEDIA_TYPE);
        Request request = buildPostRequest(httpUrl.toString(), requestBody);
        executeRequest(request);
    }

    private InputStream executeRequest(Request request) {
        Response response = null;
        try {
            response = httpClient.newCall(request).execute();
            if (!response.isSuccessful()) {
                throw new ProcessException(String.format("Invalid response [%s]: %s", response.code(), response.body() == null ? null : response.body().string()));
            }
            return response.body().byteStream();
        } catch (IOException e) {
            if (response != null) {
                response.close();
            }
            throw new UncheckedIOException(String.format("Salesforce HTTP request failed [%s]", request.url()), e);
        }
    }

    private String getUrl(String path) {
        return getVersionedBaseUrl() + path;
    }

    public String getVersionedBaseUrl() {
        return configuration.getInstanceUrl() + "/services/data/v" + configuration.getVersion();
    }

    private Request buildGetRequest(String url) {
        return new Request.Builder()
                .addHeader("Authorization", "Bearer " + configuration.getAccessTokenProvider().get())
                .url(url)
                .get()
                .build();
    }

    private Request buildPostRequest(String url, RequestBody requestBody) {
        return new Request.Builder()
                .addHeader("Authorization", "Bearer " + configuration.getAccessTokenProvider().get())
                .url(url)
                .post(requestBody)
                .build();
    }
}
