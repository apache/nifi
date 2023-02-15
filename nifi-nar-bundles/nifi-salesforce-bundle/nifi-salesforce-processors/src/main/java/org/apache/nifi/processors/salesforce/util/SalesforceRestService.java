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
package org.apache.nifi.processors.salesforce.util;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class SalesforceRestService {
    private final String version;
    private final String baseUrl;
    private final Supplier<String> accessTokenProvider;
    private final OkHttpClient httpClient;

    public SalesforceRestService(String version, String baseUrl, Supplier<String> accessTokenProvider, int responseTimeoutMillis) {
        this.version = version;
        this.baseUrl = baseUrl;
        this.accessTokenProvider = accessTokenProvider;
        httpClient = new OkHttpClient.Builder()
                .readTimeout(responseTimeoutMillis, TimeUnit.MILLISECONDS)
                .build();
    }

    public InputStream describeSObject(String sObject) {
        String url = getVersionedBaseUrl() + "/sobjects/" + sObject + "/describe?maxRecords=1";

        Request request = new Request.Builder()
                .addHeader("Authorization", "Bearer " + accessTokenProvider.get())
                .url(url)
                .get()
                .build();

        return request(request);
    }

    public InputStream query(String query) {
        String url = getVersionedBaseUrl() + "/query";

        HttpUrl httpUrl = HttpUrl.get(url).newBuilder()
                .addQueryParameter("q", query)
                .build();

        Request request = new Request.Builder()
                .addHeader("Authorization", "Bearer " + accessTokenProvider.get())
                .url(httpUrl)
                .get()
                .build();

        return request(request);
    }

    public InputStream getNextRecords(String nextRecordsUrl) {
        String url = baseUrl + nextRecordsUrl;

        HttpUrl httpUrl = HttpUrl.get(url).newBuilder()
                .build();

        Request request = new Request.Builder()
                .addHeader("Authorization", "Bearer " + accessTokenProvider.get())
                .url(httpUrl)
                .get()
                .build();

        return request(request);
    }

    public InputStream postRecord(String sObjectApiName, String body) {
        String url = getVersionedBaseUrl() + "/composite/tree/" + sObjectApiName;

        HttpUrl httpUrl = HttpUrl.get(url).newBuilder()
                .build();

        final RequestBody requestBody = RequestBody.create(body, MediaType.parse("application/json"));

        Request request = new Request.Builder()
                .addHeader("Authorization", "Bearer " + accessTokenProvider.get())
                .url(httpUrl)
                .post(requestBody)
                .build();

        return request(request);
    }

    public String getVersionedBaseUrl() {
        return baseUrl + "/services/data/v" + version;
    }

    private InputStream request(Request request) {
        Response response = null;
        try {
            response = httpClient.newCall(request).execute();
            if (response.code() < 200 || response.code() > 201) {
                throw new ProcessException("Invalid response" +
                        " Code: " + response.code() +
                        " Message: " + response.message() +
                        " Body: " + (response.body() == null ? null : response.body().string())
                );
            }
            return response.body().byteStream();
        } catch (ProcessException e) {
            if (response != null) {
                response.close();
            }
            throw e;
        } catch (Exception e) {
            if (response != null) {
                response.close();
            }
            throw new ProcessException(String.format("Salesforce HTTP request failed [%s]", request.url()), e);
        }
    }
}
