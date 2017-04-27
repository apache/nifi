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

package org.apache.nifi.minifi.bootstrap.configuration.ingestors.common;

import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeNotifier;
import org.apache.nifi.minifi.bootstrap.configuration.ListenerHandleResult;
import org.apache.nifi.minifi.bootstrap.configuration.differentiators.interfaces.Differentiator;
import org.apache.nifi.minifi.bootstrap.configuration.ingestors.RestChangeIngestor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class RestChangeIngestorCommonTest {

    private static String testString = "This is a test string.";

    public static OkHttpClient client;
    public static RestChangeIngestor restChangeIngestor;
    public static final MediaType MEDIA_TYPE_MARKDOWN  = MediaType.parse("text/x-markdown; charset=utf-8");
    public static String url;
    public static ConfigurationChangeNotifier testNotifier;
    public static Differentiator<InputStream> mockDifferentiator = Mockito.mock(Differentiator.class);


    @Before
    public void before() {
        Mockito.reset(testNotifier);
        ConfigurationChangeListener testListener = Mockito.mock(ConfigurationChangeListener.class);
        when(testListener.getDescriptor()).thenReturn("MockChangeListener");
        Mockito.when(testNotifier.notifyListeners(Mockito.any())).thenReturn(Collections.singleton(new ListenerHandleResult(testListener)));
    }

    @Test
    public void testGet() throws Exception {
        Request request = new Request.Builder()
                .url(url)
                .build();

        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);

        Headers responseHeaders = response.headers();
        for (int i = 0; i < responseHeaders.size(); i++) {
            System.out.println(responseHeaders.name(i) + ": " + responseHeaders.value(i));
        }

        assertEquals(RestChangeIngestor.GET_TEXT, response.body().string());
        verify(testNotifier, Mockito.never()).notifyListeners(Mockito.any(ByteBuffer.class));
    }

    @Test
    public void testFileUploadNewConfig() throws Exception {
        when(mockDifferentiator.isNew(Mockito.any(InputStream.class))).thenReturn(true);

        Request request = new Request.Builder()
                .url(url)
                .post(RequestBody.create(MEDIA_TYPE_MARKDOWN, testString))
                .addHeader("charset","UTF-8")
                .build();

        Response response = client.newCall(request).execute();
        if (!response.isSuccessful()) throw new IOException("Unexpected code " + response);

        Headers responseHeaders = response.headers();
        for (int i = 0; i < responseHeaders.size(); i++) {
            System.out.println(responseHeaders.name(i) + ": " + responseHeaders.value(i));
        }

        assertEquals("The result of notifying listeners:\nMockChangeListener successfully handled the configuration change\n", response.body().string());

        verify(testNotifier, Mockito.times(1)).notifyListeners(Mockito.eq(ByteBuffer.wrap(testString.getBytes())));
    }

    @Test
    public void testFileUploadSameConfig() throws Exception {
        when(mockDifferentiator.isNew(Mockito.any(InputStream.class))).thenReturn(false);

        Request request = new Request.Builder()
                .url(url)
                .post(RequestBody.create(MEDIA_TYPE_MARKDOWN, testString))
                .addHeader("charset","UTF-8")
                .build();

        Response response = client.newCall(request).execute();
        if (response.isSuccessful()) throw new IOException("Unexpected code " + response);

        Headers responseHeaders = response.headers();
        for (int i = 0; i < responseHeaders.size(); i++) {
            System.out.println(responseHeaders.name(i) + ": " + responseHeaders.value(i));
        }

        assertEquals("Request received but instance is already running this config.", response.body().string());

        verify(testNotifier, Mockito.never()).notifyListeners(Mockito.any());
    }
}
