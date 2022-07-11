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
package org.apache.nifi.flow.resource;

import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpsExternalResourceProviderTest extends AbstractHttpsExternalResourceProviderTest {
    private static final List<String> AVAILABLE_VERSIONS = Arrays.asList("1.15.0", "1.16.0", "1.16.1", "1.16.2", "1.16.13", "1.17.0");
    private static final List<String> EXPECTED_VERSIONS = Arrays.asList("1.16.0", "1.16.1", "1.16.2", "1.16.13");
    private static final String NAR_LOCATION_NOT_SPECIFIED = "";
    private static final String NAR_LOCATION = "nars";
    private static final String NAR_NAME_FORMAT = "file1-%s.nar";
    private static final String LAST_MODIFIED = "2021-05-17 21:09";
    private static final String SIZE = "size";
    private static final long LAST_MODIFIED_AS_LONG = 1621285740000L;
    private static final String URL = "https://test/versions/";
    private static final String VERSION_BASED_PATH_FILTER = "1\\.16\\.\\d*\\/|.*\\.nar";
    private static final String DIRECT_PATH_FILTER = ".*1\\.16\\.\\d*\\.nar";
    private static final String NAR_LOCATION_SPECIFIC_FILTER_WITHOUT_PROPERTY = "1\\.16.\\d*\\/|nars\\/|.*\\.nar";
    private static final String NAR_LOCATION_SPECIFIC_FILTER_WITH_PROPERTY = "1\\.16.\\d*\\/|.*\\.nar";
    private static final Request REQUEST = new Request.Builder().get().url(URL).build();
    private static final String FILE1_1_16_0_NAR = String.format(NAR_NAME_FORMAT, EXPECTED_VERSIONS.get(0));
    private static final String FILE1_1_16_1_NAR = String.format(NAR_NAME_FORMAT, EXPECTED_VERSIONS.get(1));
    private static final String FILE1_1_16_2_NAR = String.format(NAR_NAME_FORMAT, EXPECTED_VERSIONS.get(2));
    private static final String FILE1_1_16_13_NAR = String.format(NAR_NAME_FORMAT, EXPECTED_VERSIONS.get(3));
    private static final String HTML_FOR_DIRECTORY_LIKE_ENTRIES_WITH_ALL_AVAILABLE_VERSIONS = createHtmlForDirectoryLikeEntriesWithAllAvailableVersions();
    private static final String HTML_FOR_RESOURCE_FILE1_1_16_0_NAR = createHtmlForResource(FILE1_1_16_0_NAR);
    private static final String HTML_FOR_RESOURCE_FILE1_1_16_1_NAR = createHtmlForResource(FILE1_1_16_1_NAR);
    private static final String HTML_FOR_RESOURCE_FILE1_1_16_2_NAR = createHtmlForResource(FILE1_1_16_2_NAR);
    private static final String HTML_FOR_RESOURCE_FILE1_1_16_13_NAR = createHtmlForResource(FILE1_1_16_13_NAR);
    private static final String HTML_FOR_ALL_AVAILABLE_RESOURCES = createHtmlWithAllAvailableResources();
    private static final String HTML_FOR_NAR_LOCATION = createHtmlForNarLocation();
    public static final long SLEEPING_TIME = 1000L;

    @Test
    void testWhenAvailableResourcesAreUnderVersionBasedPaths() throws IOException, IllegalAccessException {
        // Example url for resources may be:
        // http://test/versions/1.15.0/file1-1.15.0.nar
        // http://test/versions/1.16.0/file1-1.16.0.nar
        // and so on...

        final HttpsExternalResourceProvider providerSpy = getHttpsExternalResourceProviderSpy(VERSION_BASED_PATH_FILTER, NAR_LOCATION_NOT_SPECIFIED);

        final Response versionResponse = createResponse(HTML_FOR_DIRECTORY_LIKE_ENTRIES_WITH_ALL_AVAILABLE_VERSIONS);
        final Response responseForResource1 = createResponse(HTML_FOR_RESOURCE_FILE1_1_16_0_NAR);
        final Response responseForResource2 = createResponse(HTML_FOR_RESOURCE_FILE1_1_16_1_NAR);
        final Response responseForResource3 = createResponse(HTML_FOR_RESOURCE_FILE1_1_16_2_NAR);
        final Response responseForResource4 = createResponse(HTML_FOR_RESOURCE_FILE1_1_16_13_NAR);

        final Call call = configureHttpCallForProvider(providerSpy);
        when(call.execute()).thenReturn(versionResponse, responseForResource1, responseForResource2, responseForResource3, responseForResource4);

        final Collection<ExternalResourceDescriptor> expected = createExpectedVersionBasedDescriptors();

        final Collection<ExternalResourceDescriptor> result = poll(providerSpy);

        assertSuccess(expected, result);
        verify(providerSpy, times(1)).listResources();
        verify(providerSpy, times(4)).listResources(any());
    }

    @Test
    void testWhenAvailableResourcesDirectlyUnderAPath() throws IOException, IllegalAccessException {
        // Example url for resources may be:
        // http://test/versions/file1-1.15.0.nar
        // http://test/versions/file1-1.16.0.nar
        // and so on...

        final HttpsExternalResourceProvider providerSpy = getHttpsExternalResourceProviderSpy(DIRECT_PATH_FILTER, NAR_LOCATION_NOT_SPECIFIED);

        final Response response = createResponse(HTML_FOR_ALL_AVAILABLE_RESOURCES);

        final Call call = configureHttpCallForProvider(providerSpy);
        when(call.execute()).thenReturn(response);

        final Collection<ExternalResourceDescriptor> expected = createExpectedDescriptorsWithDirectPath();

        final Collection<ExternalResourceDescriptor> result = poll(providerSpy);

        assertSuccess(expected, result);
        assertResourceIsNotIncluded(result,"read.me");
        verify(providerSpy, times(1)).listResources();
        verify(providerSpy, never()).listResources(any());
    }

    @Test
    void testWhenAvailableResourcesAreUnderAVersionBasedPathWithSpecificNarLocation_WithNarLocationPropertyProvided() throws IOException, IllegalAccessException {
        // Example url for resources may be:
        // http://test/versions/1.15.0/nars/file1-1.15.0.nar
        // http://test/versions/1.16.0/nars/file1-1.16.0.nar
        // and so on...
        // nar.location property = nars

        final HttpsExternalResourceProvider providerSpy = getHttpsExternalResourceProviderSpy(NAR_LOCATION_SPECIFIC_FILTER_WITH_PROPERTY, NAR_LOCATION);

        final Response versionResponse = createResponse(HTML_FOR_DIRECTORY_LIKE_ENTRIES_WITH_ALL_AVAILABLE_VERSIONS);
        final Response responseForResource1 = createResponse(HTML_FOR_RESOURCE_FILE1_1_16_0_NAR);
        final Response responseForResource2 = createResponse(HTML_FOR_RESOURCE_FILE1_1_16_1_NAR);
        final Response responseForResource3 = createResponse(HTML_FOR_RESOURCE_FILE1_1_16_2_NAR);
        final Response responseForResource4 = createResponse(HTML_FOR_RESOURCE_FILE1_1_16_13_NAR);

        final Call call = configureHttpCallForProvider(providerSpy);
        when(call.execute()).thenReturn(versionResponse, responseForResource1, responseForResource2, responseForResource3, responseForResource4);

        final Collection<ExternalResourceDescriptor> expected = createExpectedNarLocationSpecificDescriptors();

        final Collection<ExternalResourceDescriptor> result = poll(providerSpy);

        assertSuccess(expected, result);
        verify(providerSpy, times(1)).listResources();
        verify(providerSpy, times(4)).listResources(any());
    }

    @Test
    void testWhenAvailableResourcesAreUnderAVersionBasedPathWithSpecificNarLocation_WithoutNarLocationPropertyProvided() throws IOException, IllegalAccessException {
        // Example url for resources may be:
        // http://test/versions/1.15.0/nars/file1-1.15.0.nar
        // http://test/versions/1.16.0/nars/file1-1.16.0.nar
        // and so on...
        // nar.location property = "" or not provided

       final HttpsExternalResourceProvider providerSpy = getHttpsExternalResourceProviderSpy(NAR_LOCATION_SPECIFIC_FILTER_WITHOUT_PROPERTY, NAR_LOCATION_NOT_SPECIFIED);

        final Response versionResponse = createResponse(HTML_FOR_DIRECTORY_LIKE_ENTRIES_WITH_ALL_AVAILABLE_VERSIONS);
        final Response responseForNarLocation1 = createResponse(HTML_FOR_NAR_LOCATION);
        final Response responseForNarLocation2 = createResponse(HTML_FOR_NAR_LOCATION);
        final Response responseForNarLocation3 = createResponse(HTML_FOR_NAR_LOCATION);
        final Response responseForNarLocation4 = createResponse(HTML_FOR_NAR_LOCATION);
        final Response responseForResource1 = createResponse(HTML_FOR_RESOURCE_FILE1_1_16_0_NAR);
        final Response responseForResource2 = createResponse(HTML_FOR_RESOURCE_FILE1_1_16_1_NAR);
        final Response responseForResource3 = createResponse(HTML_FOR_RESOURCE_FILE1_1_16_2_NAR);
        final Response responseForResource4 = createResponse(HTML_FOR_RESOURCE_FILE1_1_16_13_NAR);

        final Call call = configureHttpCallForProvider(providerSpy);
        when(call.execute()).thenReturn(versionResponse, responseForNarLocation1, responseForNarLocation2, responseForNarLocation3, responseForNarLocation4,
                responseForResource1, responseForResource2, responseForResource3, responseForResource4);

        final Collection<ExternalResourceDescriptor> expected = createExpectedNarLocationSpecificDescriptors();

        final Collection<ExternalResourceDescriptor> result = poll(providerSpy);

        assertSuccess(expected, result);
        verify(providerSpy, times(1)).listResources();
        verify(providerSpy, times(8)).listResources(any());
    }

    private Response createResponse(final String body) {
        return new Response.Builder()
                .request(REQUEST)
                .protocol(Protocol.HTTP_2)
                .code(200)
                .message("")
                .body(createResponseBody(body))
                .build();
    }

    @NotNull
    private ResponseBody createResponseBody(final String responseBody) {
        return ResponseBody.create(responseBody, MediaType.get("text/html"));
    }

    private HttpsExternalResourceProvider getHttpsExternalResourceProviderSpy(final String filter, final String narLocation) {
        final HttpsExternalResourceProvider provider = new HttpsExternalResourceProvider();
        final Map<String, String> properties = new HashMap<>();
        properties.put("base.url", URL);
        properties.put("filter", filter);
        properties.put("user.name", "user");
        properties.put("password", "password");
        if (!narLocation.isEmpty()) {
            properties.put("nar.location", narLocation);
        }


        final ExternalResourceProviderInitializationContext context = mock(ExternalResourceProviderInitializationContext.class);
        when(context.getProperties()).thenReturn(properties);

        provider.initialize(context);

        return spy(provider);
    }

    private Call configureHttpCallForProvider(final ExternalResourceProvider provider) throws IllegalAccessException {
        final OkHttpClient client = mock(OkHttpClient.class);
        FieldUtils.writeField(provider, "client", client, true);
        final Call call = mock(Call.class);
        when(client.newCall(any())).thenReturn(call);
        return call;
    }

    private Collection<ExternalResourceDescriptor> createExpectedDescriptors(final boolean withVersions, final String narLocation) {
        final Collection<ExternalResourceDescriptor> expected = new ArrayList<>();
        String url = normalizeURL(String.format("%s%s/", URL, narLocation));

        for (final String version : EXPECTED_VERSIONS) {
            final String narName = String.format(NAR_NAME_FORMAT, version);
            if (withVersions) {
                url = normalizeURL(String.format("%s%s/%s/", URL, version, narLocation));
            }
            final ExternalResourceDescriptor descriptor = new ImmutableExternalResourceDescriptor(narName, LAST_MODIFIED_AS_LONG, url, false);
            expected.add(descriptor);
        }

        return expected;
    }

    private Collection<ExternalResourceDescriptor> createExpectedVersionBasedDescriptors() {
        return createExpectedDescriptors(true, NAR_LOCATION_NOT_SPECIFIED);
    }

    private Collection<ExternalResourceDescriptor> createExpectedDescriptorsWithDirectPath() {
        return createExpectedDescriptors(false, NAR_LOCATION_NOT_SPECIFIED);
    }

    private Collection<ExternalResourceDescriptor> createExpectedNarLocationSpecificDescriptors() {
        return createExpectedDescriptors(true, NAR_LOCATION);
    }

    private static String createHtmlForResource(final String fileName) {
        return HtmlBuilder.newInstance()
                .htmlStart()
                .tableStart()
                .tableHeaderEntry()
                .parentEntry()
                .fileEntry(fileName, LAST_MODIFIED, SIZE)
                .tableEnd()
                .htmlEnd()
                .build();
    }

    private static String createHtmlWithAllAvailableResources() {
        final HtmlBuilder builder = HtmlBuilder.newInstance()
                .htmlStart()
                .tableStart()
                .tableHeaderEntry()
                .parentEntry();

        for (final String version : AVAILABLE_VERSIONS) {
            builder.fileEntry(String.format(NAR_NAME_FORMAT, version), LAST_MODIFIED, SIZE);
        }

        builder.fileEntry("read.me", LAST_MODIFIED, SIZE)
                .tableEnd()
                .htmlEnd();

        return builder.build();
    }

    private static String createHtmlForDirectoryLikeEntriesWithAllAvailableVersions() {
        final HtmlBuilder builder = HtmlBuilder.newInstance()
                .htmlStart()
                .tableStart()
                .tableHeaderEntry()
                .parentEntry();

        for (String version : AVAILABLE_VERSIONS) {
            builder.directoryEntry(version);
        }

        builder.tableEnd()
                .htmlEnd();

        return builder.build();
    }

    private static String createHtmlForNarLocation() {
        return HtmlBuilder.newInstance()
                .htmlStart()
                .tableStart()
                .tableHeaderEntry()
                .parentEntry()
                .directoryEntry(NAR_LOCATION)
                .tableEnd()
                .htmlEnd()
                .build();
    }

    private Collection<ExternalResourceDescriptor> poll(final ExternalResourceProvider provider) {
        final Collection<ExternalResourceDescriptor> result = new ArrayList<>();

        final File targetDirectory = mock(File.class);
        final ExternalResourceConflictResolutionStrategy strategy = mock(ExternalResourceConflictResolutionStrategy.class);

        final ConflictResolvingExternalResourceProviderWorker worker = new ConflictResolvingExternalResourceProviderWorker(
                "",
                provider.getClass().getClassLoader(),
                provider,
                strategy,
                targetDirectory,
                SLEEPING_TIME,
                mock(CountDownLatch.class)
        ) {
            @Override
            protected void acquireResource(ExternalResourceDescriptor availableResource) {
                result.add(availableResource);
            }
        };

        when(targetDirectory.isDirectory()).thenReturn(true);
        when(targetDirectory.exists()).thenReturn(true);
        when(targetDirectory.canRead()).thenReturn(true);
        when(targetDirectory.canWrite()).thenReturn(true);
        when(strategy.shouldBeFetched(any(), any())).thenReturn(true);

        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(SLEEPING_TIME);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            worker.stop();
        });
        thread.start();
        worker.run();

        return result;
    }
}
