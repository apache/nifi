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

package org.apache.nifi.processors.airtable.service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Range;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

public class AirtableRestService {

    public static final String API_V0_BASE_URL = "https://api.airtable.com/v0";

    private static final int TOO_MANY_REQUESTS = 429;
    private static final Range<Integer> SUCCESSFUL_RESPONSE_RANGE = Range.of(200, 299);

    private final WebClientServiceProvider webClientServiceProvider;
    private final String apiUrl;
    private final String pat;
    private final String baseId;
    private final String tableId;

    public AirtableRestService(final WebClientServiceProvider webClientServiceProvider,
            final String apiUrl,
            final String pat,
            final String baseId,
            final String tableId) {
        this.webClientServiceProvider = webClientServiceProvider;
        this.apiUrl = apiUrl;
        this.pat = pat;
        this.baseId = baseId;
        this.tableId = tableId;
    }

    public <R> R getRecords(final AirtableGetRecordsParameters getRecordsParameters, final Function<InputStream, R> callback) {
        final URI uri = buildUri(getRecordsParameters);
        try (final HttpResponseEntity response = webClientServiceProvider.getWebClientService()
                .get()
                .uri(uri)
                .header("Authorization", "Bearer " + pat)
                .retrieve()) {

            final InputStream bodyInputStream = response.body();
            if (SUCCESSFUL_RESPONSE_RANGE.contains(response.statusCode())) {
                return callback.apply(bodyInputStream);
            }
            if (response.statusCode() == TOO_MANY_REQUESTS) {
                throw new RateLimitExceededException();
            }
            final StringBuilder exceptionMessageBuilder = new StringBuilder("Error response. Code: " + response.statusCode());
            final String bodyText = IOUtils.toString(bodyInputStream, StandardCharsets.UTF_8);
            if (bodyText != null) {
                exceptionMessageBuilder.append(" Body: ").append(bodyText);
            }

            throw new ProcessException(exceptionMessageBuilder.toString());
        } catch (IOException e) {
            throw new ProcessException(String.format("Airtable HTTP request failed [%s]", uri), e);
        }
    }

    public HttpUriBuilder createUriBuilder() {
        final URI uri = URI.create(apiUrl);
        final HttpUriBuilder uriBuilder = webClientServiceProvider.getHttpUriBuilder()
                .scheme(uri.getScheme())
                .host(uri.getHost())
                .encodedPath(uri.getPath())
                .addPathSegment(baseId)
                .addPathSegment(tableId);
        if (uri.getPort() != -1) {
            uriBuilder.port(uri.getPort());
        }
        return uriBuilder;
    }

    private URI buildUri(AirtableGetRecordsParameters getRecordsParameters) {
        final HttpUriBuilder uriBuilder = createUriBuilder();
        for (final String field : getRecordsParameters.getFields()) {
            uriBuilder.addQueryParameter("fields[]", field);
        }

        final List<String> filters = new ArrayList<>();
        getRecordsParameters.getCustomFilter()
                .ifPresent(filters::add);
        getRecordsParameters.getModifiedAfter()
                .map(modifiedAfter -> {
                    final String isSameFormula = "IS_SAME(LAST_MODIFIED_TIME(),DATETIME_PARSE(\"" + modifiedAfter + "\"), 'second')";
                    final String isAfterFormula = "IS_AFTER(LAST_MODIFIED_TIME(),DATETIME_PARSE(\"" + modifiedAfter + "\"))";
                    return "OR(" + isSameFormula + "," + isAfterFormula + ")";
                })
                .ifPresent(filters::add);
        getRecordsParameters.getModifiedBefore()
                .map(modifiedBefore -> "IS_BEFORE(LAST_MODIFIED_TIME(),DATETIME_PARSE(\"" + modifiedBefore + "\"))")
                .ifPresent(filters::add);
        if (!filters.isEmpty()) {
            uriBuilder.addQueryParameter("filterByFormula", "AND(" + String.join(",", filters) + ")");
        }
        getRecordsParameters.getOffset().ifPresent(offset -> uriBuilder.addQueryParameter("offset", offset));
        getRecordsParameters.getPageSize().ifPresent(pageSize -> uriBuilder.addQueryParameter("pageSize", String.valueOf(pageSize)));

        return uriBuilder.build();
    }
}
