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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.UriBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;

public class AirtableRestService {

    public static final String API_V0_BASE_URL = "https://api.airtable.com/v0";

    private final WebClientService webClientService;
    private final String apiUrl;
    private final String apiToken;
    private final String baseId;
    private final String tableId;

    public AirtableRestService(final WebClientService webClientService,
            final String apiUrl,
            final String apiToken,
            final String baseId,
            final String tableId) {
        this.webClientService = webClientService;
        this.apiUrl = apiUrl;
        this.apiToken = apiToken;
        this.baseId = baseId;
        this.tableId = tableId;
    }

    public byte[] getRecords(final AirtableGetRecordsParameters filter) throws RateLimitExceeded {
        final URI uri = buildUri(filter);
        try (final HttpResponseEntity response = webClientService.get()
                .uri(uri)
                .header("Authorization", "Bearer " + apiToken)
                .retrieve()) {

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return IOUtils.toByteArray(response.body());
            }
            if (response.statusCode() == 429) {
                throw new RateLimitExceeded();
            }
            final StringBuilder exceptionMessageBuilder = new StringBuilder("Invalid response. Code: " + response.statusCode());
            final String bodyText = IOUtils.toString(response.body(), StandardCharsets.UTF_8);
            if (bodyText != null) {
                exceptionMessageBuilder.append(" Body: ").append(bodyText);
            }

            throw new ProcessException(exceptionMessageBuilder.toString());
        } catch (IOException e) {
            throw new ProcessException(String.format("Airtable HTTP request failed [%s]", uri), e);
        }
    }

    public static class RateLimitExceeded extends Exception {
        public RateLimitExceeded() {
            super("Airtable REST API rate limit exceeded");
        }
    }

    private URI buildUri(AirtableGetRecordsParameters filter) {
        final UriBuilder uriBuilder = UriBuilder.fromUri(apiUrl).path(baseId).path(tableId);

        for (final String field : filter.getFields()) {
            uriBuilder.queryParam("fields[]", field);
        }

        final List<String> filtersByFormula = new ArrayList<>();
        filter.getFilterByFormula()
                .ifPresent(filtersByFormula::add);
        filter.getModifiedAfter()
                .map(modifiedAfter -> "IS_AFTER(LAST_MODIFIED_TIME(),DATETIME_PARSE(\"" + modifiedAfter + "\"))")
                .ifPresent(filtersByFormula::add);
        filter.getModifiedBefore()
                .map(modifiedBefore -> "IS_BEFORE(LAST_MODIFIED_TIME(),DATETIME_PARSE(\"" + modifiedBefore + "\"))")
                .ifPresent(filtersByFormula::add);
        if (!filtersByFormula.isEmpty()) {
            uriBuilder.queryParam("filterByFormula", "AND(" + String.join(",", filtersByFormula) + ")");
        }
        filter.getOffset().ifPresent(offset -> uriBuilder.queryParam("offset", offset));
        filter.getPageSize().ifPresent(pageSize -> uriBuilder.queryParam("pageSize", pageSize));

        return uriBuilder.build();
    }
}
