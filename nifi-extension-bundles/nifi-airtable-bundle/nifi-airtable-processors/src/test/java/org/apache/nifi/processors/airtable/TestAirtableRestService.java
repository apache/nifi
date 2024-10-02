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
package org.apache.nifi.processors.airtable;

import org.apache.nifi.processors.airtable.service.AirtableRestService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.apache.nifi.web.client.provider.service.StandardWebClientServiceProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestAirtableRestService {

    private static final String API_URL_WITHOUT_SLASH = "https://api.airtable.com/v0";
    private static final String API_URL_WITH_SLASH = "https://api.airtable.com/v0/";
    private static final String PAT = "pat";
    private static final String BASE_ID = "base-id";
    private static final String TABLE_ID = "table-id";
    private static final String EXPECTED_URL = String.format("%s/%s/%s", API_URL_WITHOUT_SLASH, BASE_ID, TABLE_ID);

    private final WebClientServiceProvider webClientServiceProvider = new StandardWebClientServiceProvider();

    @Test
    void testApiUrlEndsWithoutSlash() {
        AirtableRestService serviceWithoutSlash = new AirtableRestService(webClientServiceProvider, API_URL_WITHOUT_SLASH, PAT, BASE_ID, TABLE_ID);
        String apiUrlWithSlash = serviceWithoutSlash.createUriBuilder().build().toString();
        assertEquals(EXPECTED_URL, apiUrlWithSlash);
    }

    @Test
    void testApiUrlEndsWithSlash() {
        AirtableRestService serviceWithSlash = new AirtableRestService(webClientServiceProvider, API_URL_WITH_SLASH, PAT, BASE_ID, TABLE_ID);
        String apiUrlWithSlash = serviceWithSlash.createUriBuilder().build().toString();
        assertEquals(EXPECTED_URL, apiUrlWithSlash);
    }
}

