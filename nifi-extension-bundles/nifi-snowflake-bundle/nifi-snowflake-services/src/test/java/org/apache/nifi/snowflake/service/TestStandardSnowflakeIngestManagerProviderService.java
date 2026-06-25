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
package org.apache.nifi.snowflake.service;

import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import mockwebserver3.junit5.StartStop;
import okio.ByteString;
import org.apache.nifi.processors.snowflake.snowpipe.InsertFile;
import org.apache.nifi.processors.snowflake.snowpipe.InsertFileStatus;
import org.apache.nifi.processors.snowflake.snowpipe.InsertFiles;
import org.apache.nifi.processors.snowflake.snowpipe.InsertReport;
import org.apache.nifi.processors.snowflake.util.SnowflakeProperties;
import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.web.client.StandardWebClientService;
import org.apache.nifi.web.client.api.WebClientService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.HttpURLConnection;
import java.net.URI;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateCrtKey;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(15)
public class TestStandardSnowflakeIngestManagerProviderService {

    private static final String KEY_ALGORITHM = "RSA";

    private static final String ACCOUNT = "ACCOUNT";

    private static final String USER = "USER";

    private static final String PIPE_NAME = "DB.SCHEMA.PIPE";

    private static final String STAGED_FILE_PATH = "staged-file.csv";

    private static final String POST_METHOD = "POST";

    private static final String GET_METHOD = "GET";

    private static final String AUTHORIZATION_HEADER = "Authorization";

    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final String APPLICATION_JSON = "application/json";

    private static final String BEARER_PREFIX = "Bearer ";

    private static final String INSERT_FILES_PATH_PREFIX = "/v1/data/pipes/DB.SCHEMA.PIPE/insertFiles";

    private static final String INSERT_REPORT_PATH_PREFIX = "/v1/data/pipes/DB.SCHEMA.PIPE/insertReport";

    private static final String HTTP_URI_FORMAT = "http://%s:%d";

    private static final String REQUEST_ID_PARAMETER = "requestId=";

    private static final String PIPE_NOT_RECOGNIZED_BODY = "Pipe not recognized";

    private static final String INSERT_FILES_SUCCESS_RESPONSE = """
            {"requestId":"id","status":"success"}""";

    private static final String INSERT_REPORT_RESPONSE = """
            {
                "pipe": "DB.SCHEMA.PIPE",
                "completeResult": true,
                "files": [
                    {
                        "path": "staged-file.csv",
                        "complete": true,
                        "errorsSeen": 0,
                        "status": "LOADED"
                    }
                ]
            }""";

    @StartStop
    public final MockWebServer mockWebServer = new MockWebServer();

    private StandardWebClientService webClientService;

    private SnowpipeIngestClient client;

    @BeforeEach
    void setUp() throws NoSuchAlgorithmException {
        webClientService = new StandardWebClientService();
        client = createClient(mockWebServer, webClientService);
    }

    @AfterEach
    void closeClient() {
        webClientService.close();
    }

    @Test
    void testMigrateProperties() {
        final StandardSnowflakeIngestManagerProviderService service = new StandardSnowflakeIngestManagerProviderService();
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("account-identifier-format", StandardSnowflakeIngestManagerProviderService.ACCOUNT_IDENTIFIER_FORMAT.getName()),
                Map.entry("host-url", StandardSnowflakeIngestManagerProviderService.HOST_URL.getName()),
                Map.entry("user-name", StandardSnowflakeIngestManagerProviderService.USER_NAME.getName()),
                Map.entry("private-key-service", StandardSnowflakeIngestManagerProviderService.PRIVATE_KEY_SERVICE.getName()),
                Map.entry("pipe", StandardSnowflakeIngestManagerProviderService.PIPE.getName()),
                Map.entry(SnowflakeProperties.OLD_ACCOUNT_LOCATOR_PROPERTY_NAME, SnowflakeProperties.ACCOUNT_LOCATOR.getName()),
                Map.entry(SnowflakeProperties.OLD_CLOUD_REGION_PROPERTY_NAME, SnowflakeProperties.CLOUD_REGION.getName()),
                Map.entry(SnowflakeProperties.OLD_CLOUD_TYPE_PROPERTY_NAME, SnowflakeProperties.CLOUD_TYPE.getName()),
                Map.entry(SnowflakeProperties.OLD_ORGANIZATION_NAME_PROPERTY_NAME, SnowflakeProperties.ORGANIZATION_NAME.getName()),
                Map.entry(SnowflakeProperties.OLD_ACCOUNT_NAME_PROPERTY_NAME, SnowflakeProperties.ACCOUNT_NAME.getName()),
                Map.entry(SnowflakeProperties.OLD_DATABASE_PROPERTY_NAME, SnowflakeProperties.DATABASE.getName()),
                Map.entry(SnowflakeProperties.OLD_SCHEMA_PROPERTY_NAME, SnowflakeProperties.SCHEMA.getName())
        );

        final Map<String, String> propertyValues = Map.of();
        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        service.migrateProperties(configuration);

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expectedRenamed, propertiesRenamed);

        final Set<MockPropertyConfiguration.CreatedControllerService> createdControllerServices = result.getCreatedControllerServices();
        assertFalse(createdControllerServices.isEmpty());

        final MockPropertyConfiguration.CreatedControllerService createdControllerService = createdControllerServices.iterator().next();
        assertTrue(createdControllerService.implementationClassName().endsWith("StandardWebClientServiceProvider"));
    }

    @Test
    void testInsertFilesRequest() throws Exception {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .addHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON)
                .body(INSERT_FILES_SUCCESS_RESPONSE)
                .build());

        final InsertFiles insertFiles = new InsertFiles(List.of(new InsertFile(STAGED_FILE_PATH)));
        client.insertFiles(insertFiles);

        final RecordedRequest request = mockWebServer.takeRequest();
        assertEquals(POST_METHOD, request.getMethod());
        final String target = request.getTarget();
        assertNotNull(target);
        assertTrue(target.startsWith(INSERT_FILES_PATH_PREFIX));
        assertTrue(target.contains(REQUEST_ID_PARAMETER));

        final ByteString requestBody = request.getBody();
        assertNotNull(requestBody);
        final String body = requestBody.utf8();
        assertTrue(body.contains(STAGED_FILE_PATH));

        final String authHeader = request.getHeaders().get(AUTHORIZATION_HEADER);
        assertNotNull(authHeader);
        assertTrue(authHeader.startsWith(BEARER_PREFIX));
    }

    @Test
    void testGetInsertReportRequest() throws Exception {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .addHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON)
                .body(INSERT_REPORT_RESPONSE)
                .build());

        final InsertReport response = client.getInsertReport();

        final RecordedRequest request = mockWebServer.takeRequest();
        assertEquals(GET_METHOD, request.getMethod());
        final String target = request.getTarget();
        assertNotNull(target);
        assertTrue(target.startsWith(INSERT_REPORT_PATH_PREFIX));

        assertNotNull(response);
        final List<InsertFileStatus> files = response.files();
        assertEquals(1, files.size());
        assertEquals(STAGED_FILE_PATH, files.getFirst().path());
        assertTrue(files.getFirst().complete());
    }

    @Test
    void testInsertFilesErrorResponse() {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_NOT_FOUND)
                .body(PIPE_NOT_RECOGNIZED_BODY)
                .build());

        final InsertFiles insertFiles = new InsertFiles(List.of(new InsertFile(STAGED_FILE_PATH)));
        final RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> client.insertFiles(insertFiles)
        );
        assertTrue(exception.getMessage().contains(String.valueOf(HttpURLConnection.HTTP_NOT_FOUND)));
    }

    private static SnowpipeIngestClient createClient(final MockWebServer mockWebServer, final WebClientService webClientService) throws NoSuchAlgorithmException {
        final URI baseUri = URI.create(HTTP_URI_FORMAT.formatted(mockWebServer.getHostName(), mockWebServer.getPort()));
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final RSAPrivateCrtKey privateKey = (RSAPrivateCrtKey) keyPair.getPrivate();
        final RSAKeyAuthorizationProvider authProvider = new RSAKeyAuthorizationProvider(ACCOUNT, USER, privateKey);
        return new SnowpipeIngestClient(baseUri, PIPE_NAME, authProvider, webClientService);
    }
}
