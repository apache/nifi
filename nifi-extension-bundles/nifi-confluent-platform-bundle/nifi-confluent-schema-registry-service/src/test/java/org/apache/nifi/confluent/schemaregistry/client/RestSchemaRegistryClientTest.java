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
package org.apache.nifi.confluent.schemaregistry.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import okhttp3.Headers;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.SchemaDefinition.SchemaType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"OptionalGetWithoutIsPresent", "SameParameterValue"})
@ExtendWith(MockitoExtension.class)
class RestSchemaRegistryClientTest {

    public static final String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";
    private static final int SCHEMA_ID = 123;
    private static final int SCHEMA_VERSION = 1;
    private static final String SUBJECT_NAME = "test-subject";
    private static final int REFERENCED_SCHEMA_VERSION = 2;
    private static final int REFERENCED_SCHEMA_ID = 456;
    private static final String REFERENCED_SUBJECT_NAME = "referenced-subject";
    private static final String REFERENCED_SCHEMA_NAME = "common.proto";
    private static final String PROTOBUF = "PROTOBUF";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String AVRO_SCHEMA_TEXT = """
        {
            "type": "record",
            "name": "User",
            "fields": [
                {
                    "name": "id",
                    "type": "int"
                },
                {
                    "name": "name",
                    "type": "string"
                },
                {
                    "name": "address",
                    "type": {
                        "type": "record",
                        "name": "Address",
                        "fields": [
                            { "name": "street", "type": "string" },
                            { "name": "city", "type": "string" },
                            { "name": "zip", "type": "int" }
                        ]
                    }
                }
            ]
        }""";

    private static final String PROTOBUF_SCHEMA_TEXT = """
        syntax = "proto3";

        package example;

        import "common.proto";

        message User {
          int32 id = 1;
          string name = 2;
          string email = 3;
          Address address = 4;
          google.protobuf.Timestamp created_at = 5;
          common.Status status = 6;
        }

        message Address {
          string street = 1;
          string city = 2;
          string state = 3;
          string zip_code = 4;
          string country = 5;
        }""";

    private static final String PROTOBUF_REFERENCED_SCHEMA_TEXT = """
        syntax = "proto3";
        package common;
        enum Status {
          UNKNOWN = 0;
          ACTIVE = 1;
          INACTIVE = 2;
          PENDING = 3;
        }""";

    private MockWebServer mockWebServer;
    private String baseUrl;
    private RestSchemaRegistryClient client;
    private ObjectMapper objectMapper;

    @Mock
    private ComponentLog logger;

    @BeforeEach
    void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        baseUrl = mockWebServer.url("/").toString();

        client = new RestSchemaRegistryClient(List.of(baseUrl), 30000, null, null, null, logger, Map.of());
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void tearDown() {
        if (mockWebServer != null) {
            mockWebServer.close();
        }
    }

    @Test
    void testGetSchemaByIdWithoutSubjectAndVersionInfo() throws IOException, SchemaNotFoundException, InterruptedException {
        /*
         * Client request sequence when no subject/version info is available:
         * 1. GET /schemas/ids/{id} - retrieves schema text (200 OK)
         * 2. GET /schemas/ids/{id}/subjects - tries to get subject info (404 Not Found)
         * 3. GET /schemas/ids/{id}/versions - fallback to get version info (404 Not Found)
         * 4. GET /subjects - fallback to get all subjects for manual search (404 Not Found)
         *
         * Expected result: Schema is returned but without subject name or version info
         */
        enqueueSchemaByIdResponse(AVRO_SCHEMA_TEXT);
        enqueueNotFoundResponse();
        enqueueNotFoundResponse();
        enqueueNotFoundResponse();

        RecordSchema schema = client.getSchema(SCHEMA_ID);

        assertNotNull(schema);
        assertFalse(schema.getIdentifier().getName().isPresent());
        assertEquals(SCHEMA_ID, schema.getIdentifier().getIdentifier().getAsLong());
        assertFalse(schema.getIdentifier().getVersion().isPresent());
        assertTrue(schema.getSchemaText().isPresent());

        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID);
        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID + "/subjects");
        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID + "/versions");
        verifyRequest("GET", "/subjects");
    }

    @Test
    void testGetSchemaByIdWithSubjectsEndpointSupport() throws IOException, SchemaNotFoundException, InterruptedException {
        // Set Username and Password example from RFC 7617 Section 2
        final String username = "Aladdin";
        final String password = "open sesame";
        final String expectedAuthorization = "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==";
        client = new RestSchemaRegistryClient(List.of(baseUrl), 30000, null, username, password, logger, Map.of());

        /*
         * Client request sequence when subjects endpoint is supported (Schema Registry v5.3.1+):
         * 1. GET /schemas/ids/{id} - retrieves schema text (200 OK)
         * 2. GET /schemas/ids/{id}/subjects - gets list of subjects using this schema (200 OK)
         * 3. POST /subjects/{subject} - gets complete schema info including version (200 OK)
         *
         * Expected result: Schema is returned with full subject name and version info
         */
        enqueueSchemaByIdResponse(AVRO_SCHEMA_TEXT);
        enqueueSubjectsResponse(List.of(SUBJECT_NAME));
        enqueueCompleteSchemaResponse(SUBJECT_NAME, SCHEMA_ID, SCHEMA_VERSION, AVRO_SCHEMA_TEXT);

        RecordSchema schema = client.getSchema(SCHEMA_ID);

        assertNotNull(schema);
        assertEquals(SUBJECT_NAME, schema.getIdentifier().getName().get());
        assertEquals(SCHEMA_ID, schema.getIdentifier().getIdentifier().getAsLong());
        assertEquals(SCHEMA_VERSION, schema.getIdentifier().getVersion().getAsInt());
        assertTrue(schema.getSchemaText().isPresent());

        final RecordedRequest schemaIdRequest = verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID);
        final Headers schemaIdRequestHeaders = schemaIdRequest.getHeaders();
        final String authorization = schemaIdRequestHeaders.get(AUTHORIZATION_HEADER);
        assertEquals(expectedAuthorization, authorization);

        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID + "/subjects");
        verifyRequest("POST", "/subjects/" + SUBJECT_NAME);
    }

    @Test
    void testGetSchemaByIdWithVersionsEndpointFallback() throws IOException, SchemaNotFoundException, InterruptedException {
        /*
         * Client request sequence when subjects endpoint is not available but versions endpoint works:
         * 1. GET /schemas/ids/{id} - retrieves schema text (200 OK)
         * 2. GET /schemas/ids/{id}/subjects - subjects endpoint not supported (404 Not Found)
         * 3. GET /schemas/ids/{id}/versions - fallback to versions endpoint (200 OK with subject/version info)
         *
         * Expected result: Schema is returned with subject name and version info from versions endpoint
         */
        enqueueSchemaByIdResponse(AVRO_SCHEMA_TEXT);
        enqueueNotFoundResponse();
        enqueueVersionsResponse(SUBJECT_NAME, SCHEMA_VERSION);

        RecordSchema schema = client.getSchema(SCHEMA_ID);

        assertNotNull(schema);
        assertEquals(SUBJECT_NAME, schema.getIdentifier().getName().get());
        assertEquals(SCHEMA_ID, schema.getIdentifier().getIdentifier().getAsLong());
        assertEquals(SCHEMA_VERSION, schema.getIdentifier().getVersion().getAsInt());
        assertTrue(schema.getSchemaText().isPresent());

        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID);
        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID + "/subjects");
        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID + "/versions");
    }

    @Test
    void testGetSchemaByIdWithAllSubjectsFallback() throws IOException, SchemaNotFoundException, InterruptedException {
        /*
         * Client request sequence when both subjects and versions endpoints fail, requiring full subject search:
         * 1. GET /schemas/ids/{id} - retrieves schema text (200 OK)
         * 2. GET /schemas/ids/{id}/subjects - subjects endpoint not supported (404 Not Found)
         * 3. GET /schemas/ids/{id}/versions - versions endpoint not supported (404 Not Found)
         * 4. GET /subjects - gets all available subjects for manual search (200 OK)
         * 5. POST /subjects/{subject} - tries first subject to find matching schema (200 OK - match found!)
         * 6. POST /subjects/other-subject - would try second subject but not needed since first matched
         *
         * Expected result: Schema is returned with subject name and version info found via subject search
         */
        enqueueSchemaByIdResponse(AVRO_SCHEMA_TEXT);
        enqueueNotFoundResponse();
        enqueueNotFoundResponse();
        enqueueAllSubjectsResponse(List.of(SUBJECT_NAME, "other-subject"));
        enqueueCompleteSchemaResponse(SUBJECT_NAME, SCHEMA_ID, SCHEMA_VERSION, AVRO_SCHEMA_TEXT);
        enqueueNotFoundResponse();

        RecordSchema schema = client.getSchema(SCHEMA_ID);

        assertNotNull(schema);
        assertEquals(SUBJECT_NAME, schema.getIdentifier().getName().get());
        assertEquals(SCHEMA_ID, schema.getIdentifier().getIdentifier().getAsLong());
        assertEquals(SCHEMA_VERSION, schema.getIdentifier().getVersion().getAsInt());
        assertTrue(schema.getSchemaText().isPresent());

        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID);
        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID + "/subjects");
        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID + "/versions");
        verifyRequest("GET", "/subjects");
        verifyRequest("POST", "/subjects/" + SUBJECT_NAME);
    }

    @Test
    void testGetSchemaByIdNotFound() throws InterruptedException {
        /*
         * Client request sequence when schema doesn't exist:
         * 1. GET /schemas/ids/{id} - schema not found (404 Not Found)
         *
         * Expected result: SchemaNotFoundException thrown immediately, no fallback attempts
         */
        enqueueNotFoundResponse();

        assertThrows(SchemaNotFoundException.class, () -> client.getSchema(SCHEMA_ID));

        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID);
    }

    @Test
    void testGetSchemaByIdServerError() throws InterruptedException {
        /*
         * Client request sequence when server error occurs:
         * 1. GET /schemas/ids/{id} - server error (500 Internal Server Error)
         *
         * Expected result: SchemaNotFoundException thrown immediately, no fallback attempts
         */
        enqueueServerErrorResponse("Internal Server Error");

        assertThrows(SchemaNotFoundException.class, () -> client.getSchema(SCHEMA_ID));

        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID);
    }

    @Test
    void testGetSchemaByIdInvalidAvroSchema() throws JsonProcessingException, InterruptedException {
        /*
         * Client request sequence when schema text is invalid/unparseable:
         * 1. GET /schemas/ids/{id} - retrieves invalid schema text (200 OK but invalid content)
         * 2. GET /schemas/ids/{id}/subjects - tries to get subject info for fallback (404 Not Found)
         * 3. GET /schemas/ids/{id}/versions - tries version fallback (404 Not Found)
         * 4. GET /subjects - tries all subjects fallback (404 Not Found)
         *
         * Expected result: SchemaNotFoundException thrown after all fallback attempts fail
         *
         * Note: Client exhausts all fallback options before giving up, even with invalid schema
         */
        enqueueSchemaByIdResponse("invalid-avro-schema");  // Response for request #1 (invalid but 200 OK)
        enqueueNotFoundResponse();
        enqueueNotFoundResponse();
        enqueueNotFoundResponse();

        assertThrows(SchemaNotFoundException.class, () -> client.getSchema(SCHEMA_ID));

        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID);
        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID + "/subjects");
        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID + "/versions");
        verifyRequest("GET", "/subjects");
    }

    @Test
    void testGetSchemaDefinitionWithProtobufAndReferences() throws IOException, SchemaNotFoundException, InterruptedException {
        /*
         * Client request sequence when fetching schema with references (e.g., Protobuf with imports):
         * 1. GET /schemas/ids/{id} - retrieves main schema with reference info (200 OK)
         * 2. GET /subjects/{referenced-subject}/versions/{version} - fetches referenced schema (200 OK)
         *
         * Expected result: SchemaDefinition returned with main schema and all referenced schemas resolved
         */
        SchemaReference reference = new SchemaReference(REFERENCED_SCHEMA_NAME, REFERENCED_SUBJECT_NAME, REFERENCED_SCHEMA_VERSION);
        enqueueSchemaByIdResponseWithReferences(PROTOBUF_SCHEMA_TEXT, List.of(reference));
        enqueueSchemaBySubjectVersionResponse(REFERENCED_SCHEMA_ID, REFERENCED_SUBJECT_NAME, PROTOBUF_REFERENCED_SCHEMA_TEXT, REFERENCED_SCHEMA_VERSION);

        SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().schemaVersionId((long) SCHEMA_ID).build();

        SchemaDefinition schemaDefinition = client.getSchemaDefinition(schemaIdentifier);

        assertNotNull(schemaDefinition);

        final SchemaIdentifier identifier = schemaDefinition.getIdentifier();
        assertTrue(identifier.getName().isEmpty());
        assertTrue(identifier.getVersion().isEmpty());

        assertEquals(SCHEMA_ID, identifier.getIdentifier().getAsLong());
        assertEquals(SchemaType.PROTOBUF, schemaDefinition.getSchemaType());
        assertEquals(PROTOBUF_SCHEMA_TEXT, schemaDefinition.getText());

        // Verify that references were fetched
        final Map<String, SchemaDefinition> references = schemaDefinition.getReferences();
        assertNotNull(references);
        assertEquals(1, references.size());
        SchemaDefinition referencedSchema = references.get(REFERENCED_SCHEMA_NAME);
        assertNotNull(referencedSchema);
        final SchemaIdentifier referencedId = referencedSchema.getIdentifier();
        assertEquals(REFERENCED_SUBJECT_NAME, referencedId.getName().get());
        assertEquals(REFERENCED_SCHEMA_ID, referencedId.getIdentifier().getAsLong());
        assertEquals(REFERENCED_SCHEMA_VERSION, referencedId.getVersion().getAsInt());

        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID);  // Request #1
        verifyRequest("GET", "/subjects/" + REFERENCED_SUBJECT_NAME + "/versions/" + REFERENCED_SCHEMA_VERSION);  // Request #2
    }

    @Test
    void testGetSchemaDefinitionNotFound() throws InterruptedException {
        /*
         * Client request sequence when schema definition doesn't exist:
         * 1. GET /schemas/ids/{id} - schema not found (404 Not Found)
         *
         * Expected result: SchemaNotFoundException thrown immediately
         */
        enqueueNotFoundResponse();

        SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().schemaVersionId((long) SCHEMA_ID).build();

        assertThrows(SchemaNotFoundException.class, () -> client.getSchemaDefinition(schemaIdentifier));

        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID);
    }

    @Test
    void testGetSchemaDefinitionServerError() throws InterruptedException {
        /*
         * Client request sequence when server error occurs during schema definition fetch:
         * 1. GET /schemas/ids/{id} - server error (500 Internal Server Error)
         *
         * Expected result: SchemaNotFoundException thrown immediately
         */
        enqueueServerErrorResponse("Internal Server Error");
        SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().schemaVersionId((long) SCHEMA_ID).build();

        assertThrows(SchemaNotFoundException.class, () -> client.getSchemaDefinition(schemaIdentifier));

        verifyRequest("GET", "/schemas/ids/" + SCHEMA_ID);
    }

    private void enqueueSchemaByIdResponse(String schemaText) throws JsonProcessingException {
        Map<String, String> response = Map.of("schema", schemaText);
        String jsonResponse = objectMapper.writeValueAsString(response);

        mockWebServer.enqueue(new MockResponse.Builder().code(200).addHeader("Content-Type", CONTENT_TYPE).body(jsonResponse).build());
    }

    private void enqueueSchemaByIdResponseWithReferences(String schemaText, List<SchemaReference> references) throws JsonProcessingException {
        SchemaResponse response = new SchemaResponse(schemaText, PROTOBUF, references);
        String jsonResponse = objectMapper.writeValueAsString(response);

        mockWebServer.enqueue(new MockResponse.Builder().code(200).addHeader("Content-Type", CONTENT_TYPE).body(jsonResponse).build());
    }

    private void enqueueSchemaBySubjectVersionResponse(int schemaId, String subject, String schemaText, int version) throws JsonProcessingException {
        CompleteSchemaResponse completeResponse = new CompleteSchemaResponse(subject, version, schemaId, schemaText, PROTOBUF, List.of());

        String jsonResponse = objectMapper.writeValueAsString(completeResponse);

        mockWebServer.enqueue(new MockResponse.Builder().code(200).addHeader("Content-Type", CONTENT_TYPE).body(jsonResponse).build());
    }

    private void enqueueCompleteSchemaResponse(String subjectName, int schemaId, int schemaVersion, String schemaText) throws JsonProcessingException {
        CompleteSchemaResponse response = new CompleteSchemaResponse(subjectName, schemaVersion, schemaId, schemaText, "AVRO", List.of());
        String jsonResponse = objectMapper.writeValueAsString(response);

        mockWebServer.enqueue(new MockResponse.Builder().code(200).addHeader("Content-Type", CONTENT_TYPE).body(jsonResponse).build());
    }

    private void enqueueSubjectsResponse(List<String> subjects) throws JsonProcessingException {
        String jsonResponse = objectMapper.writeValueAsString(subjects);

        mockWebServer.enqueue(new MockResponse.Builder().code(200).addHeader("Content-Type", CONTENT_TYPE).body(jsonResponse).build());
    }

    private void enqueueVersionsResponse(String subjectName, int schemaVersion) throws JsonProcessingException {
        record VersionInfo(String subject, int version) { }
        VersionInfo versionInfo = new VersionInfo(subjectName, schemaVersion);
        List<VersionInfo> response = List.of(versionInfo);
        String jsonResponse = objectMapper.writeValueAsString(response);

        mockWebServer.enqueue(new MockResponse.Builder().code(200).addHeader("Content-Type", CONTENT_TYPE).body(jsonResponse).build());
    }

    private void enqueueAllSubjectsResponse(List<String> subjects) throws JsonProcessingException {
        String jsonResponse = objectMapper.writeValueAsString(subjects);

        mockWebServer.enqueue(new MockResponse.Builder().code(200).addHeader("Content-Type", CONTENT_TYPE).body(jsonResponse).build());
    }

    private void enqueueNotFoundResponse() {
        mockWebServer.enqueue(new MockResponse.Builder().code(404).build());
    }

    private void enqueueServerErrorResponse(String errorMessage) {
        mockWebServer.enqueue(new MockResponse.Builder().code(500).body(errorMessage).build());
    }

    private RecordedRequest verifyRequest(String method, String expectedPath) throws InterruptedException {
        RecordedRequest request = mockWebServer.takeRequest();
        assertEquals(method, request.getMethod());
        assertEquals(expectedPath, request.getTarget());
        return request;
    }
}
