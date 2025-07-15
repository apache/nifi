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
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
@ExtendWith(MockitoExtension.class)
class RestSchemaRegistryClientTest {

    public static final String CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";
    private static final int SCHEMA_ID = 123;
    private static final int SCHEMA_VERSION = 1;
    private static final String SUBJECT_NAME = "test-subject";
    private static final int REFERENCED_SCHEMA_VERSION = 2;
    private static final int REFERENCED_SCHEMA_ID = 123;
    private static final String REFERENCED_SUBJECT_NAME = "referenced-subject";
    private static final String REFERENCED_SCHEMA_NAME = "common.proto";
    private static final String PROTOBUF = "PROTOBUF";
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
    private static final String REFERENCED_SCHEMA_TEXT = """
        syntax = "proto3";
                    
        package common;
                    
        enum Status {
          UNKNOWN = 0;
          ACTIVE = 1;
          INACTIVE = 2;
          PENDING = 3;
        }""";
    private WireMockServer wireMockServer;
    private RestSchemaRegistryClient client;
    private String baseUrl;
    private ObjectMapper objectMapper;

    @Mock
    private ComponentLog logger;

    @BeforeEach
    void setUp() {
        wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().port(0));
        wireMockServer.start();

        WireMock.configureFor("localhost", wireMockServer.port());
        baseUrl = "http://localhost:" + wireMockServer.port();

        client = new RestSchemaRegistryClient(List.of(baseUrl), 30000, null, null, null, logger, Map.of());
        objectMapper = new ObjectMapper();
    }

    @AfterEach
    void tearDown() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @Test
    void testGetSchemaByIdWithoutSubjectAndVersionInfo() throws IOException, SchemaNotFoundException {
        // Mock the primary schema endpoint
        stubSchemaByIdEndpoint(SCHEMA_ID, AVRO_SCHEMA_TEXT);

        // Mock all other endpoints to return 404 (not supported or no data)
        stubEndpointNotFound("/schemas/ids/" + SCHEMA_ID + "/subjects");
        stubEndpointNotFound("/schemas/ids/" + SCHEMA_ID + "/versions");
        stubEndpointNotFound("/subjects");

        RecordSchema schema = client.getSchema(SCHEMA_ID);

        // Verify the result (should still work but without subject/version info)
        assertNotNull(schema);
        assertFalse(schema.getIdentifier().getName().isPresent());
        assertEquals(SCHEMA_ID, schema.getIdentifier().getIdentifier().getAsLong());
        assertFalse(schema.getIdentifier().getVersion().isPresent());
        assertTrue(schema.getSchemaText().isPresent());

        // Verify that the expected endpoints were called
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID)));
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID + "/subjects")));
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID + "/versions")));
        verify(getRequestedFor(urlEqualTo("/subjects")));
    }


    @Test
    void testGetSchemaByIdWithSubjectsEndpointSupport() throws IOException, SchemaNotFoundException {
        stubSchemaByIdEndpoint(SCHEMA_ID, AVRO_SCHEMA_TEXT);

        // Mock the subjects endpoint (v5.3.1+ feature)
        stubSubjectsEndpoint(SCHEMA_ID, SUBJECT_NAME);

        stubCompleteSchemaEndpoint(SUBJECT_NAME, SCHEMA_ID, SCHEMA_VERSION, AVRO_SCHEMA_TEXT);

        RecordSchema schema = client.getSchema(SCHEMA_ID);

        assertNotNull(schema);
        assertEquals(SUBJECT_NAME, schema.getIdentifier().getName().get());
        assertEquals(SCHEMA_ID, schema.getIdentifier().getIdentifier().getAsLong());
        assertEquals(SCHEMA_VERSION, schema.getIdentifier().getVersion().getAsInt());
        assertTrue(schema.getSchemaText().isPresent());

        // Verify that the expected endpoints were called
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID)));
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID + "/subjects")));
        verify(postRequestedFor(urlEqualTo("/subjects/" + SUBJECT_NAME)));
    }

    @Test
    void testGetSchemaByIdWithVersionsEndpointFallback() throws IOException, SchemaNotFoundException {
        stubSchemaByIdEndpoint(SCHEMA_ID, AVRO_SCHEMA_TEXT);

        stubEndpointNotFound("/schemas/ids/" + SCHEMA_ID + "/subjects");
        stubVersionsEndpoint(SCHEMA_ID, SUBJECT_NAME, SCHEMA_VERSION);

        RecordSchema schema = client.getSchema(SCHEMA_ID);

        assertNotNull(schema);
        assertEquals(SUBJECT_NAME, schema.getIdentifier().getName().get());
        assertEquals(SCHEMA_ID, schema.getIdentifier().getIdentifier().getAsLong());
        assertEquals(SCHEMA_VERSION, schema.getIdentifier().getVersion().getAsInt());
        assertTrue(schema.getSchemaText().isPresent());

        // Verify that the expected endpoints were called
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID)));
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID + "/subjects")));
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID + "/versions")));
    }

    @Test
    void testGetSchemaByIdWithAllSubjectsFallback() throws IOException, SchemaNotFoundException {
        stubSchemaByIdEndpoint(SCHEMA_ID, AVRO_SCHEMA_TEXT);
        stubEndpointNotFound("/schemas/ids/" + SCHEMA_ID + "/subjects");
        stubEndpointNotFound("/schemas/ids/" + SCHEMA_ID + "/versions");
        stubAllSubjectsEndpoint(SUBJECT_NAME, "other-subject");
        stubCompleteSchemaEndpoint(SUBJECT_NAME, SCHEMA_ID, SCHEMA_VERSION, AVRO_SCHEMA_TEXT);
        stubPostEndpointNotFound("/subjects/other-subject");

        RecordSchema schema = client.getSchema(SCHEMA_ID);

        assertNotNull(schema);
        assertEquals(SUBJECT_NAME, schema.getIdentifier().getName().get());
        assertEquals(SCHEMA_ID, schema.getIdentifier().getIdentifier().getAsLong());
        assertEquals(SCHEMA_VERSION, schema.getIdentifier().getVersion().getAsInt());
        assertTrue(schema.getSchemaText().isPresent());

        // Verify that the expected endpoints were called
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID)));
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID + "/subjects")));
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID + "/versions")));
        verify(getRequestedFor(urlEqualTo("/subjects")));
        verify(postRequestedFor(urlEqualTo("/subjects/" + SUBJECT_NAME)));
    }


    @Test
    void testGetSchemaByIdNotFound() {

        stubEndpointNotFound("/schemas/ids/" + SCHEMA_ID);
        assertThrows(SchemaNotFoundException.class, () -> client.getSchema(SCHEMA_ID));

        // Verify that the expected endpoint was called        
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID)));
    }

    @Test
    void testGetSchemaByIdServerError() {

        stubEndpointServerError("/schemas/ids/" + SCHEMA_ID, "Internal Server Error");
        assertThrows(SchemaNotFoundException.class, () -> client.getSchema(SCHEMA_ID));

        // Verify that the expected endpoint was called
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID)));
    }

    @Test
    void testGetSchemaByIdInvalidAvroSchema() throws JsonProcessingException {
        stubSchemaByIdEndpoint(SCHEMA_ID, "invalid-avro-schema");
        stubEndpointNotFound("/schemas/ids/" + SCHEMA_ID + "/subjects");

        assertThrows(SchemaNotFoundException.class, () -> client.getSchema(SCHEMA_ID));

        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID)));
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID + "/subjects")));
    }

    @Test
    void testGetSchemaDefinitionWithProtobufAndReferences() throws IOException, SchemaNotFoundException {
        SchemaReference reference = new SchemaReference(REFERENCED_SCHEMA_NAME, REFERENCED_SUBJECT_NAME, REFERENCED_SCHEMA_VERSION);
        stubSchemaByIdEndpointWithReferences(SCHEMA_ID, PROTOBUF_SCHEMA_TEXT, List.of(reference));
        stubSchemaBySubjectVersionEndpoint(REFERENCED_SCHEMA_ID, REFERENCED_SUBJECT_NAME, REFERENCED_SCHEMA_TEXT, REFERENCED_SCHEMA_VERSION);

        SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().schemaVersionId((long) SCHEMA_ID).build();

        SchemaDefinition schemaDefinition = client.getSchemaDefinition(schemaIdentifier);

        assertNotNull(schemaDefinition);

        final SchemaIdentifier identifier = schemaDefinition.identifier();
        assertTrue(identifier.getName().isEmpty());
        assertTrue(identifier.getVersion().isEmpty());

        assertEquals(SCHEMA_ID, identifier.getIdentifier().getAsLong());
        assertEquals(SchemaType.PROTOBUF, schemaDefinition.getSchemaType());
        assertEquals(PROTOBUF_SCHEMA_TEXT, schemaDefinition.text());

        // Verify that references were fetched
        final Map<String, SchemaDefinition> references = schemaDefinition.getReferences();
        assertNotNull(references);
        assertEquals(1, references.size());
        SchemaDefinition referencedSchema = references.get(REFERENCED_SCHEMA_NAME);
        assertNotNull(referencedSchema);
        final SchemaIdentifier referencedId = referencedSchema.identifier();
        assertEquals(REFERENCED_SUBJECT_NAME, referencedId.getName().get());
        assertEquals(REFERENCED_SCHEMA_ID, referencedId.getIdentifier().getAsLong());
        assertEquals(REFERENCED_SCHEMA_VERSION, referencedId.getVersion().getAsInt());

        // Verify that the expected endpoints were called
        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID)));
    }

    @Test
    void testGetSchemaDefinitionNotFound() {
        stubEndpointNotFound("/schemas/ids/" + SCHEMA_ID);

        SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().schemaVersionId((long) SCHEMA_ID).build();

        assertThrows(SchemaNotFoundException.class, () -> client.getSchemaDefinition(schemaIdentifier));

        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID)));
    }

    @Test
    void testGetSchemaDefinitionServerError() {
        stubEndpointServerError("/schemas/ids/" + SCHEMA_ID, "Internal Server Error");

        SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder().schemaVersionId((long) SCHEMA_ID).build();

        assertThrows(SchemaNotFoundException.class, () -> client.getSchemaDefinition(schemaIdentifier));

        verify(getRequestedFor(urlEqualTo("/schemas/ids/" + SCHEMA_ID)));
    }

    private void stubSchemaByIdEndpoint(int schemaId, String schemaText) throws JsonProcessingException {
        Map<String, String> response = Map.of("schema", schemaText);
        String jsonResponse = objectMapper.writeValueAsString(response);

        stubFor(
            get(urlEqualTo("/schemas/ids/" + schemaId))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", CONTENT_TYPE)
                        .withBody(jsonResponse)
                )
        );
    }

    private void stubSchemaByIdEndpointWithReferences(int schemaId, String schemaText, List<SchemaReference> references) throws JsonProcessingException {
        SchemaResponse response = new SchemaResponse(schemaText, PROTOBUF, references);
        String jsonResponse = objectMapper.writeValueAsString(response);

        stubFor(
            get(urlEqualTo("/schemas/ids/" + schemaId))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", CONTENT_TYPE)
                        .withBody(jsonResponse)
                )
        );
    }

    private void stubSchemaBySubjectVersionEndpoint(int schemaId, String subject, String schemaText, int version) throws JsonProcessingException {
        CompleteSchemaResponse completeResponse = new CompleteSchemaResponse(subject, version, schemaId, schemaText, PROTOBUF, List.of());

        String jsonResponse = objectMapper.writeValueAsString(completeResponse);

        stubFor(
            get(urlEqualTo(format("/subjects/%s/versions/%d", subject, version)))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", CONTENT_TYPE)
                        .withBody(jsonResponse)
                )
        );
    }

    private void stubCompleteSchemaEndpoint(String subjectName, int schemaId, int schemaVersion, String schemaText) throws JsonProcessingException {
        Map<String, Object> response = Map.of("subject", subjectName, "version", schemaVersion, "id", schemaId, "schema", schemaText);
        String jsonResponse = objectMapper.writeValueAsString(response);

        stubFor(
            post(urlEqualTo("/subjects/" + subjectName))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", CONTENT_TYPE)
                        .withBody(jsonResponse)
                )
        );
    }

    private void stubSubjectsEndpoint(int schemaId, String... subjects) throws JsonProcessingException {
        List<String> subjectList = List.of(subjects);
        String jsonResponse = objectMapper.writeValueAsString(subjectList);

        stubFor(
            get(urlEqualTo("/schemas/ids/" + schemaId + "/subjects"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", CONTENT_TYPE)
                        .withBody(jsonResponse)
                )
        );
    }

    private void stubVersionsEndpoint(int schemaId, String subjectName, int schemaVersion) throws JsonProcessingException {
        Map<String, Object> versionInfo1 = Map.of("subject", subjectName, "version", schemaVersion);
        Map<String, Object> versionInfo2 = Map.of("subject", subjectName, "version", schemaVersion);
        List<Map<String, Object>> response = List.of(versionInfo1, versionInfo2);
        String jsonResponse = objectMapper.writeValueAsString(response);

        stubFor(
            get(urlEqualTo("/schemas/ids/" + schemaId + "/versions"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", CONTENT_TYPE)
                        .withBody(jsonResponse)
                )
        );
    }

    private void stubAllSubjectsEndpoint(String... subjects) throws JsonProcessingException {
        List<String> subjectList = List.of(subjects);
        String jsonResponse = objectMapper.writeValueAsString(subjectList);

        stubFor(
            get(urlEqualTo("/subjects"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", CONTENT_TYPE)
                        .withBody(jsonResponse)
                )
        );
    }

    private void stubEndpointNotFound(String url) {
        stubFor(
            get(urlEqualTo(url))
                .willReturn(
                    aResponse()
                        .withStatus(404)
                )
        );
    }

    private void stubPostEndpointNotFound(String url) {
        stubFor(
            post(urlEqualTo(url))
                .withHeader("Content-Type", equalTo(CONTENT_TYPE))
                .willReturn(
                    aResponse()
                        .withStatus(404)
                )
        );
    }

    private void stubEndpointServerError(String url, String errorMessage) {
        stubFor(
            get(urlEqualTo(url))
                .willReturn(
                    aResponse()
                        .withStatus(500)
                        .withBody(errorMessage)
                )
        );
    }

} 