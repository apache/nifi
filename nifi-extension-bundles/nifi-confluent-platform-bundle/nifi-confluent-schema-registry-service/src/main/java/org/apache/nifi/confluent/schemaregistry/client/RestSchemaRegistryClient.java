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

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.web.util.WebClientUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import javax.net.ssl.SSLContext;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * <p>
 * A Client for interacting with Confluent Schema Registry. We make use of Jersey Client to interact with the
 * Confluent Schema Registry REST API because the provided schema registry client does not provide a way to
 * use HTTPS for interacting with the schema registry (it assumes that system properties will be used, instead of
 * an SSLContext) and also does not allow configuration of (or use) timeouts. As a result, if the Schema Registry
 * crashed or was shut down, NiFi threads could be stuck indefinitely until NiFi is restarted. To avoid this,
 * we make use of Jersey Client and set timeouts appropriately.
 * </p>
 */
public class RestSchemaRegistryClient implements SchemaRegistryClient {

    private final List<String> baseUrls;
    private final Client client;
    private final ComponentLog logger;
    private final Map<String, String> httpHeaders;

    private static final String SUBJECT_FIELD_NAME = "subject";
    private static final String VERSION_FIELD_NAME = "version";
    private static final String ID_FIELD_NAME = "id";
    private static final String SCHEMA_TEXT_FIELD_NAME = "schema";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String SCHEMA_REGISTRY_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";


    public RestSchemaRegistryClient(final List<String> baseUrls,
                                    final int timeoutMillis,
                                    final SSLContext sslContext,
                                    final String username,
                                    final String password,
                                    final ComponentLog logger,
                                    final Map<String, String> httpHeaders) {
        this.baseUrls = new ArrayList<>(baseUrls);
        this.httpHeaders = httpHeaders;

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(ClientProperties.CONNECT_TIMEOUT, timeoutMillis);
        clientConfig.property(ClientProperties.READ_TIMEOUT, timeoutMillis);
        client = WebClientUtils.createClient(clientConfig, sslContext);

        if (StringUtils.isNoneBlank(username, password)) {
            client.register(HttpAuthenticationFeature.basic(username, password));
        }

        this.logger = logger;
    }


    @Override
    public RecordSchema getSchema(final String schemaName) throws IOException, SchemaNotFoundException {
        final String pathSuffix = getSubjectPath(schemaName, null);
        final JsonNode responseJson = fetchJsonResponse(pathSuffix, "name " + schemaName);

        return createRecordSchema(responseJson);
    }

    @Override
    public RecordSchema getSchema(final String schemaName, final int schemaVersion) throws IOException, SchemaNotFoundException {
        final String pathSuffix = getSubjectPath(schemaName, schemaVersion);
        final JsonNode responseJson = fetchJsonResponse(pathSuffix, "name " + schemaName);

        return createRecordSchema(responseJson);
    }

    @Override
    public RecordSchema getSchema(final int schemaId) throws IOException, SchemaNotFoundException {
        // The Confluent Schema Registry's version below 5.3.1 REST API does not provide us with the 'subject' (name) of a Schema given the ID.
        // It will provide us only the text of the Schema itself. Therefore, in order to determine the name (which is required for
        // a SchemaIdentifier), we must obtain a list of all Schema names, and then request each and every one of the schemas to determine
        // if the ID requested matches the Schema's ID.
        // To make this more efficient, we will cache a mapping of Schema Name to identifier, so that we can look this up more efficiently.

        // Check if we have cached the Identifier to Name mapping

        JsonNode completeSchema = null;

        // We get the schema definition using the ID of the schema
        // GET /schemas/ids/{int: id}
        final String schemaPath = getSchemaPath(schemaId);
        final JsonNode schemaJson = fetchJsonResponse(schemaPath, "id " + schemaId);

        // Get subject name by id, works only with v5.3.1+ Confluent Schema Registry
        // GET /schemas/ids/{int: id}/subjects
        JsonNode subjectsJson = null;
        try {
            subjectsJson = fetchJsonResponse(schemaPath + "/subjects", "schema name");

            if (subjectsJson != null) {
                final ArrayNode subjectsList = (ArrayNode) subjectsJson;
                for (JsonNode subject: subjectsList) {
                    final String searchName = subject.asText();
                    try {
                        // get complete schema (name + id + version) using the subject name API
                        completeSchema = postJsonResponse("/subjects/" + searchName, schemaJson, "schema id: " + schemaId);
                        break;
                    } catch (SchemaNotFoundException e) {
                        logger.debug("Could not find schema in registry by subject name {}", searchName, e);
                        continue;
                    }
                }
            }

        } catch (SchemaNotFoundException e) {
            logger.debug("Could not find schema metadata in registry by id and subjects in: {}", schemaPath);
        }

        // Get all couples (subject name, version) for a given schema ID
        // GET /schemas/ids/{int: id}/versions
        if (completeSchema == null) {
            try {
                JsonNode subjectsVersions = fetchJsonResponse(schemaPath + "/versions", "schema name");

                if (subjectsVersions != null) {
                    final ArrayNode subjectsVersionsList = (ArrayNode) subjectsVersions;
                    // we want to make sure we get the latest version
                    int maxVersion = 0;
                    String subjectName = null;
                    for (JsonNode subjectVersion: subjectsVersionsList) {
                        int currentVersion = subjectVersion.get(VERSION_FIELD_NAME).asInt();
                        String currentSubjectName = subjectVersion.get(SUBJECT_FIELD_NAME).asText();
                        if (currentVersion > maxVersion) {
                            maxVersion = currentVersion;
                            subjectName = currentSubjectName;
                        }
                    }

                    if (subjectName != null) {
                        return createRecordSchema(subjectName, maxVersion, schemaId, schemaJson.get(SCHEMA_TEXT_FIELD_NAME).asText());
                    }
                }
            } catch (SchemaNotFoundException e) {
                logger.debug("Could not find schema metadata in registry by id and versions in: {}", schemaPath);
            }
        }

        // Last resort option: we get the full list of subjects and check one by one to get the complete schema info
        if (completeSchema == null) {
            try {
                final JsonNode subjectsAllJson = fetchJsonResponse("/subjects", "subjects array");
                final ArrayNode subjectsAllList = (ArrayNode) subjectsAllJson;
                for (JsonNode subject: subjectsAllList) {
                    try {
                        final String searchName = subject.asText();
                        completeSchema = postJsonResponse("/subjects/" + searchName, schemaJson, "schema id: " + schemaId);
                        break;
                    } catch (SchemaNotFoundException e) {
                        continue;
                    }
                }
            } catch (SchemaNotFoundException e) {
                logger.debug("Could not find schema metadata in registry by iterating through subjects");
            }
        }

        // At this point, we could not get a subject/version associated to the schema and its ID
        // we add the schema and its ID in the cache without a subject/version
        if (completeSchema == null) {
            return createRecordSchema(null, null, schemaId, schemaJson.get(SCHEMA_TEXT_FIELD_NAME).asText());
        }

        return createRecordSchema(completeSchema);
    }

    private RecordSchema createRecordSchema(final String name, final Integer version, final int id, final String schema) throws SchemaNotFoundException {
        try {
            final Schema avroSchema = new Schema.Parser().parse(schema);
            final SchemaIdentifier schemaId = SchemaIdentifier.builder().name(name).id((long) id).version(version).build();
            return AvroTypeUtil.createSchema(avroSchema, schema, schemaId);
        } catch (final SchemaParseException spe) {
            throw new SchemaNotFoundException("Obtained Schema with id " + id + " and name " + name
                    + " from Confluent Schema Registry but the Schema Text that was returned is not a valid Avro Schema");
        }
    }

    private RecordSchema createRecordSchema(final JsonNode schemaNode) throws SchemaNotFoundException {
        final String subject = schemaNode.get(SUBJECT_FIELD_NAME).asText();
        final int version = schemaNode.get(VERSION_FIELD_NAME).asInt();
        final int id = schemaNode.get(ID_FIELD_NAME).asInt();
        final String schemaText = schemaNode.get(SCHEMA_TEXT_FIELD_NAME).asText();

        try {
            final Schema avroSchema = new Schema.Parser().parse(schemaText);
            final SchemaIdentifier schemaId = SchemaIdentifier.builder().name(subject).id((long) id).version(version).build();
            return AvroTypeUtil.createSchema(avroSchema, schemaText, schemaId);
        } catch (final SchemaParseException spe) {
            throw new SchemaNotFoundException("Obtained Schema with id " + id + " and name " + subject
                    + " from Confluent Schema Registry but the Schema Text that was returned is not a valid Avro Schema");
        }
    }

    private String getSubjectPath(final String schemaName, final Integer schemaVersion) {
        return "/subjects/" + URLEncoder.encode(schemaName, StandardCharsets.UTF_8) + "/versions/" +
                (schemaVersion == null ? "latest" : URLEncoder.encode(String.valueOf(schemaVersion), StandardCharsets.UTF_8));
    }

    private String getSchemaPath(final int schemaId) {
        return "/schemas/ids/" + URLEncoder.encode(String.valueOf(schemaId), StandardCharsets.UTF_8);
    }

    private JsonNode postJsonResponse(final String pathSuffix, final JsonNode schema, final String schemaDescription) throws SchemaNotFoundException {
        String errorMessage = null;
        for (final String baseUrl: baseUrls) {
            final String path = getPath(pathSuffix);
            final String trimmedBase = getTrimmedBase(baseUrl);
            final String url = trimmedBase + path;

            logger.debug("POST JSON response URL {}", url);

            final WebTarget webTarget = client.target(url);
            Invocation.Builder builder = webTarget.request().accept(MediaType.APPLICATION_JSON).header(CONTENT_TYPE_HEADER, SCHEMA_REGISTRY_CONTENT_TYPE);
            for (Map.Entry<String, String> header : httpHeaders.entrySet()) {
                builder = builder.header(header.getKey(), header.getValue());
            }
            final Response response = builder.post(Entity.json(schema.toString()));
            final int responseCode = response.getStatus();

            switch (Response.Status.fromStatusCode(responseCode)) {
                case OK:
                    JsonNode jsonResponse =  response.readEntity(JsonNode.class);

                    if (logger.isDebugEnabled()) {
                        logger.debug("JSON Response: {}", jsonResponse);
                    }

                    return jsonResponse;

                case NOT_FOUND:
                    logger.debug("Could not find Schema {} from Registry {}", schemaDescription, baseUrl);
                    continue;

                default:
                    errorMessage = response.readEntity(String.class);
                    continue;
            }
        }

        throw new SchemaNotFoundException("Failed to retrieve Schema with " + schemaDescription
                + " from any of the Confluent Schema Registry URL's provided; failure response message: "
                + errorMessage);
    }

    private JsonNode fetchJsonResponse(final String pathSuffix, final String schemaDescription) throws SchemaNotFoundException {
        String errorMessage = null;
        for (final String baseUrl : baseUrls) {
            final String path = getPath(pathSuffix);
            final String trimmedBase = getTrimmedBase(baseUrl);
            final String url = trimmedBase + path;

            logger.debug("GET JSON response URL {}", url);

            final WebTarget webTarget = client.target(url);
            Invocation.Builder builder = webTarget.request().accept(MediaType.APPLICATION_JSON);
            for (Map.Entry<String, String> header : httpHeaders.entrySet()) {
                builder = builder.header(header.getKey(), header.getValue());
            }
            final Response response = builder.get();
            final int responseCode = response.getStatus();

            switch (Response.Status.fromStatusCode(responseCode)) {
                case OK:
                    JsonNode jsonResponse =  response.readEntity(JsonNode.class);

                    if (logger.isDebugEnabled()) {
                        logger.debug("JSON Response {}", jsonResponse);
                    }

                    return jsonResponse;

                case NOT_FOUND:
                    logger.debug("Could not find Schema {} from Registry {}", schemaDescription, baseUrl);
                    continue;

                default:
                    errorMessage = response.readEntity(String.class);
                    continue;
            }
        }

        throw new SchemaNotFoundException("Failed to retrieve Schema with " + schemaDescription
                + " from any of the Confluent Schema Registry URL's provided; failure response message: " + errorMessage);
    }

    private String getTrimmedBase(String baseUrl) {
        return baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    private String getPath(String pathSuffix) {
        return pathSuffix.startsWith("/") ? pathSuffix : "/" + pathSuffix;
    }

}
