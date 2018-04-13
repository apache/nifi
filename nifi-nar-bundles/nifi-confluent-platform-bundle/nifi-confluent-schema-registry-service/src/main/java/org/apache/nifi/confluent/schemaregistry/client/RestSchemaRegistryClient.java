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
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.web.util.WebUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;


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

    private static final String SUBJECT_FIELD_NAME = "subject";
    private static final String VERSION_FIELD_NAME = "version";
    private static final String ID_FIELD_NAME = "id";
    private static final String SCHEMA_TEXT_FIELD_NAME = "schema";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String SCHEMA_REGISTRY_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";


    public RestSchemaRegistryClient(final List<String> baseUrls, final int timeoutMillis, final SSLContext sslContext, final ComponentLog logger) {
        this.baseUrls = new ArrayList<>(baseUrls);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.property(ClientProperties.CONNECT_TIMEOUT, timeoutMillis);
        clientConfig.property(ClientProperties.READ_TIMEOUT, timeoutMillis);
        client = WebUtils.createClient(clientConfig, sslContext);

        this.logger = logger;
    }


    @Override
    public RecordSchema getSchema(final String schemaName) throws IOException, SchemaNotFoundException {
        final String pathSuffix = getSubjectPath(schemaName);
        final JsonNode responseJson = fetchJsonResponse(pathSuffix, "name " + schemaName);

        final RecordSchema recordSchema = createRecordSchema(responseJson);
        return recordSchema;
    }


    @Override
    public RecordSchema getSchema(final int schemaId) throws IOException, SchemaNotFoundException {
        // The Confluent Schema Registry's REST API does not provide us with the 'subject' (name) of a Schema given the ID.
        // It will provide us only the text of the Schema itself. Therefore, in order to determine the name (which is required for
        // a SchemaIdentifier), we must obtain a list of all Schema names, and then request each and every one of the schemas to determine
        // if the ID requested matches the Schema's ID.
        // To make this more efficient, we will cache a mapping of Schema Name to identifier, so that we can look this up more efficiently.

        // Check if we have cached the Identifier to Name mapping

        final String schemaPath = getSchemaPath(schemaId);
        final JsonNode responseJson = fetchJsonResponse(schemaPath, "id " + schemaId);
        final JsonNode subjectsJson = fetchJsonResponse("/subjects", "subjects array");
        final ArrayNode subjectsList = (ArrayNode) subjectsJson;

        JsonNode completeSchema = null;
        for (JsonNode subject: subjectsList) {
            try {
                final String subjectName = subject.asText();
                completeSchema = postJsonResponse("/subjects/" + subjectName, responseJson, "schema id: " + schemaId);
                break;
            } catch (SchemaNotFoundException e) {
                continue;
            }

        }

        if(completeSchema == null) {
            throw new SchemaNotFoundException("could not get schema with id: " + schemaId);
        }

        final RecordSchema recordSchema = createRecordSchema(completeSchema);
        return recordSchema;
    }

    private RecordSchema createRecordSchema(final JsonNode schemaNode) throws SchemaNotFoundException {
        final String subject = schemaNode.get(SUBJECT_FIELD_NAME).asText();
        final int version = schemaNode.get(VERSION_FIELD_NAME).asInt();
        final int id = schemaNode.get(ID_FIELD_NAME).asInt();
        final String schemaText = schemaNode.get(SCHEMA_TEXT_FIELD_NAME).asText();

        try {
            final Schema avroSchema = new Schema.Parser().parse(schemaText);
            final SchemaIdentifier schemaId = SchemaIdentifier.builder().name(subject).id(Long.valueOf(id)).version(version).build();

            final RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema, schemaText, schemaId);
            return recordSchema;
        } catch (final SchemaParseException spe) {
            throw new SchemaNotFoundException("Obtained Schema with id " + id + " and name " + subject
                + " from Confluent Schema Registry but the Schema Text that was returned is not a valid Avro Schema");
        }
    }

    private String getSubjectPath(final String schemaName) throws UnsupportedEncodingException {
        return "/subjects/" + URLEncoder.encode(schemaName, "UTF-8") + "/versions/latest";
    }

    private String getSchemaPath(final int schemaId) throws UnsupportedEncodingException {
        return "/schemas/ids/" + URLEncoder.encode(String.valueOf(schemaId), "UTF-8");
    }

    private JsonNode postJsonResponse(final String pathSuffix, final JsonNode schema, final String schemaDescription) throws SchemaNotFoundException {
        String errorMessage = null;
        for(final String baseUrl: baseUrls) {
            final String path = getPath(pathSuffix);
            final String trimmedBase = getTrimmedBase(baseUrl);
            final String url = trimmedBase + path;
            final WebTarget builder = client.target(url);
            final Response response = builder.request().accept(MediaType.APPLICATION_JSON).header(CONTENT_TYPE_HEADER, SCHEMA_REGISTRY_CONTENT_TYPE).post(Entity.json(schema.toString()));
            final int responseCode = response.getStatus();

            if (responseCode == Response.Status.NOT_FOUND.getStatusCode()) {
                continue;
            }

            if(responseCode == Response.Status.OK.getStatusCode()) {
                final JsonNode responseJson = response.readEntity(JsonNode.class);
                return responseJson;
            }
        }

        throw new SchemaNotFoundException("Failed to retrieve Schema with " + schemaDescription + " from any of the Confluent Schema Registry URL's provided; failure response message: "
                + errorMessage);
    }

    private JsonNode fetchJsonResponse(final String pathSuffix, final String schemaDescription) throws SchemaNotFoundException, IOException {
        String errorMessage = null;
        for (final String baseUrl : baseUrls) {
            final String path = getPath(pathSuffix);
            final String trimmedBase = getTrimmedBase(baseUrl);
            final String url = trimmedBase + path;

            final WebTarget webTarget = client.target(url);
            final Response response = webTarget.request().accept(MediaType.APPLICATION_JSON).get();
            final int responseCode = response.getStatus();

            if (responseCode == Response.Status.OK.getStatusCode()) {
                final JsonNode responseJson = response.readEntity(JsonNode.class);
                return responseJson;
            }

            if (responseCode == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new SchemaNotFoundException("Could not find Schema with " + schemaDescription + " from the Confluent Schema Registry located at " + baseUrl);
            }

            if (errorMessage == null) {
                errorMessage = response.readEntity(String.class);
            }
        }

        throw new IOException("Failed to retrieve Schema with " + schemaDescription + " from any of the Confluent Schema Registry URL's provided; failure response message: " + errorMessage);
    }

    private String getTrimmedBase(String baseUrl) {
        return baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    private String getPath(String pathSuffix) {
        return pathSuffix.startsWith("/") ? pathSuffix : "/" + pathSuffix;
    }

}
