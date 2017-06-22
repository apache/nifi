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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.net.ssl.SSLContext;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.web.util.WebUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;


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

    private static final String SUBJECT_FIELD_NAME = "subject";
    private static final String VERSION_FIELD_NAME = "version";
    private static final String ID_FIELD_NAME = "id";
    private static final String SCHEMA_TEXT_FIELD_NAME = "schema";

    private final ConcurrentMap<String, Integer> schemaNameToIdentifierMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<Integer, String> schemaIdentifierToNameMap = new ConcurrentHashMap<>();


    public RestSchemaRegistryClient(final List<String> baseUrls, final int timeoutMillis, final SSLContext sslContext) {
        this.baseUrls = new ArrayList<>(baseUrls);

        final ClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getProperties().put(ClientConfig.PROPERTY_CONNECT_TIMEOUT, timeoutMillis);
        clientConfig.getProperties().put(ClientConfig.PROPERTY_READ_TIMEOUT, timeoutMillis);
        client = WebUtils.createClient(clientConfig, sslContext);
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
        final String schemaName = schemaIdentifierToNameMap.get(schemaId);
        if (schemaName != null) {
            return getSchema(schemaName);
        }

        final String schemaDescription = "identifier " + schemaId;
        final JsonNode schemaNameArray = fetchJsonResponse("/subjects", schemaDescription);
        if (!schemaNameArray.isArray()) {
            throw new IOException("When determining Subjects that are available, expected a JSON Array but did not receive a valid response");
        }

        final ArrayNode arrayNode = (ArrayNode) schemaNameArray;
        for (final JsonNode node : arrayNode) {
            final String nodeName = node.getTextValue();

            final String schemaPath = getSubjectPath(nodeName);
            final JsonNode schemaNode = fetchJsonResponse(schemaPath, schemaDescription);

            final int id = schemaNode.get(ID_FIELD_NAME).asInt();
            schemaNameToIdentifierMap.put(nodeName, id);
            schemaIdentifierToNameMap.put(id, nodeName);

            if (id == schemaId) {
                return createRecordSchema(schemaNode);
            }
        }

        throw new SchemaNotFoundException("Could not find a schema with identifier " + schemaId);
    }

    private RecordSchema createRecordSchema(final JsonNode schemaNode) throws SchemaNotFoundException {
        final String subject = schemaNode.get(SUBJECT_FIELD_NAME).getTextValue();
        final int version = schemaNode.get(VERSION_FIELD_NAME).asInt();
        final int id = schemaNode.get(ID_FIELD_NAME).asInt();
        final String schemaText = schemaNode.get(SCHEMA_TEXT_FIELD_NAME).getTextValue();

        try {
            final Schema avroSchema = new Schema.Parser().parse(schemaText);
            final SchemaIdentifier schemaId = SchemaIdentifier.of(subject, id, version);

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

    private JsonNode fetchJsonResponse(final String pathSuffix, final String schemaDescription) throws SchemaNotFoundException, IOException {
        String errorMessage = null;
        for (final String baseUrl : baseUrls) {
            final String path = pathSuffix.startsWith("/") ? pathSuffix : "/" + pathSuffix;
            final String trimmedBase = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
            final String url = trimmedBase + path;

            final WebResource.Builder builder = client.resource(url).accept(MediaType.APPLICATION_JSON);
            final ClientResponse response = builder.get(ClientResponse.class);
            final int responseCode = response.getStatus();

            if (responseCode == Response.Status.OK.getStatusCode()) {
                final JsonNode responseJson = response.getEntity(JsonNode.class);
                return responseJson;
            }

            if (responseCode == Response.Status.NOT_FOUND.getStatusCode()) {
                throw new SchemaNotFoundException("Could not find Schema with " + schemaDescription + " from the Confluent Schema Registry located at " + baseUrl);
            }

            if (errorMessage == null) {
                errorMessage = response.getEntity(String.class);
            }
        }

        throw new IOException("Failed to retrieve Schema with " + schemaDescription + " from any of the Confluent Schema Registry URL's provided; failure response message: " + errorMessage);
    }

}
