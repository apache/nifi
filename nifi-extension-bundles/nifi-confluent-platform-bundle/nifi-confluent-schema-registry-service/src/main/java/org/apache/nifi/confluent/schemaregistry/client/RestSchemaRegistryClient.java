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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.StandardSchemaDefinition;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.web.client.StandardWebClientService;
import org.apache.nifi.web.client.api.HttpHeaderName;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.ssl.TlsContext;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.nifi.schemaregistry.services.SchemaDefinition.SchemaType;

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
    private final ComponentLog logger;
    private final Map<String, String> httpHeaders;
    private final WebClientService webClientService;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String SUBJECT_FIELD_NAME = "subject";
    private static final String VERSION_FIELD_NAME = "version";
    private static final String ID_FIELD_NAME = "id";
    private static final String SCHEMA_TEXT_FIELD_NAME = "schema";
    private static final String SCHEMA_TYPE_FIELD_NAME = "schemaType";
    private static final String SCHEMA_REGISTRY_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";
    private static final String REFERENCES_FIELD_NAME = "references";
    private static final String REFERENCE_NAME_FIELD_NAME = "name";
    private static final String REFERENCE_SUBJECT_FIELD_NAME = "subject";
    private static final String REFERENCE_VERSION_FIELD_NAME = "version";
    private static final String APPLICATION_JSON_CONTENT_TYPE = "application/json";
    private static final String BASIC_CREDENTIALS_FORMAT = "%s:%s";
    private static final String BASIC_AUTHORIZATION_FORMAT = "Basic %s";

    public RestSchemaRegistryClient(final List<String> baseUrls,
                                    final int timeoutMillis,
                                    final SSLContextProvider sslContextProvider,
                                    final String username,
                                    final String password,
                                    final ComponentLog logger,
                                    final Map<String, String> httpHeaders) {
        this.baseUrls = new ArrayList<>(baseUrls);
        this.httpHeaders = new HashMap<>(httpHeaders);

        if (StringUtils.isNoneBlank(username, password)) {
            final String credentials = BASIC_CREDENTIALS_FORMAT.formatted(username, password);
            final byte[] credentialsEncoded = credentials.getBytes(StandardCharsets.UTF_8);
            final String authorization = Base64.getEncoder().encodeToString(credentialsEncoded);
            final String basicAuthorization = BASIC_AUTHORIZATION_FORMAT.formatted(authorization);
            this.httpHeaders.put(HttpHeaderName.AUTHORIZATION.getHeaderName(), basicAuthorization);
        }

        final StandardWebClientService standardWebClientService = new StandardWebClientService();
        final Duration timeout = Duration.ofMillis(timeoutMillis);
        standardWebClientService.setConnectTimeout(timeout);
        standardWebClientService.setReadTimeout(timeout);

        if (sslContextProvider != null) {
            final Optional<X509KeyManager> keyManager = sslContextProvider.createKeyManager().map(X509KeyManager.class::cast);
            final X509TrustManager trustManager = sslContextProvider.createTrustManager();
            final SSLContext sslContext = sslContextProvider.createContext();

            final TlsContext tlsContext = new TlsContext() {
                @Override
                public String getProtocol() {
                    return sslContext.getProtocol();
                }

                @Override
                public X509TrustManager getTrustManager() {
                    return trustManager;
                }

                @Override
                public Optional<X509KeyManager> getKeyManager() {
                    return keyManager;
                }
            };
            standardWebClientService.setTlsContext(tlsContext);
        }

        webClientService = standardWebClientService;

        this.logger = logger;
    }


    @Override
    public RecordSchema getSchema(final String schemaName) throws SchemaNotFoundException {
        final String pathSuffix = getSubjectPath(schemaName, null);
        final JsonNode responseJson = fetchJsonResponse(pathSuffix, "name " + schemaName);

        return createRecordSchema(responseJson);
    }

    @Override
    public RecordSchema getSchema(final String schemaName, final int schemaVersion) throws SchemaNotFoundException {
        final String pathSuffix = getSubjectPath(schemaName, schemaVersion);
        final JsonNode responseJson = fetchJsonResponse(pathSuffix, "name " + schemaName);

        return createRecordSchema(responseJson);
    }

    @Override
    public RecordSchema getSchema(final int schemaId) throws SchemaNotFoundException {
        // The Confluent Schema Registry's version below 5.3.1 REST API does not provide us with the 'subject' (name) of a Schema given the ID.
        // It will provide us only the text of the Schema itself. Therefore, in order to determine the name (which is required for
        // a SchemaIdentifier), we must obtain a list of all Schema names, and then request each one of the schemas to determine
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
        JsonNode subjectsJson;
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
                    } catch (SchemaNotFoundException ignored) {
                        // Trying next schema location
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

    @Override
    public SchemaDefinition getSchemaDefinition(SchemaIdentifier identifier) throws SchemaNotFoundException {
        final JsonNode schemaJson;
        String subject = null;
        Integer version = null;

        // If we have an ID, get the schema by ID first
        // Using schemaVersionId, because that is what is set by ConfluentEncodedSchemaReferenceReader.
        // probably identifier field should be used, but I'm not changing ConfluentEncodedSchemaReferenceReader for backward compatibility reasons.
        if (identifier.getSchemaVersionId().isPresent()) {
            long schemaId = identifier.getSchemaVersionId().getAsLong();
            String schemaPath = getSchemaPath(schemaId);
            schemaJson = fetchJsonResponse(schemaPath, "id " + schemaId);
        } else if (identifier.getName().isPresent()) {
            // If we have a name or (name and version), get the schema by those
            subject = identifier.getName().get();
            version = identifier.getVersion().isPresent() ? identifier.getVersion().getAsInt() : null;
            // if no version was specified, the latest version will be used. See @getSubjectPath method.
            String pathSuffix = getSubjectPath(subject, version);
            schemaJson = fetchJsonResponse(pathSuffix, "name " + subject);
        } else {
            throw new SchemaNotFoundException("Schema identifier must contain either a version identifier or a subject name");
        }

        // Extract schema information
        String schemaText = schemaJson.get(SCHEMA_TEXT_FIELD_NAME).asText();
        String schemaTypeText = schemaJson.get(SCHEMA_TYPE_FIELD_NAME).asText();
        SchemaType schemaType = toSchemaType(schemaTypeText);

        long schemaId;
        if (schemaJson.has(ID_FIELD_NAME)) {
            schemaId = schemaJson.get(ID_FIELD_NAME).asLong();
        } else {
            schemaId = identifier.getSchemaVersionId().getAsLong();
        }

        if (subject == null && schemaJson.has(SUBJECT_FIELD_NAME)) {
            subject = schemaJson.get(SUBJECT_FIELD_NAME).asText();
        }

        if (version == null && schemaJson.has(VERSION_FIELD_NAME)) {
            version = schemaJson.get(VERSION_FIELD_NAME).asInt();
        }

        // Build schema identifier with all available information
        SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder()
            .id(schemaId)
            .name(subject)
            .version(version)
            .build();

        // Process references if present
        Map<String, SchemaDefinition> references = new HashMap<>();
        if (schemaJson.has(REFERENCES_FIELD_NAME) && !schemaJson.get(REFERENCES_FIELD_NAME).isNull()) {
            ArrayNode refsArray = (ArrayNode) schemaJson.get(REFERENCES_FIELD_NAME);
            for (JsonNode ref : refsArray) {
                String refName = ref.get(REFERENCE_NAME_FIELD_NAME).asText();
                String refSubject = ref.get(REFERENCE_SUBJECT_FIELD_NAME).asText();
                int refVersion = ref.get(REFERENCE_VERSION_FIELD_NAME).asInt();

                // Recursively get referenced schema
                SchemaIdentifier refId = SchemaIdentifier.builder()
                    .name(refSubject)
                    .version(refVersion)
                    .build();
                SchemaDefinition refSchema = getSchemaDefinition(refId);
                references.put(refName, refSchema);
            }
        }

        return new StandardSchemaDefinition(schemaIdentifier, schemaText, schemaType, references);
    }

    private SchemaType toSchemaType(final String schemaTypeText) {
        try {
            if (schemaTypeText == null || schemaTypeText.isEmpty()) {
                return SchemaType.AVRO; // Default schema type for confluent schema registry is AVRO.
            }
            return SchemaType.valueOf(schemaTypeText.toUpperCase().trim());
        } catch (final Exception e) {
            final String message = String.format("Could not convert schema type '%s' to SchemaType enum", schemaTypeText);
            throw new IllegalArgumentException(message, e);
        }
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

    private String getSchemaPath(final long schemaId) {
        return "/schemas/ids/" + URLEncoder.encode(String.valueOf(schemaId), StandardCharsets.UTF_8);
    }

    private JsonNode postJsonResponse(final String pathSuffix, final JsonNode schema, final String schemaDescription) throws SchemaNotFoundException {
        String errorMessage = null;
        for (final String baseUrl: baseUrls) {
            final String path = getPath(pathSuffix);
            final String trimmedBase = getTrimmedBase(baseUrl);
            final String url = trimmedBase + path;
            final URI uri = URI.create(url);

            logger.debug("POST JSON response URL {}", url);

            HttpRequestBodySpec requestBodySpec = webClientService.post()
                    .uri(uri)
                    .header(HttpHeaderName.ACCEPT.getHeaderName(), APPLICATION_JSON_CONTENT_TYPE)
                    .header(HttpHeaderName.CONTENT_TYPE.getHeaderName(), SCHEMA_REGISTRY_CONTENT_TYPE);

            for (final Map.Entry<String, String> header : httpHeaders.entrySet()) {
                requestBodySpec = requestBodySpec.header(header.getKey(), header.getValue());
            }

            final String requestBody = schema.toString();
            try (HttpResponseEntity responseEntity = requestBodySpec.body(requestBody).retrieve()) {
                final int responseCode = responseEntity.statusCode();

                switch (responseCode) {
                    case HTTP_OK:
                        try (InputStream responseBody = responseEntity.body()) {
                            final JsonNode jsonResponse = objectMapper.readTree(responseBody);

                            if (logger.isDebugEnabled()) {
                                logger.debug("JSON Response: {}", jsonResponse);
                            }

                            return jsonResponse;
                        } catch (final IOException e) {
                            throw new SchemaNotFoundException("Failed to read Response Body from URL [%s]".formatted(url), e);
                        }
                    case HTTP_NOT_FOUND:
                        logger.debug("Could not find Schema {} from Registry {}", schemaDescription, baseUrl);
                        continue;

                    default:
                        errorMessage = readErrorResponseBody(responseEntity);
                }
            } catch (final IOException e) {
                throw new SchemaNotFoundException("Failed to read Response from URL [%s]".formatted(url), e);
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
            final URI uri = URI.create(url);

            logger.debug("GET JSON response URL {}", url);

            HttpRequestBodySpec requestBodySpec = webClientService.get()
                    .uri(uri)
                    .header(HttpHeaderName.ACCEPT.getHeaderName(), APPLICATION_JSON_CONTENT_TYPE);

            for (final Map.Entry<String, String> header : httpHeaders.entrySet()) {
                requestBodySpec = requestBodySpec.header(header.getKey(), header.getValue());
            }
            try (HttpResponseEntity responseEntity = requestBodySpec.retrieve()) {
                final int responseCode = responseEntity.statusCode();

                switch (responseCode) {
                    case HTTP_OK:
                        try (InputStream responseBody = responseEntity.body()) {
                            final JsonNode jsonResponse = objectMapper.readTree(responseBody);

                            if (logger.isDebugEnabled()) {
                                logger.debug("JSON Response {}", jsonResponse);
                            }

                            return jsonResponse;
                        } catch (final IOException e) {
                            throw new SchemaNotFoundException("Failed to read Schema Response Body from URL [%s]".formatted(url), e);
                        }
                    case HTTP_NOT_FOUND:
                        logger.debug("Could not find Schema {} from Registry {}", schemaDescription, baseUrl);
                        continue;

                    default:
                        errorMessage = readErrorResponseBody(responseEntity);
                }
            } catch (final IOException e) {
                throw new SchemaNotFoundException("Failed to read Response from URL [%s]".formatted(url), e);
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

    private String readErrorResponseBody(final HttpResponseEntity responseEntity) {
        try (InputStream responseBody = responseEntity.body()) {
            final byte[] responseBodyBinary = responseBody.readAllBytes();
            return new String(responseBodyBinary, StandardCharsets.UTF_8);
        } catch (final IOException e) {
            logger.info("Failed to read Response Body", e);
            return e.getMessage();
        }
    }
}
