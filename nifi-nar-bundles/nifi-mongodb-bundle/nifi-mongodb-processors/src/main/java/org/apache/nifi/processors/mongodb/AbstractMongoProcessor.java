/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.SSLContextService;
import org.bson.Document;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public abstract class AbstractMongoProcessor extends AbstractProcessor {
    static final String WRITE_CONCERN_ACKNOWLEDGED = "ACKNOWLEDGED";
    static final String WRITE_CONCERN_UNACKNOWLEDGED = "UNACKNOWLEDGED";
    static final String WRITE_CONCERN_FSYNCED = "FSYNCED";
    static final String WRITE_CONCERN_JOURNALED = "JOURNALED";
    static final String WRITE_CONCERN_REPLICA_ACKNOWLEDGED = "REPLICA_ACKNOWLEDGED";
    static final String WRITE_CONCERN_MAJORITY = "MAJORITY";

    protected static final String JSON_TYPE_EXTENDED = "Extended";
    protected static final String JSON_TYPE_STANDARD   = "Standard";
    protected static final AllowableValue JSON_EXTENDED = new AllowableValue(JSON_TYPE_EXTENDED, "Extended JSON",
            "Use MongoDB's \"extended JSON\". This is the JSON generated with toJson() on a MongoDB Document from the Java driver");
    protected static final AllowableValue JSON_STANDARD = new AllowableValue(JSON_TYPE_STANDARD, "Standard JSON",
            "Generate a JSON document that conforms to typical JSON conventions instead of Mongo-specific conventions.");

    static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
        .name("mongo-client-service")
        .displayName("Client Service")
        .description("If configured, this property will use the assigned client service for connection pooling.")
        .required(false)
        .identifiesControllerService(MongoDBClientService.class)
        .build();

    static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
        .name("Mongo URI")
        .displayName("Mongo URI")
        .description("MongoURI, typically of the form: mongodb://host1[:port1][,host2[:port2],...]")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
        .name("Mongo Database Name")
        .displayName("Mongo Database Name")
        .description("The name of the database to use")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
        .name("Mongo Collection Name")
        .description("The name of the collection to use")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    protected static final PropertyDescriptor JSON_TYPE = new PropertyDescriptor.Builder()
            .allowableValues(JSON_EXTENDED, JSON_STANDARD)
            .defaultValue(JSON_TYPE_EXTENDED)
            .displayName("JSON Type")
            .name("json-type")
            .description("By default, MongoDB's Java driver returns \"extended JSON\". Some of the features of this variant of JSON" +
                    " may cause problems for other JSON parsers that expect only standard JSON types and conventions. This configuration setting " +
                    " controls whether to use extended JSON or provide a clean view that conforms to standard JSON.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("ssl-context-service")
        .displayName("SSL Context Service")
        .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                + "connections.")
        .required(false)
        .identifiesControllerService(SSLContextService.class)
        .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
        .name("ssl-client-auth")
        .displayName("Client Auth")
        .description("Client authentication policy when connecting to secure (TLS/SSL) cluster. "
                + "Possible values are REQUIRED, WANT, NONE. This property is only used when an SSL Context "
                + "has been defined and enabled.")
        .required(false)
        .allowableValues(SSLContextService.ClientAuth.values())
        .defaultValue("REQUIRED")
        .build();

    public static final PropertyDescriptor WRITE_CONCERN = new PropertyDescriptor.Builder()
            .name("Write Concern")
            .displayName("Write Concern")
            .description("The write concern to use")
            .required(true)
            .allowableValues(WRITE_CONCERN_ACKNOWLEDGED, WRITE_CONCERN_UNACKNOWLEDGED, WRITE_CONCERN_FSYNCED, WRITE_CONCERN_JOURNALED,
                    WRITE_CONCERN_REPLICA_ACKNOWLEDGED, WRITE_CONCERN_MAJORITY)
            .defaultValue(WRITE_CONCERN_ACKNOWLEDGED)
            .build();

    static final PropertyDescriptor RESULTS_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("results-per-flowfile")
            .displayName("Results Per FlowFile")
            .description("How many results to put into a flowfile at once. The whole body will be treated as a JSON array of results.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .displayName("Batch Size")
            .description("The number of elements returned from the server in one batch.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    static final PropertyDescriptor QUERY_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("mongo-query-attribute")
            .displayName("Query Output Attribute")
            .description("If set, the query will be written to a specified attribute on the output flowfiles.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .required(false)
            .build();
    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("mongo-charset")
            .displayName("Character Set")
            .description("Specifies the character set of the document data.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor DATE_FORMAT = new PropertyDescriptor.Builder()
        .name("mongo-date-format")
        .displayName("Date Format")
        .description("The date format string to use for formatting Date fields that are returned from Mongo. It is only " +
                "applied when the JSON output format is set to Standard JSON. Full documentation for format characters can be " +
                "found here: https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html")
        .defaultValue("yyyy-MM-dd'T'HH:mm:ss'Z'")
        .addValidator((subject, input, context) -> {
            ValidationResult.Builder result = new ValidationResult.Builder()
                .subject(subject)
                .input(input);
            try {
                new SimpleDateFormat(input).format(new Date());
                result.valid(true);
            } catch (Exception ex) {
                result.valid(false)
                    .explanation(ex.getMessage());
            }

            return result.build();
        })
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final List<PropertyDescriptor> descriptors;

    static {
        List<PropertyDescriptor> _temp = new ArrayList<>();
        _temp.add(CLIENT_SERVICE);
        _temp.add(URI);
        _temp.add(DATABASE_NAME);
        _temp.add(COLLECTION_NAME);
        _temp.add(SSL_CONTEXT_SERVICE);
        _temp.add(CLIENT_AUTH);
        descriptors = Collections.unmodifiableList(_temp);
    }

    protected ObjectMapper objectMapper;
    protected MongoClient mongoClient;
    protected MongoDBClientService clientService;

    @OnScheduled
    public final void createClient(ProcessContext context) throws IOException {
        if (context.getProperty(CLIENT_SERVICE).isSet()) {
            clientService = context.getProperty(CLIENT_SERVICE).asControllerService(MongoDBClientService.class);
            return;
        }

        if (mongoClient != null) {
            closeClient();
        }

        getLogger().info("Creating MongoClient");

        // Set up the client for secure (SSL/TLS communications) if configured to do so
        final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final String rawClientAuth = context.getProperty(CLIENT_AUTH).getValue();
        final SSLContext sslContext;

        if (sslService != null) {
            final SSLContextService.ClientAuth clientAuth;
            if (StringUtils.isBlank(rawClientAuth)) {
                clientAuth = SSLContextService.ClientAuth.REQUIRED;
            } else {
                try {
                    clientAuth = SSLContextService.ClientAuth.valueOf(rawClientAuth);
                } catch (final IllegalArgumentException iae) {
                    throw new IllegalStateException(String.format("Unrecognized client auth '%s'. Possible values are [%s]",
                            rawClientAuth, StringUtils.join(SslContextFactory.ClientAuth.values(), ", ")));
                }
            }
            sslContext = sslService.createSSLContext(clientAuth);
        } else {
            sslContext = null;
        }

        try {
            if(sslContext == null) {
                mongoClient = new MongoClient(new MongoClientURI(getURI(context)));
            } else {
                mongoClient = new MongoClient(new MongoClientURI(getURI(context), getClientOptions(sslContext)));
            }
        } catch (Exception e) {
            getLogger().error("Failed to schedule {} due to {}", new Object[] { this.getClass().getName(), e }, e);
            throw e;
        }
    }

    protected Builder getClientOptions(final SSLContext sslContext) {
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        builder.sslEnabled(true);
        builder.socketFactory(sslContext.getSocketFactory());
        return builder;
    }

    @OnStopped
    public final void closeClient() {
        if (mongoClient != null) {
            getLogger().info("Closing MongoClient");
            mongoClient.close();
            mongoClient = null;
        }
    }

    protected MongoDatabase getDatabase(final ProcessContext context, final FlowFile flowFile) {
        final String databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(flowFile).getValue();

        return clientService!= null ? clientService.getDatabase(databaseName) : mongoClient.getDatabase(databaseName);
    }

    protected MongoCollection<Document> getCollection(final ProcessContext context, final FlowFile flowFile) {
        final String collectionName = context.getProperty(COLLECTION_NAME).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isEmpty(collectionName)) {
            throw new ProcessException("Collection name was empty after expression language evaluation.");
        }
        return getDatabase(context, flowFile).getCollection(collectionName);
    }

    protected String getURI(final ProcessContext context) {
        if (clientService != null) {
            return clientService.getURI();
        } else {
            return context.getProperty(URI).evaluateAttributeExpressions().getValue();
        }
    }

    protected WriteConcern getWriteConcern(final ProcessContext context) {
        final String writeConcernProperty = context.getProperty(WRITE_CONCERN).getValue();
        WriteConcern writeConcern = null;
        switch (writeConcernProperty) {
            case WRITE_CONCERN_ACKNOWLEDGED:
                writeConcern = WriteConcern.ACKNOWLEDGED;
                break;
            case WRITE_CONCERN_UNACKNOWLEDGED:
                writeConcern = WriteConcern.UNACKNOWLEDGED;
                break;
            case WRITE_CONCERN_FSYNCED:
                writeConcern = WriteConcern.FSYNCED;
                break;
            case WRITE_CONCERN_JOURNALED:
                writeConcern = WriteConcern.JOURNALED;
                break;
            case WRITE_CONCERN_REPLICA_ACKNOWLEDGED:
                writeConcern = WriteConcern.REPLICA_ACKNOWLEDGED;
                break;
            case WRITE_CONCERN_MAJORITY:
                writeConcern = WriteConcern.MAJORITY;
                break;
            default:
                writeConcern = WriteConcern.ACKNOWLEDGED;
        }
        return writeConcern;
    }

    protected void writeBatch(String payload, FlowFile parent, ProcessContext context, ProcessSession session,
            Map<String, String> extraAttributes, Relationship rel) throws UnsupportedEncodingException {
        String charset = context.getProperty(CHARSET).evaluateAttributeExpressions(parent).getValue();

        FlowFile flowFile = parent != null ? session.create(parent) : session.create();
        flowFile = session.importFrom(new ByteArrayInputStream(payload.getBytes(charset)), flowFile);
        flowFile = session.putAllAttributes(flowFile, extraAttributes);
        session.getProvenanceReporter().receive(flowFile, getURI(context));
        session.transfer(flowFile, rel);
    }

    protected synchronized void configureMapper(String setting, String dateFormat) {
        objectMapper = new ObjectMapper();

        if (setting.equals(JSON_TYPE_STANDARD)) {
            objectMapper.registerModule(ObjectIdSerializer.getModule());
            DateFormat df = new SimpleDateFormat(dateFormat);
            objectMapper.setDateFormat(df);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> retVal = new ArrayList<>();

        boolean clientIsSet = context.getProperty(CLIENT_SERVICE).isSet();
        boolean uriIsSet    = context.getProperty(URI).isSet();

        if (clientIsSet && uriIsSet) {
            String msg = "The client service and URI fields cannot be set at the same time.";
            retVal.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        } else if (!clientIsSet && !uriIsSet) {
            String msg = "The client service or the URI field must be set.";
            retVal.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        }

        return retVal;
    }
}
