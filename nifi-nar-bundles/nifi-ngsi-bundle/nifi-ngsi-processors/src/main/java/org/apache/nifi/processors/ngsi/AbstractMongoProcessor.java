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
package org.apache.nifi.processors.ngsi;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientOptions.Builder;
import com.mongodb.MongoClientURI;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.authentication.exception.ProviderCreationException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.mongodb.MongoDBClientService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ngsi.ngsi.aggregators.MongoAggregator;
import org.apache.nifi.processors.ngsi.ngsi.backends.MongoBackend;
import org.apache.nifi.processors.ngsi.ngsi.utils.Entity;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIEvent;
import org.apache.nifi.processors.ngsi.ngsi.utils.NGSIUtils;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.ssl.SSLContextService;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public abstract class AbstractMongoProcessor extends AbstractProcessor {

    protected MongoBackend mongoClient;
    protected MongoClient mongoClientSSL;
    protected MongoDBClientService clientService;

    static final PropertyDescriptor CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("mongo-client-service")
            .displayName("Client Service")
            .description("If configured, this property will use the assigned client service for connection pooling.")
            .required(false)
            .identifiesControllerService(MongoDBClientService.class)
            .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("ssl-client-auth")
            .displayName("Client Auth")
            .description("Client authentication policy when connecting to secure (TLS/SSL) cluster. "
                    + "Possible values are REQUIRED, WANT, NONE. This property is only used when an SSL Context "
                    + "has been defined and enabled.")
            .required(false)
            .allowableValues(ClientAuth.values())
            .defaultValue("REQUIRED")
            .build();

    protected static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
        .name("Mongo URI")
        .displayName("Mongo URI")
        .description("MongoURI, typically of the form: mongodb://host1[:port1][,host2[:port2],...]")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    protected static final PropertyDescriptor DATA_MODEL = new PropertyDescriptor.Builder()
            .name("data-model")
            .displayName("Data Model")
            .description("The Data model for creating the tables when an event have been received you can choose between" +
                    ":db-by-service-path or db-by-entity, default value is db-by-service-path")
            .required(false)
            .allowableValues("db-by-service-path", "db-by-entity")
            .defaultValue("db-by-service-path")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor ATTR_PERSISTENCE = new PropertyDescriptor.Builder()
            .name("attr-persistence")
            .displayName("Attribute Persistence")
            .description("The mode of storing the data inside of the table")
            .required(false)
            .allowableValues("row","column")
            .defaultValue("row")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor DB_PREFIX = new PropertyDescriptor.Builder()
            .name("db-prefix")
            .displayName("Database Prefix")
            .description("TA configured prefix is added")
            .required(false)
            .defaultValue("sth_")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor COLLECTION_PREFIX = new PropertyDescriptor.Builder()
            .name("collection-prefix")
            .displayName("Collection Prefix")
            .description("A configured prefix is added")
            .required(false)
            .defaultValue("sth_")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor NGSI_VERSION = new PropertyDescriptor.Builder()
            .name("ngsi-version")
            .displayName("NGSI Version")
            .description("The version of NGSI of your incomming events. You can choose Between v2 for NGSIv2 and ld for NGSI-LD. NGSI-LD is not supported yet ")
            .required(false)
            .allowableValues("v2")
            .defaultValue("v2")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor DEFAULT_SERVICE = new PropertyDescriptor.Builder()
            .name("default-service")
            .displayName("Default Service")
            .description("Default Fiware Service for building the database name")
            .required(false)
            .defaultValue("test")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor DEFAULT_SERVICE_PATH = new PropertyDescriptor.Builder()
            .name("default-service-path")
            .displayName("Default Service path")
            .description("Default Fiware ServicePath for building the table name")
            .required(false)
            .defaultValue("/path")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor DATA_EXPIRATION = new PropertyDescriptor.Builder()
            .name("data-expiration")
            .displayName("Data Expiration")
            .description("Collections will be removed if older than the value specified in seconds. The reference of time is the one stored in the recvTime property. Set to 0 if not wanting this policy. ")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    protected static final PropertyDescriptor COLLECTION_SIZE = new PropertyDescriptor.Builder()
            .name("collection-size")
            .displayName("Collection Size")
            .description("The oldest data (according to insertion time) will be removed if the size of the data collection gets bigger than the value specified in bytes. Notice that the size-based truncation policy takes precedence over the time-based one. Set to 0 if not wanting this policy. Minimum value (different than 0) is 4096 bytes.")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    protected static final PropertyDescriptor MAX_DOCUMENTS = new PropertyDescriptor.Builder()
            .name("max-documents")
            .displayName("Max documents")
            .description("The oldest data (according to insertion time) will be removed if the number of documents in the data collections goes beyond the specified value. Set to 0 if not wanting this policy.")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    protected static final PropertyDescriptor ENABLE_ENCODING= new PropertyDescriptor.Builder()
            .name("enable-encoding")
            .displayName("Enable Encoding")
            .description("true or false, true applies the new encoding, false applies the old encoding.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    protected static final PropertyDescriptor ENABLE_LOWERCASE= new PropertyDescriptor.Builder()
            .name("enable-lowercase")
            .displayName("Enable Lowercase")
            .description("true or false, true for creating the Schema and Tables name with lowercase.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
        .name("ssl-context-service")
        .displayName("SSL Context Service")
        .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                + "connections.")
        .required(false)
        .identifiesControllerService(SSLContextService.class)
        .build();

    protected static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .displayName("Batch Size")
            .description("The number of elements returned from the server in one batch.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    protected static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("mongo-charset")
            .displayName("Character Set")
            .description("Specifies the character set of the document data.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    protected static List<PropertyDescriptor> descriptors = new ArrayList<>();

    static {
        descriptors.add(URI);
        descriptors.add(NGSI_VERSION);
        descriptors.add(DATA_MODEL);
        descriptors.add(ATTR_PERSISTENCE);
        descriptors.add(DEFAULT_SERVICE);
        descriptors.add(DEFAULT_SERVICE_PATH);
        descriptors.add(ENABLE_ENCODING);
        descriptors.add(ENABLE_LOWERCASE);
        descriptors.add(DB_PREFIX);
        descriptors.add(COLLECTION_PREFIX);
        descriptors.add(DATA_EXPIRATION);
        descriptors.add(COLLECTION_SIZE);
        descriptors.add(MAX_DOCUMENTS);
    }

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

        final String dataModel=context.getProperty(DATA_MODEL).getValue();
        // Set up the client for secure (SSL/TLS communications) if configured to do so
        final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        //final String rawClientAuth = "none";//context.getProperty(CLIENT_AUTH).getValue();
        final SSLContext sslContext;

        if (sslService != null) {
            sslContext = sslService.createContext();
        } else {
            sslContext = null;
        }

        try {
            if(sslContext == null) {
                mongoClient = new MongoBackend(new MongoClientURI(getURI(context)),dataModel);;
            } else {
                mongoClientSSL = new MongoClient(new MongoClientURI(getURI(context), getClientOptions(sslContext)));
            }
        } catch (Exception e) {
            getLogger().error("Failed to schedule {} due to {}", new Object[] { this.getClass().getName(), e }, e);
            throw e;
        }
    }

    protected Builder getClientOptions(final SSLContext sslContext) {
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        builder.sslEnabled(true);
        builder.sslContext(sslContext);
        return builder;
    }

    @OnStopped
    public final void closeClient() {
        if (mongoClient.getClient() != null) {
            getLogger().info("Closing MongoClient");
            mongoClient.getClient().close();
            mongoClient = null;
        }
    }


    protected void persistFlowFile(final ProcessContext context, final FlowFile flowFile,ProcessSession session) {
        NGSIUtils n = new NGSIUtils();
        final String attrPersistence = context.getProperty(ATTR_PERSISTENCE).getValue();
        final NGSIEvent event=n.getEventFromFlowFile(flowFile,session,context.getProperty(NGSI_VERSION).getValue());
        final String fiwareService = (event.getFiwareService().compareToIgnoreCase("nd")==0)?context.getProperty(DEFAULT_SERVICE).getValue():event.getFiwareService();
        final String fiwareServicePath = (event.getFiwareServicePath().compareToIgnoreCase("/nd")==0)?context.getProperty(DEFAULT_SERVICE_PATH).getValue():event.getFiwareServicePath();
        final long creationTime = event.getCreationTime();
        try {
            final String dbName = mongoClient.buildDbName(fiwareService, context.getProperty(ENABLE_ENCODING).asBoolean(), context.getProperty(DB_PREFIX).getValue());

            for (Entity entity : event.getEntities()) {
                String collectionName = mongoClient.buildCollectionName(fiwareServicePath, entity.getEntityId(), entity.getEntityType(), entity.getEntityAttrs().get(0).getAttrName()
                        , context.getProperty(ENABLE_ENCODING).asBoolean(), context.getProperty(COLLECTION_PREFIX).getValue());
                MongoAggregator aggregator = new MongoAggregator() {
                    @Override
                    public void aggregate(Entity entity, long creationTime, String dataModel) {

                    }
                };
                if (attrPersistence.compareToIgnoreCase("row") == 0) {
                    aggregator = aggregator.getAggregator(true);
                    aggregator.aggregate(entity, creationTime, context.getProperty(DATA_MODEL).getValue());
                } else if (attrPersistence.compareToIgnoreCase("column") == 0) {
                    aggregator = aggregator.getAggregator(false);
                    aggregator.aggregate(entity, creationTime, context.getProperty(DATA_MODEL).getValue());
                }
                aggregator.persistAggregation(aggregator, dbName, collectionName,
                        context.getProperty(ENABLE_LOWERCASE).asBoolean(),
                        mongoClient, Long.valueOf(context.getProperty(COLLECTION_SIZE).getValue()),
                        Long.valueOf(context.getProperty(MAX_DOCUMENTS).getValue()),
                        Long.valueOf(context.getProperty(DATA_EXPIRATION).getValue()));
            }
        }catch (Exception e){
            getLogger().error(e.toString());
        }
    }

    protected String getURI(final ProcessContext context) {
        return context.getProperty(URI).evaluateAttributeExpressions().getValue();
    }

}
