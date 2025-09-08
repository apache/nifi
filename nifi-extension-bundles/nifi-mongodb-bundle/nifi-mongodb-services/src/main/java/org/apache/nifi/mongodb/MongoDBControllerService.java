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

package org.apache.nifi.mongodb;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.ssl.SSLContextProvider;
import org.bson.Document;

import javax.net.ssl.SSLContext;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Tags({"mongo", "mongodb", "service"})
@CapabilityDescription(
        "Provides a controller service that configures a connection to MongoDB and provides access to that connection to " +
                "other Mongo-related components."
)
public class MongoDBControllerService extends AbstractControllerService implements MongoDBClientService {
    // Regex to find authMechanism value (case-insensitive)
    private static final Pattern AUTH_MECHANISM_PATTERN = Pattern.compile("(?i)(?:[?&])authmechanism=([^&]*)");
    // Regex to find the user from the URI if specified
    private static final Pattern USER_PATTERN = Pattern.compile("(?i)^mongodb(?:\\+srv)?://[^/]*@.*");
    private String uri;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.uri = context.getProperty(URI).evaluateAttributeExpressions().getValue();
        this.mongoClient = createClient(context, this.mongoClient);
    }

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
        URI,
        DB_USER,
        DB_PASSWORD,
        SSL_CONTEXT_SERVICE,
        WRITE_CONCERN
    );

    protected MongoClient mongoClient;
    private String writeConcernProperty;

    @Override
    public void migrateProperties(final PropertyConfiguration propertyConfiguration) {
        propertyConfiguration.removeProperty("ssl-client-auth");

        propertyConfiguration.renameProperty("mongo-uri", URI.getName());
        propertyConfiguration.renameProperty("ssl-context-service", SSL_CONTEXT_SERVICE.getName());
        propertyConfiguration.renameProperty("mongo-write-concern", WRITE_CONCERN.getName());
    }

    // TODO: Remove duplicate code by refactoring shared method to accept PropertyContext
    protected MongoClient createClient(ConfigurationContext context, MongoClient existing) {
        if (existing != null) {
            closeClient(existing);
        }

        writeConcernProperty = context.getProperty(WRITE_CONCERN).getValue();

        // Set up the client for secure (SSL/TLS communications) if configured to do so
        final SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        final SSLContext sslContext;

        if (sslContextProvider == null) {
            sslContext = null;
        } else {
            sslContext = sslContextProvider.createContext();
        }

        try {
            String uri = context.getProperty(URI).evaluateAttributeExpressions().getValue();
            final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
            final String passw = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();

            final MongoClientSettings.Builder builder = MongoClientSettings.builder();

            final Matcher authMechanismMatcher = AUTH_MECHANISM_PATTERN.matcher(uri);
            final String authMechanism;
            if (authMechanismMatcher.find()) {
                authMechanism = authMechanismMatcher.group(1);
            } else {
                authMechanism = null;
            }

            // When properties specify the user, and the URI includes an authMechanism but no user in the URI,
            // the Mongo driver would attempt to build credentials from the URI and fail. In that case, remove the
            // authMechanism from the URI to allow property-based credentials. If the URI already includes user info,
            // keep the mechanism so the URI remains valid; property credentials will override later.
            final boolean hasUserInfoInUri = USER_PATTERN.matcher(uri).matches();
            final String effectiveUri;
            if (authMechanism != null && user != null && !hasUserInfoInUri) {
                String stripped = AUTH_MECHANISM_PATTERN.matcher(uri).replaceFirst(uri.contains("?") ? "?" : "");
                stripped = stripped.replaceAll("[&?]+$", "");
                effectiveUri = stripped;
            } else {
                effectiveUri = uri;
            }

            final ConnectionString cs = new ConnectionString(effectiveUri);
            final String database = cs.getDatabase() == null ? "admin" : cs.getDatabase();

            // Apply connection string first to avoid clearing explicitly set credentials later
            builder.applyConnectionString(cs);

            // If properties specify a user, apply credentials based on properties and mechanism
            if (user != null) {
                if (authMechanism != null) {
                    final AuthenticationMechanism mechanism = AuthenticationMechanism.fromMechanismName(authMechanism.toUpperCase());

                    if (passw != null) {
                        switch (mechanism) {
                            case SCRAM_SHA_1 -> builder.credential(MongoCredential.createScramSha1Credential(user, database, passw.toCharArray()));
                            case SCRAM_SHA_256 -> builder.credential(MongoCredential.createScramSha256Credential(user, database, passw.toCharArray()));
                            case MONGODB_AWS -> builder.credential(MongoCredential.createAwsCredential(user, passw.toCharArray()));
                            case PLAIN -> builder.credential(MongoCredential.createPlainCredential(user, database, passw.toCharArray()));
                            default -> throw new IllegalArgumentException("Unsupported authentication mechanism with username and password: " + mechanism);
                        }
                    } else { // user only
                        switch (mechanism) {
                            case MONGODB_X509 -> builder.credential(MongoCredential.createMongoX509Credential(user));
                            case MONGODB_OIDC -> builder.credential(MongoCredential.createOidcCredential(user));
                            case GSSAPI -> builder.credential(MongoCredential.createGSSAPICredential(user));
                            default -> throw new IllegalArgumentException("Unsupported authentication mechanism with username only: " + mechanism);
                        }
                    }
                } else if (passw != null) {
                    final MongoCredential credential = MongoCredential.createCredential(user, database, passw.toCharArray());
                    builder.credential(credential);
                }
            }

            if (sslContext != null) {
                builder.applyToSslSettings(sslBuilder -> sslBuilder.enabled(true).context(sslContext));
            }

            final MongoClientSettings clientSettings = builder.build();
            return MongoClients.create(clientSettings);

        } catch (Exception e) {
            getLogger().error("Failed to schedule {} due to {}", this.getClass().getName(), e, e);
            throw e;
        }
    }

    @OnStopped
    public final void onStopped() {
        closeClient(mongoClient);
    }

    private void closeClient(MongoClient client) {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public WriteConcern getWriteConcern() {
        WriteConcern writeConcern;
        switch (writeConcernProperty) {
            case WRITE_CONCERN_ACKNOWLEDGED:
                writeConcern = WriteConcern.ACKNOWLEDGED;
                break;
            case WRITE_CONCERN_UNACKNOWLEDGED:
                writeConcern = WriteConcern.UNACKNOWLEDGED;
                break;
            case WRITE_CONCERN_FSYNCED:
                writeConcern = WriteConcern.JOURNALED;
                getLogger().warn("Using deprecated write concern FSYNCED");
                break;
            case WRITE_CONCERN_JOURNALED:
                writeConcern = WriteConcern.JOURNALED;
                break;
            case WRITE_CONCERN_REPLICA_ACKNOWLEDGED:
                writeConcern = WriteConcern.W2;
                getLogger().warn("Using deprecated write concern REPLICA_ACKNOWLEDGED");
                break;
            case WRITE_CONCERN_MAJORITY:
                writeConcern = WriteConcern.MAJORITY;
                break;
            case WRITE_CONCERN_W1:
                writeConcern = WriteConcern.W1;
                break;
            case WRITE_CONCERN_W2:
                writeConcern = WriteConcern.W2;
                break;
            case WRITE_CONCERN_W3:
                writeConcern = WriteConcern.W3;
                break;
            default:
                writeConcern = WriteConcern.ACKNOWLEDGED;
        }
        return writeConcern;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnDisabled
    public void onDisable() {
        this.mongoClient.close();
    }


    @Override
    public MongoDatabase getDatabase(String name) {
        return mongoClient.getDatabase(name);
    }

    @Override
    public String getURI() {
        return uri;
    }

    @Override
    public List<ConfigVerificationResult> verify(ConfigurationContext context,
                                                 ComponentLog verificationLogger,
                                                 Map<String, String> variables) {
        ConfigVerificationResult.Builder connectionSuccessful = new ConfigVerificationResult.Builder()
                .verificationStepName("Connection test");

        MongoClient client = null;
        try {
            client = createClient(context, null);
            MongoDatabase db = client.getDatabase("test");
            db.runCommand(new Document("buildInfo", 1));
            connectionSuccessful.outcome(ConfigVerificationResult.Outcome.SUCCESSFUL);
        } catch (Exception ex) {
            connectionSuccessful
                    .explanation(ex.getMessage())
                    .outcome(ConfigVerificationResult.Outcome.FAILED);
        } finally {
            closeClient(client);
        }

        return List.of(connectionSuccessful.build());
    }
}
