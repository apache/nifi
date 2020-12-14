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

package org.apache.nifi.confluent.schemaregistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.confluent.schemaregistry.client.CachingSchemaRegistryClient;
import org.apache.nifi.confluent.schemaregistry.client.RestSchemaRegistryClient;
import org.apache.nifi.confluent.schemaregistry.client.SchemaRegistryClient;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.security.util.SslContextFactory.ClientAuth;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.ssl.SSLContextService;

import org.apache.commons.lang3.StringUtils;


@Tags({"schema", "registry", "confluent", "avro", "kafka"})
@CapabilityDescription("Provides a Schema Registry that interacts with the Confluent Schema Registry so that those Schemas that are stored in the Confluent Schema "
    + "Registry can be used in NiFi. The Confluent Schema Registry has a notion of a \"subject\" for schemas, which is their terminology for a schema name. When a Schema "
    + "is looked up by name by this registry, it will find a Schema in the Confluent Schema Registry with that subject.")
public class ConfluentSchemaRegistry extends AbstractControllerService implements SchemaRegistry {

    private static final Set<SchemaField> schemaFields = EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_TEXT,
        SchemaField.SCHEMA_TEXT_FORMAT, SchemaField.SCHEMA_IDENTIFIER, SchemaField.SCHEMA_VERSION);


    static final PropertyDescriptor SCHEMA_REGISTRY_URLS = new PropertyDescriptor.Builder()
        .name("url")
        .displayName("Schema Registry URLs")
        .description("A comma-separated list of URLs of the Schema Registry to interact with")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .defaultValue("http://localhost:8081")
        .required(true)
        .addValidator(new MultipleURLValidator())
        .build();

    static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
        .name("ssl-context")
        .displayName("SSL Context Service")
        .description("Specifies the SSL Context Service to use for interacting with the Confluent Schema Registry")
        .identifiesControllerService(SSLContextService.class)
        .required(false)
        .build();

    static final PropertyDescriptor CACHE_SIZE = new PropertyDescriptor.Builder()
        .name("cache-size")
        .displayName("Cache Size")
        .description("Specifies how many Schemas should be cached from the Schema Registry")
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .defaultValue("1000")
        .required(true)
        .build();

    static final PropertyDescriptor CACHE_EXPIRATION = new PropertyDescriptor.Builder()
        .name("cache-expiration")
        .displayName("Cache Expiration")
        .description("Specifies how long a Schema that is cached should remain in the cache. Once this time period elapses, a "
            + "cached version of a schema will no longer be used, and the service will have to communicate with the "
            + "Schema Registry again in order to obtain the schema.")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("1 hour")
        .required(true)
        .build();

    static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
        .name("timeout")
        .displayName("Communications Timeout")
        .description("Specifies how long to wait to receive data from the Schema Registry before considering the communications a failure")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .defaultValue("30 secs")
        .required(true)
        .build();

    static final PropertyDescriptor PROP_USERNAME = new PropertyDescriptor.Builder()
        .name("Authentication Username")
        .displayName("Authentication Username")
        .description("The username to be used by the client to authenticate against the Remote URL.  Cannot include control characters (0-31), ':', or DEL (127).")
        .required(false)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x39\\x3b-\\x7e\\x80-\\xff]+$")))
        .build();

    static final PropertyDescriptor PROP_PASSWORD = new PropertyDescriptor.Builder()
        .name("Authentication Password")
        .displayName("Authentication Password")
        .description("The password to be used by the client to authenticate against the Remote URL.")
        .required(false)
        .sensitive(true)
        .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x7e\\x80-\\xff]+$")))
        .build();

    static final PropertyDescriptor PROP_AUTH_TYPE = new PropertyDescriptor.Builder()
        .name("Authentication Type")
        .displayName("Authentication Type")
        .description("Basic authentication will use the 'Authentication Username' " +
                "and 'Authentication Password' property values. See Confluent Schema Registry documentation for more details.")
        .required(false)
        .allowableValues("BASIC")
        .build();

    private volatile SchemaRegistryClient client;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SCHEMA_REGISTRY_URLS);
        properties.add(SSL_CONTEXT);
        properties.add(TIMEOUT);
        properties.add(CACHE_SIZE);
        properties.add(CACHE_EXPIRATION);
        properties.add(PROP_AUTH_TYPE);
        properties.add(PROP_USERNAME);
        properties.add(PROP_PASSWORD);
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final List<String> baseUrls = getBaseURLs(context);

        final String authUser = StringUtils.trimToEmpty(context.getProperty(PROP_USERNAME).getValue());
        final String authPass = StringUtils.trimToEmpty(context.getProperty(PROP_PASSWORD).getValue());
        final String authType = context.getProperty(PROP_AUTH_TYPE).getValue();

        final int timeoutMillis = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        final SSLContext sslContext;
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
        if (sslContextService == null) {
            sslContext = null;
        } else {
            sslContext = sslContextService.createSSLContext(ClientAuth.REQUIRED);
        }

        final SchemaRegistryClient restClient = new RestSchemaRegistryClient(baseUrls, authType, authUser, authPass, timeoutMillis, sslContext, getLogger());

        final int cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final long cacheExpiration = context.getProperty(CACHE_EXPIRATION).asTimePeriod(TimeUnit.NANOSECONDS).longValue();

        client = new CachingSchemaRegistryClient(restClient, cacheSize, cacheExpiration);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final String authUser = StringUtils.trimToEmpty(validationContext
                .getProperty(PROP_USERNAME).getValue());
        final String authPass = StringUtils.trimToEmpty(validationContext
                .getProperty(PROP_PASSWORD).getValue());
        final boolean authUsernameEmpty = authUser.isEmpty();
        final boolean authPasswordEmpty = authPass.isEmpty();
        final boolean authSet = validationContext.getProperty(PROP_AUTH_TYPE).isSet();

        if (authSet) {
            final boolean authEnabled = validationContext.getProperty(PROP_AUTH_TYPE).toString().toUpperCase()
                    .equals("BASIC");
            if (authEnabled) {
                final List<String> baseUrls = getBaseURLs(validationContext);
                final List<String> insecure = baseUrls.stream()
                        .filter(url -> !url.toUpperCase().startsWith("HTTPS"))
                        .collect(Collectors.toList());
                if (!insecure.isEmpty()) {
                    getLogger().warn("basic HTTP authentication without TLS is insecure, consider upgrading" +
                            " the schema registry endpoint to use TLS encryption");
                }
                if(authUsernameEmpty || authPasswordEmpty) {
                    return Collections.singleton(new ValidationResult.Builder()
                            .subject(PROP_AUTH_TYPE.getDisplayName())
                            .input(validationContext.getProperty(PROP_AUTH_TYPE).getValue())
                            .valid(false)
                            .explanation("When basic authentication is configured both the " + PROP_USERNAME.getDisplayName() + " and " +
                                    PROP_PASSWORD.getDisplayName() + " parameters must be set")
                            .build());
                }
            }
        }
        return Collections.emptyList();
    }

    private List<String> getBaseURLs(final PropertyContext context) {
        final String urls = context.getProperty(SCHEMA_REGISTRY_URLS).evaluateAttributeExpressions().getValue();
        final List<String> baseUrls = Stream.of(urls.split(","))
            .map(url -> url.trim())
            .collect(Collectors.toList());

        return baseUrls;
    }

    private RecordSchema retrieveSchemaByName(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final Optional<String> schemaName = schemaIdentifier.getName();
        if (!schemaName.isPresent()) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Cannot retrieve schema because Schema Name is not present");
        }

        final RecordSchema schema;
        if (schemaIdentifier.getVersion().isPresent()) {
            schema = client.getSchema(schemaName.get(), schemaIdentifier.getVersion().getAsInt());
        } else {
            schema = client.getSchema(schemaName.get());
        }

        return schema;
    }

    private RecordSchema retrieveSchemaById(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final OptionalLong schemaId = schemaIdentifier.getIdentifier();
        if (!schemaId.isPresent()) {
            throw new org.apache.nifi.schema.access.SchemaNotFoundException("Cannot retrieve schema because Schema Id is not present");
        }

        final RecordSchema schema = client.getSchema((int) schemaId.getAsLong());
        return schema;
    }

    @Override
    public RecordSchema retrieveSchema(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        if (schemaIdentifier.getName().isPresent()) {
            return retrieveSchemaByName(schemaIdentifier);
        } else {
            return retrieveSchemaById(schemaIdentifier);
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
