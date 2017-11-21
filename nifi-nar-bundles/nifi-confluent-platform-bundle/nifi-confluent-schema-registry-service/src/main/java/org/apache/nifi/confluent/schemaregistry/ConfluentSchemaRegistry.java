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
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;



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
        .expressionLanguageSupported(true)
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
        .expressionLanguageSupported(false)
        .defaultValue("30 secs")
        .required(true)
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
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final List<String> baseUrls = getBaseURLs(context);
        final int timeoutMillis = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();

        final SSLContext sslContext;
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
        if (sslContextService == null) {
            sslContext = null;
        } else {
            sslContext = sslContextService.createSSLContext(ClientAuth.REQUIRED);
        }

        final SchemaRegistryClient restClient = new RestSchemaRegistryClient(baseUrls, timeoutMillis, sslContext);

        final int cacheSize = context.getProperty(CACHE_SIZE).asInteger();
        final long cacheExpiration = context.getProperty(CACHE_EXPIRATION).asTimePeriod(TimeUnit.NANOSECONDS).longValue();

        client = new CachingSchemaRegistryClient(restClient, cacheSize, cacheExpiration);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final boolean sslContextSet = validationContext.getProperty(SSL_CONTEXT).isSet();
        if (sslContextSet) {
            final List<String> baseUrls = getBaseURLs(validationContext);
            final List<String> insecure = baseUrls.stream()
                .filter(url -> !url.startsWith("https"))
                .collect(Collectors.toList());

            if (!insecure.isEmpty()) {
                return Collections.singleton(new ValidationResult.Builder()
                    .subject(SCHEMA_REGISTRY_URLS.getDisplayName())
                    .input(insecure.get(0))
                    .valid(false)
                    .explanation("When SSL Context is configured, all Schema Registry URL's must use HTTPS, not HTTP")
                    .build());
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

    @Override
    public String retrieveSchemaText(final String schemaName) throws IOException, SchemaNotFoundException {
        final RecordSchema schema = retrieveSchema(schemaName);
        return schema.getSchemaText().get();
    }

    @Override
    public String retrieveSchemaText(final long schemaId, final int version) throws IOException, SchemaNotFoundException {
        final RecordSchema schema = retrieveSchema(schemaId, version);
        return schema.getSchemaText().get();
    }

    @Override
    public RecordSchema retrieveSchema(final String schemaName) throws IOException, SchemaNotFoundException {
        final RecordSchema schema = client.getSchema(schemaName);
        return schema;
    }

    @Override
    public RecordSchema retrieveSchema(final long schemaId, final int version) throws IOException, SchemaNotFoundException {
        final RecordSchema schema = client.getSchema((int) schemaId);
        return schema;
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
