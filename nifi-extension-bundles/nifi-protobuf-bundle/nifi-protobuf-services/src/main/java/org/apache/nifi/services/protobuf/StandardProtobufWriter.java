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
package org.apache.nifi.services.protobuf;

import com.squareup.wire.schema.Schema;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.MessageIndexWriter;
import org.apache.nifi.schemaregistry.services.MessageName;
import org.apache.nifi.schemaregistry.services.MessageNameResolver;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.SchemaReferenceWriter;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.schemaregistry.services.StandardMessageNameFactory;
import org.apache.nifi.schemaregistry.services.StandardSchemaDefinition;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.services.protobuf.schema.ProtoSchemaParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;
import static org.apache.nifi.services.protobuf.StandardProtobufWriter.MessageNameResolverStrategy.MESSAGE_NAME_PROPERTY;

@Tags({"protobuf", "record", "writer", "serializer", "confluent"})
@CapabilityDescription("""
    Serializes NiFi Records into Protocol Buffers binary format. \
    Supports inline schema text and schema registry lookup for determining the Proto schema. \
    When a Schema Reference Writer is configured, a Confluent wire-format header is written; when a \
    Message Index Writer is also configured, the Confluent message index array is written after the header. \
    The target Proto message name can be determined statically using the 'Message Name' property, \
    or dynamically using a Message Name Resolver service.
    A single record is written per FlowFile, since concatenated Protocol Buffers messages cannot be delimited. \
    The 'google.protobuf.Any' well-known type is not expanded on write; a Record derived from an Any-typed message \
    is serialized as an ordinary nested message rather than being re-wrapped as an Any.""")
public class StandardProtobufWriter extends SchemaRegistryService implements RecordSetWriterFactory {

    public static final PropertyDescriptor MESSAGE_NAME_RESOLUTION_STRATEGY = new PropertyDescriptor.Builder()
        .name("Message Name Resolution Strategy")
        .description("Strategy for determining the Protocol Buffers message name for serialization")
        .required(true)
        .allowableValues(MESSAGE_NAME_PROPERTY, MessageNameResolverStrategy.MESSAGE_NAME_RESOLVER)
        .defaultValue(MESSAGE_NAME_PROPERTY)
        .build();

    public static final PropertyDescriptor MESSAGE_NAME = new PropertyDescriptor.Builder()
        .name("Message Name")
        .description("Fully qualified name of the Protocol Buffers message including its package (eg. mypackage.MyMessage).")
        .required(true)
        .expressionLanguageSupported(FLOWFILE_ATTRIBUTES)
        .dependsOn(MESSAGE_NAME_RESOLUTION_STRATEGY, MESSAGE_NAME_PROPERTY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor MESSAGE_NAME_RESOLVER = new PropertyDescriptor.Builder()
        .name("Message Name Resolver")
        .description("Service that dynamically resolves Protocol Buffer message names from FlowFile attributes. "
            + "On the write side the resolver is invoked with an empty content stream, so only resolvers that derive the "
            + "message name from attributes are supported; resolvers that read the message name from message content "
            + "(such as the Confluent wire-format resolver used on the read side) are not applicable here.")
        .required(true)
        .identifiesControllerService(MessageNameResolver.class)
        .dependsOn(MESSAGE_NAME_RESOLUTION_STRATEGY, MessageNameResolverStrategy.MESSAGE_NAME_RESOLVER)
        .build();

    public static final PropertyDescriptor SCHEMA_REFERENCE_WRITER = new PropertyDescriptor.Builder()
        .name("Schema Reference Writer")
        .description("Service used to write schema reference information, such as a Confluent wire-format header, before the serialized Protobuf content. "
            + "When not configured, plain Protobuf content is written without any header.")
        .required(false)
        .identifiesControllerService(SchemaReferenceWriter.class)
        .build();

    public static final PropertyDescriptor MESSAGE_INDEX_WRITER = new PropertyDescriptor.Builder()
        .name("Message Index Writer")
        .description("Service used to write the Confluent message index array identifying the target message within the schema, written after the Schema Reference Writer header. "
            + "Applicable only when producing Confluent wire-format content.")
        .required(false)
        .identifiesControllerService(MessageIndexWriter.class)
        .build();

    private static final PropertyDescriptor PROTOBUF_SCHEMA_TEXT = new PropertyDescriptor.Builder()
        .fromPropertyDescriptor(SCHEMA_TEXT)
        .required(true)
        .clearValidators()
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .defaultValue("${proto.schema}")
        .description("The text of a Proto 3 formatted Schema")
        .build();

    private static final String PROTO_EXTENSION = ".proto";

    private static final InputStream EMPTY_INPUT_STREAM = new ByteArrayInputStream(new byte[0]);

    private volatile ProtobufSchemaCompiler schemaCompiler;
    private volatile MessageNameResolver messageNameResolver;
    private volatile SchemaReferenceWriter schemaReferenceWriter;
    private volatile MessageIndexWriter messageIndexWriter;
    private volatile SchemaRegistry schemaRegistry;
    private volatile String schemaAccessStrategyValue;
    private volatile PropertyValue schemaText;
    private volatile PropertyValue schemaName;
    private volatile PropertyValue schemaBranchName;
    private volatile PropertyValue schemaVersion;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        super.storeSchemaAccessStrategy(context);
        setupMessageNameResolver(context);
        schemaAccessStrategyValue = context.getProperty(SCHEMA_ACCESS_STRATEGY).getValue();
        schemaRegistry = context.getProperty(SCHEMA_REGISTRY).asControllerService(SchemaRegistry.class);
        schemaReferenceWriter = context.getProperty(SCHEMA_REFERENCE_WRITER).asControllerService(SchemaReferenceWriter.class);
        messageIndexWriter = context.getProperty(MESSAGE_INDEX_WRITER).asControllerService(MessageIndexWriter.class);
        schemaName = context.getProperty(SCHEMA_NAME);
        schemaText = context.getProperty(SCHEMA_TEXT);
        schemaBranchName = context.getProperty(SCHEMA_BRANCH_NAME);
        schemaVersion = context.getProperty(SCHEMA_VERSION);
    }

    @Override
    protected void init(final ControllerServiceInitializationContext config) throws InitializationException {
        super.init(config);
        schemaCompiler = new ProtobufSchemaCompiler(getIdentifier(), getLogger());
    }

    @Override
    public RecordSchema getSchema(final Map<String, String> variables, final RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        return createWriteContext(variables).recordSchema();
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out, final Map<String, String> variables) throws SchemaNotFoundException, IOException {
        final ProtobufWriteContext context = createWriteContext(variables);
        return new WriteProtobufResultWithExternalSchema(context.schema(), context.messageName(), context.recordSchema(),
            context.schemaDefinition(), schemaReferenceWriter, messageIndexWriter, variables, out);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        // The Schema Reference Reader is a read-side concern: a writer determines its schema from the configured
        // access strategy and writes references through the Schema Reference Writer instead.
        properties.removeIf(property -> SCHEMA_REFERENCE_READER.getName().equals(property.getName()));
        properties.add(MESSAGE_NAME_RESOLUTION_STRATEGY);
        properties.add(MESSAGE_NAME_RESOLVER);
        properties.add(MESSAGE_NAME);
        properties.add(SCHEMA_REFERENCE_WRITER);
        properties.add(MESSAGE_INDEX_WRITER);
        return properties;
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        // Only the strategies that createSchemaDefinition supports are offered; the inherited list also contains
        // the Schema Reference Reader strategy, which cannot be used to obtain a schema for writing.
        return List.of(SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY);
    }

    @Override
    protected PropertyDescriptor buildSchemaTextProperty() {
        return PROTOBUF_SCHEMA_TEXT;
    }

    private ProtobufWriteContext createWriteContext(final Map<String, String> variables) throws SchemaNotFoundException, IOException {
        final SchemaDefinition schemaDefinition = createSchemaDefinition(variables);
        final Schema schema = schemaCompiler.compileOrGetFromCache(schemaDefinition);
        final MessageName messageName = messageNameResolver.getMessageName(variables, schemaDefinition, EMPTY_INPUT_STREAM);

        final ProtoSchemaParser schemaParser = new ProtoSchemaParser(schema);
        final RecordSchema parsedSchema = schemaParser.createSchema(messageName.getFullyQualifiedName());
        // Preserve the schema identifier from the SchemaDefinition so the configured Schema Reference Writer can
        // write the correct schema id in the Confluent header.
        final RecordSchema recordSchema = new SimpleRecordSchema(parsedSchema.getFields(), schemaDefinition.getIdentifier());

        return new ProtobufWriteContext(schemaDefinition, schema, messageName, recordSchema);
    }

    private SchemaDefinition createSchemaDefinition(final Map<String, String> variables) throws SchemaNotFoundException, IOException {
        if (SCHEMA_TEXT_PROPERTY.getValue().equals(schemaAccessStrategyValue)) {
            return createSchemaDefinitionFromText(variables);
        } else if (SCHEMA_NAME_PROPERTY.getValue().equals(schemaAccessStrategyValue)) {
            return createSchemaDefinitionFromRegistry(variables);
        }

        throw new SchemaNotFoundException("Unsupported schema access strategy: " + schemaAccessStrategyValue);
    }

    private void setupMessageNameResolver(final ConfigurationContext context) {
        final MessageNameResolverStrategy messageNameResolverStrategy = context.getProperty(MESSAGE_NAME_RESOLUTION_STRATEGY).asAllowableValue(MessageNameResolverStrategy.class);
        messageNameResolver = switch (messageNameResolverStrategy) {
            case MESSAGE_NAME_PROPERTY -> new PropertyMessageNameResolver(context);
            case MESSAGE_NAME_RESOLVER -> context.getProperty(MESSAGE_NAME_RESOLVER).asControllerService(MessageNameResolver.class);
        };
    }

    private SchemaDefinition createSchemaDefinitionFromText(final Map<String, String> variables) throws SchemaNotFoundException {
        final String schemaTextString = schemaText.evaluateAttributeExpressions(variables).getValue();
        validateSchemaText(schemaTextString);

        final String hash = sha256Hex(schemaTextString);
        final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder()
            .name(hash + PROTO_EXTENSION)
            .build();

        return new StandardSchemaDefinition(schemaIdentifier, schemaTextString, SchemaDefinition.SchemaType.PROTOBUF);
    }

    private SchemaDefinition createSchemaDefinitionFromRegistry(final Map<String, String> variables) throws SchemaNotFoundException, IOException {
        final String schemaNameValue = schemaName.evaluateAttributeExpressions(variables).getValue();
        validateSchemaName(schemaNameValue);

        final String schemaBranchNameValue = schemaBranchName.evaluateAttributeExpressions(variables).getValue();
        final String schemaVersionValue = schemaVersion.evaluateAttributeExpressions(variables).getValue();

        final SchemaIdentifier schemaIdentifier = buildSchemaIdentifier(schemaNameValue, schemaBranchNameValue, schemaVersionValue);
        return schemaRegistry.retrieveSchemaDefinition(schemaIdentifier);
    }

    private SchemaIdentifier buildSchemaIdentifier(final String schemaNameValue, final String schemaBranchNameValue, final String schemaVersionValue) throws SchemaNotFoundException {
        final SchemaIdentifier.Builder identifierBuilder = SchemaIdentifier.builder().name(schemaNameValue);

        if (schemaBranchNameValue != null && !schemaBranchNameValue.isBlank()) {
            identifierBuilder.branch(schemaBranchNameValue);
        }

        if (schemaVersionValue != null && !schemaVersionValue.isBlank()) {
            try {
                identifierBuilder.version(Integer.valueOf(schemaVersionValue));
            } catch (final NumberFormatException nfe) {
                throw new SchemaNotFoundException("Could not retrieve schema with name '%s' because a non-numeric version was supplied '%s'"
                    .formatted(schemaNameValue, schemaVersionValue), nfe);
            }
        }

        return identifierBuilder.build();
    }

    private String sha256Hex(final String input) {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        final byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(hash);
    }

    private void validateSchemaText(final String schemaTextString) throws SchemaNotFoundException {
        if (schemaTextString == null || schemaTextString.isBlank()) {
            throw new SchemaNotFoundException("Schema text not found");
        }
    }

    private void validateSchemaName(final String schemaNameValue) throws SchemaNotFoundException {
        if (schemaNameValue == null || schemaNameValue.isBlank()) {
            throw new SchemaNotFoundException("Schema name not provided or is blank");
        }
    }

    enum MessageNameResolverStrategy implements DescribedValue {

        MESSAGE_NAME_PROPERTY("Message Name Property", "Use the 'Message Name' property value to determine the message name"),
        MESSAGE_NAME_RESOLVER("Message Name Resolver", "Use a 'Message Name Resolver' service to dynamically determine the message name");

        private final String displayName;
        private final String description;

        MessageNameResolverStrategy(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    static class PropertyMessageNameResolver extends AbstractControllerService implements MessageNameResolver {
        private final PropertyContext context;

        PropertyMessageNameResolver(final PropertyContext context) {
            this.context = context;
        }

        @Override
        public MessageName getMessageName(final Map<String, String> variables, final SchemaDefinition schemaDefinition, final InputStream in) {
            final String messageName = context.getProperty(MESSAGE_NAME).evaluateAttributeExpressions(variables).getValue();
            return StandardMessageNameFactory.fromName(messageName);
        }
    }

    private record ProtobufWriteContext(SchemaDefinition schemaDefinition, Schema schema, MessageName messageName, RecordSchema recordSchema) {
    }
}
