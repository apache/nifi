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

import com.squareup.wire.schema.MessageType;
import com.squareup.wire.schema.Schema;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaField;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
@SeeAlso(StandardProtobufReader.class)
@CapabilityDescription("""
    Serializes NiFi Records into Protocol Buffers binary format. \
    Supports inline schema text and schema registry lookup for determining the Proto schema. \
    Optional Schema Reference Writer and Message Index Writer Controller Services can add framing before the \
    Protobuf payload. When both are configured, schema reference information is written first, followed by message \
    index information. Selected implementations must be compatible with each other and with the target wire format. \
    Confluent Protobuf wire format requires both compatible services. \
    The target Proto message name can be determined statically using the 'Message Name' property or dynamically \
    using a Message Name Resolver service. \
    A single record is written per FlowFile because concatenated Protocol Buffers messages cannot be delimited. \
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
        .description("The Controller Service used to write schema reference information before any message index information "
            + "and the Protobuf payload. The selected implementation must be compatible with the target wire format and any "
            + "configured Message Index Writer. When not configured, no schema reference information is written.")
        .required(false)
        .identifiesControllerService(SchemaReferenceWriter.class)
        .build();

    public static final PropertyDescriptor MESSAGE_INDEX_WRITER = new PropertyDescriptor.Builder()
        .name("Message Index Writer")
        .description("The Controller Service used to write information identifying the selected message within the Protobuf "
            + "schema. Message index information is written after any schema reference information and before the Protobuf "
            + "payload. The selected implementation must be compatible with the target wire format and any configured "
            + "Schema Reference Writer.")
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
        return createWriteContext(variables, null).recordSchema();
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out, final Map<String, String> variables) throws SchemaNotFoundException, IOException {
        // Resolve the schema against the identifier of the supplied schema, so that a registry version registered after
        // getSchema() was called cannot change the schema used for writing
        final ProtobufWriteContext context = createWriteContext(variables, schema);

        if (schemaReferenceWriter != null) {
            schemaReferenceWriter.validateSchema(context.recordSchema());
        }
        return new WriteProtobufResultWithExternalSchema(context.schema(), context.messageName(), context.recordSchema(),
            context.schemaDefinition(), schemaReferenceWriter, messageIndexWriter, variables, out);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
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
        return List.of(SCHEMA_NAME_PROPERTY, SCHEMA_TEXT_PROPERTY);
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        validateLiteralSchemaText(validationContext, results);
        validateSchemaReferenceWriterCompatibility(validationContext, results);

        return results;
    }

    @Override
    protected PropertyDescriptor buildSchemaTextProperty() {
        return PROTOBUF_SCHEMA_TEXT;
    }

    private ProtobufWriteContext createWriteContext(final Map<String, String> variables, final RecordSchema suppliedSchema) throws SchemaNotFoundException, IOException {
        final SchemaDefinition schemaDefinition = createSchemaDefinition(variables, suppliedSchema);
        final Schema schema = schemaCompiler.compileOrGetFromCache(schemaDefinition);
        final MessageName messageName = messageNameResolver.getMessageName(variables, schemaDefinition, InputStream.nullInputStream());

        final ProtoSchemaParser schemaParser = new ProtoSchemaParser(schema);
        final RecordSchema parsedSchema = schemaParser.createSchema(messageName.getFullyQualifiedName());
        // Preserve the schema identifier required by the configured Schema Reference Writer.
        final RecordSchema recordSchema = new SimpleRecordSchema(parsedSchema.getFields(), schemaDefinition.getIdentifier());

        return new ProtobufWriteContext(schemaDefinition, schema, messageName, recordSchema);
    }

    private SchemaDefinition createSchemaDefinition(final Map<String, String> variables, final RecordSchema suppliedSchema) throws SchemaNotFoundException, IOException {
        if (SCHEMA_TEXT_PROPERTY.getValue().equals(schemaAccessStrategyValue)) {
            return createSchemaDefinitionFromText(variables);
        } else if (SCHEMA_NAME_PROPERTY.getValue().equals(schemaAccessStrategyValue)) {
            final SchemaIdentifier suppliedIdentifier = getVersionedIdentifier(suppliedSchema);
            if (suppliedIdentifier != null) {
                return schemaRegistry.retrieveSchemaDefinition(suppliedIdentifier);
            }
            return createSchemaDefinitionFromRegistry(variables);
        }

        throw new SchemaNotFoundException("Unsupported schema access strategy: " + schemaAccessStrategyValue);
    }

    private void validateLiteralSchemaText(final ValidationContext validationContext, final List<ValidationResult> results) {
        final String schemaAccessStrategy = validationContext.getProperty(SCHEMA_ACCESS_STRATEGY).getValue();
        if (!SCHEMA_TEXT_PROPERTY.getValue().equals(schemaAccessStrategy)) {
            return;
        }

        final String schemaTextValue = validationContext.getProperty(SCHEMA_TEXT).getValue();
        if (schemaTextValue == null || schemaTextValue.isBlank() || validationContext.isExpressionLanguagePresent(schemaTextValue)) {
            return;
        }

        final Schema compiledSchema;
        try {
            final SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder()
                .name(sha256Hex(schemaTextValue) + PROTO_EXTENSION)
                .build();
            final SchemaDefinition schemaDefinition = new StandardSchemaDefinition(schemaIdentifier, schemaTextValue, SchemaDefinition.SchemaType.PROTOBUF);
            compiledSchema = schemaCompiler.compileOrGetFromCache(schemaDefinition);
        } catch (final RuntimeException e) {
            // The compiler reports Wire schema errors as SchemaCompilationException and other failures as other
            // runtime exceptions, all of which indicate that the configured schema text cannot be used
            results.add(new ValidationResult.Builder()
                .subject(SCHEMA_TEXT.getDisplayName())
                .valid(false)
                .explanation("Invalid Protocol Buffers schema: " + e.getMessage())
                .build());
            return;
        }

        validateLiteralMessageName(validationContext, compiledSchema, results);
    }

    private void validateLiteralMessageName(final ValidationContext validationContext, final Schema compiledSchema, final List<ValidationResult> results) {
        final String resolutionStrategy = validationContext.getProperty(MESSAGE_NAME_RESOLUTION_STRATEGY).getValue();
        if (!MESSAGE_NAME_PROPERTY.getValue().equals(resolutionStrategy)) {
            return;
        }

        final String messageNameValue = validationContext.getProperty(MESSAGE_NAME).getValue();
        if (messageNameValue == null || messageNameValue.isBlank() || validationContext.isExpressionLanguagePresent(messageNameValue)) {
            return;
        }

        if (!(compiledSchema.getType(messageNameValue) instanceof MessageType)) {
            results.add(new ValidationResult.Builder()
                .subject(MESSAGE_NAME.getDisplayName())
                .input(messageNameValue)
                .valid(false)
                .explanation("Message name '%s' does not identify a message in the configured Protocol Buffers schema".formatted(messageNameValue))
                .build());
        }
    }

    private void validateSchemaReferenceWriterCompatibility(final ValidationContext validationContext, final List<ValidationResult> results) {
        if (!validationContext.getProperty(SCHEMA_REFERENCE_WRITER).isSet()) {
            return;
        }

        final SchemaReferenceWriter referenceWriter = validationContext.getProperty(SCHEMA_REFERENCE_WRITER).asControllerService(SchemaReferenceWriter.class);
        if (referenceWriter == null) {
            return;
        }

        final Set<SchemaField> missingFields = EnumSet.noneOf(SchemaField.class);
        missingFields.addAll(referenceWriter.getRequiredSchemaFields());
        missingFields.removeAll(getSuppliedSchemaFields(validationContext));

        if (!missingFields.isEmpty()) {
            results.add(new ValidationResult.Builder()
                .subject(SCHEMA_REFERENCE_WRITER.getDisplayName())
                .valid(false)
                .explanation("The configured Schema Reference Writer requires schema fields that are not provided "
                    + "by the configured Schema Access Strategy and Schema Registry: " + missingFields)
                .build());
        }
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

    /**
     * Returns an identifier addressing the exact registry version of the supplied schema, or null when the supplied
     * schema does not carry both a name and a version.
     */
    private SchemaIdentifier getVersionedIdentifier(final RecordSchema suppliedSchema) {
        if (suppliedSchema == null) {
            return null;
        }

        final SchemaIdentifier identifier = suppliedSchema.getIdentifier();
        if (identifier.getName().isEmpty() || identifier.getVersion().isEmpty()) {
            return null;
        }

        final SchemaIdentifier.Builder identifierBuilder = SchemaIdentifier.builder()
            .name(identifier.getName().get())
            .version(identifier.getVersion().getAsInt());
        identifier.getBranch().ifPresent(identifierBuilder::branch);
        return identifierBuilder.build();
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
