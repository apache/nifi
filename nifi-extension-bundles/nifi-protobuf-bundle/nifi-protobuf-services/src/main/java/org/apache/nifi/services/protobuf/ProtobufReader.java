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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.squareup.wire.schema.CoreLoaderKt;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.Schema;
import com.squareup.wire.schema.SchemaLoader;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.MessageName;
import org.apache.nifi.schemaregistry.services.MessageNameResolver;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.SchemaReferenceReader;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.services.protobuf.schema.ProtoSchemaParser;
import org.apache.nifi.services.protobuf.schema.ProtoSchemaStrategy;
import org.apache.nifi.services.protobuf.validation.ProtoValidationResource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.commons.io.FileUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.squareup.wire.schema.CoreLoaderKt.WIRE_RUNTIME_JAR;

@Tags({"protobuf", "record", "reader", "parser"})
@CapabilityDescription("Parses a Protocol Buffers message from binary format.")
public class ProtobufReader extends SchemaRegistryService implements RecordReaderFactory {

    private static final AllowableValue GENERATE_FROM_PROTO_FILE = new AllowableValue("generate-from-proto-file",
        "Generate from Proto file", "The record schema is generated from the provided proto file");
    private static final AllowableValue MESSAGE_TYPE_PROPERTY = new AllowableValue("message-type-property",
        "Message Type Property", "Use the configured Message Type property value to determine the message name");
    private static final AllowableValue MESSAGE_NAME_RESOLVER_SERVICE = new AllowableValue("message-name-resolver-service",
        "Message Name Resolver Service", "Use a Message Name Resolver controller service to dynamically determine the message name");

    public static final PropertyDescriptor PROTOBUF_DIRECTORY = new PropertyDescriptor.Builder()
        .name("Proto Directory")
        .displayName("Proto Directory")
        .description("Directory containing Protocol Buffers message definition (.proto) file(s).")
        .required(false)
        .addValidator(StandardValidators.createDirectoryExistsValidator(true, false))
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor MESSAGE_TYPE = new PropertyDescriptor.Builder()
        .name("Message Type")
        .displayName("Message Type")
        .description("Fully qualified name of the Protocol Buffers message type including its package (eg. mypackage.MyMessage). " +
            "The .proto files configured in '" + PROTOBUF_DIRECTORY.getDisplayName() + "' must contain the definition of this message type.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();

    public static final PropertyDescriptor MESSAGE_NAME_RESOLVER_STRATEGY = new PropertyDescriptor.Builder()
        .name("Message Name Resolver Strategy")
        .displayName("Message Name Resolver Strategy")
        .description("Strategy for determining the Protocol Buffers message name for processing")
        .required(true)
        .allowableValues(MESSAGE_TYPE_PROPERTY, MESSAGE_NAME_RESOLVER_SERVICE)
        .defaultValue(MESSAGE_TYPE_PROPERTY)
        .dependsOn(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, GENERATE_FROM_PROTO_FILE, SchemaAccessUtils.SCHEMA_REFERENCE_READER_PROPERTY)
        .build();


    public static final PropertyDescriptor MESSAGE_NAME_RESOLVER_CONTROLLER_SERVICE = new PropertyDescriptor.Builder()
        .name("Message Name Resolver Service")
        .displayName("Message Name Resolver Service")
        .description("Controller service that dynamically resolves Protocol Buffer message names from FlowFile content or attributes")
        .required(true)
        .identifiesControllerService(MessageNameResolver.class)
        .dependsOn(MESSAGE_NAME_RESOLVER_STRATEGY, MESSAGE_NAME_RESOLVER_SERVICE)
        .build();

    private static final String ANY_PROTO = "google/protobuf/any.proto";
    private static final String DURATION_PROTO = "google/protobuf/duration.proto";
    private static final String EMPTY_PROTO = "google/protobuf/empty.proto";
    private static final String STRUCT_PROTO = "google/protobuf/struct.proto";
    private static final String TIMESTAMP_PROTO = "google/protobuf/timestamp.proto";
    private static final String WRAPPERS_PROTO = "google/protobuf/wrappers.proto";

    // Holder of cached proto information so validation does not reload the same proto file over and over
    final AtomicReference<ProtoValidationResource> validationResourceHolder = new AtomicReference<>();
    volatile String messageType;
    volatile Schema protoSchema;
    private volatile String schemaAccessStrategyValue;
    private volatile SchemaReferenceReader schemaReferenceReader;
    private volatile MessageNameResolver messageNameResolver;
    private volatile SchemaRegistry schemaRegistry;
    volatile Cache<SchemaIdentifier, SchemaDefinition> schemaDefinitionCache;
    volatile Cache<SchemaIdentifier, Schema> compiledSchemaCache;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(MESSAGE_NAME_RESOLVER_STRATEGY);
        properties.add(MESSAGE_NAME_RESOLVER_CONTROLLER_SERVICE);
        properties.add(PROTOBUF_DIRECTORY);
        properties.add(MESSAGE_TYPE);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));
        final String schemaAccessStrategyValue = validationContext.getProperty(getSchemaAccessStrategyDescriptor()).getValue();


        if (GENERATE_FROM_PROTO_FILE.getValue().equals(schemaAccessStrategyValue)) {
            // Validate directory for proto file strategy
            final String directory = validationContext.getProperty(PROTOBUF_DIRECTORY).evaluateAttributeExpressions().getValue();
            if (directory == null || directory.trim().isEmpty()) {
                problems.add(new ValidationResult.Builder()
                    .subject(PROTOBUF_DIRECTORY.getDisplayName())
                    .valid(false)
                    .explanation("Proto Directory must be specified when using Generate from Proto file strategy")
                    .build());
            }

            problems.addAll(validateMessageNameResolverConfiguration(validationContext, true, null));

        } else if (SchemaAccessUtils.SCHEMA_REFERENCE_READER_PROPERTY.getValue().equals(schemaAccessStrategyValue)) {
            // Validate schema definition provider service
            final SchemaRegistry schemaRegistry = validationContext.getProperty(SchemaAccessUtils.SCHEMA_REGISTRY)
                .asControllerService(SchemaRegistry.class);
            if (schemaRegistry == null) {
                problems.add(new ValidationResult.Builder()
                    .subject(SchemaAccessUtils.SCHEMA_REGISTRY.getDisplayName())
                    .valid(false)
                    .explanation("Schema Reference Reader must be set")
                    .build());
            }

            problems.addAll(validateMessageNameResolverConfiguration(validationContext, false, schemaRegistry));
        }

        return problems;
    }

    private Collection<ValidationResult> validateMessageNameResolverConfiguration(ValidationContext validationContext,
                                                                                  boolean useLocalProtoValidation,
                                                                                  SchemaRegistry schemaRegistry) {
        final List<ValidationResult> problems = new ArrayList<>();
        final String resolverValue = validationContext.getProperty(MESSAGE_NAME_RESOLVER_STRATEGY).getValue();

        if (MESSAGE_TYPE_PROPERTY.getValue().equals(resolverValue)) {
            final String msgType = validationContext.getProperty(MESSAGE_TYPE).evaluateAttributeExpressions().getValue();
            if (msgType == null || msgType.trim().isEmpty()) {
                problems.add(new ValidationResult.Builder()
                    .subject(MESSAGE_TYPE.getDisplayName())
                    .valid(false)
                    .explanation("Message Type must be specified when using Message Type Property resolver")
                    .build());
            } else {
                // Validate that the message type exists in the schema
                if (useLocalProtoValidation) {
                    problems.addAll(validateRequirementsForLocalLoading(validationContext));
                } else if (schemaRegistry != null) {
                    // Validate that the message type exists in the schema from the provider
                    problems.addAll(schemaRegistry.validate(validationContext));
                }
            }
        } else if (MESSAGE_NAME_RESOLVER_SERVICE.getValue().equals(resolverValue)) {
            final MessageNameResolver resolver = validationContext.getProperty(MESSAGE_NAME_RESOLVER_CONTROLLER_SERVICE)
                .asControllerService(MessageNameResolver.class);
            if (resolver == null) {
                problems.add(new ValidationResult.Builder()
                    .subject(MESSAGE_NAME_RESOLVER_CONTROLLER_SERVICE.getDisplayName())
                    .valid(false)
                    .explanation("Message Name Resolver Service must be configured when using Message Name Resolver Service strategy")
                    .build());
            }
        }

        return problems;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        super.storeSchemaAccessStrategy(context);
        schemaAccessStrategyValue = context.getProperty(getSchemaAccessStrategyDescriptor()).getValue();

        if (GENERATE_FROM_PROTO_FILE.getValue().equals(schemaAccessStrategyValue)) {
            // Handle message type resolution based on resolver strategy
            final String resolverValue = context.getProperty(MESSAGE_NAME_RESOLVER_STRATEGY).getValue();
            if (MESSAGE_TYPE_PROPERTY.getValue().equals(resolverValue)) {
                messageType = context.getProperty(MESSAGE_TYPE).evaluateAttributeExpressions().getValue();
            }
            final String protoDirectory = context.getProperty(PROTOBUF_DIRECTORY).evaluateAttributeExpressions().getValue();
            protoSchema = loadProtoSchema(protoDirectory);

        } else if (SchemaAccessUtils.SCHEMA_REFERENCE_READER_PROPERTY.getValue().equals(schemaAccessStrategyValue)) {
            schemaReferenceReader = context.getProperty(SchemaAccessUtils.SCHEMA_REFERENCE_READER)
                .asControllerService(SchemaReferenceReader.class);
            messageNameResolver = context.getProperty(MESSAGE_NAME_RESOLVER_CONTROLLER_SERVICE)
                .asControllerService(MessageNameResolver.class);
            schemaRegistry = context.getProperty(SchemaAccessUtils.SCHEMA_REGISTRY)
                .asControllerService(SchemaRegistry.class);
            schemaDefinitionCache = Caffeine.newBuilder()
                .expireAfterWrite(1, TimeUnit.HOURS)
                .maximumSize(200)
                .build();
            compiledSchemaCache = Caffeine.newBuilder()
                .expireAfterWrite(1, TimeUnit.HOURS)
                .maximumSize(200)
                .build();
        }
    }


    @OnDisabled
    public void onDisabled(final ConfigurationContext context) {
        validationResourceHolder.set(null);
        if (schemaDefinitionCache != null) {
            schemaDefinitionCache.invalidateAll();
        }
        if (compiledSchemaCache != null) {
            compiledSchemaCache.invalidateAll();
        }

    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String allowableValue, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        if (allowableValue.equalsIgnoreCase(GENERATE_FROM_PROTO_FILE.getValue())) {
            return new ProtoSchemaStrategy(messageType, protoSchema);
        }
        return SchemaAccessUtils.getSchemaAccessStrategy(allowableValue, schemaRegistry, context);
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(GENERATE_FROM_PROTO_FILE);
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return GENERATE_FROM_PROTO_FILE;
    }

    @Override
    public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger) throws IOException, SchemaNotFoundException {
        if (GENERATE_FROM_PROTO_FILE.getValue().equals(schemaAccessStrategyValue)) {
            return new ProtobufRecordReader(protoSchema, messageType, in, getSchema(variables, in, null));
        } else if (SchemaAccessUtils.SCHEMA_REFERENCE_READER_PROPERTY.getValue().equals(schemaAccessStrategyValue)) {
            SchemaIdentifier schemaIdentifier = schemaReferenceReader.getSchemaIdentifier(variables, in);
            SchemaDefinition schemaDefinition = schemaDefinitionCache.get(schemaIdentifier, this::retrieveSchemaDefinition);

            MessageName messageName = messageNameResolver.getMessageName(schemaDefinition, in);
            if (logger.isDebugEnabled()) {
                logger.debug("Using message name: {} for schema identifier: {}", messageName.getFQNName(), schemaIdentifier);
            }
            // Compile SchemaDefinition structure into Schema using wire library
            Schema compiledSchema = compileOrGetFromCache(schemaDefinition);
            ProtoSchemaParser schemaParser = new ProtoSchemaParser(compiledSchema);
            RecordSchema recordSchema = schemaParser.createSchema(messageName.getFQNName());
            return new ProtobufRecordReader(compiledSchema, messageName.getFQNName(), in, recordSchema);
        } else {
            throw new IOException("Unsupported schema access strategy: " + schemaAccessStrategyValue);
        }
    }

    private Schema compileOrGetFromCache(final SchemaDefinition schemaDefinition) {
        return compiledSchemaCache.get(schemaDefinition.identifier(),
            identifier -> {
                try {
                    return compileSchemaDefinition(schemaDefinition);
                } catch (IOException e) {
                    throw new RuntimeException("Could not compile schema for identifier: " + identifier, e);
                }
            });
    }

    private Schema loadProtoSchema(final String protoDirectory) {
        final SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());
        schemaLoader.initRoots(Arrays.asList(Location.get(protoDirectory),
            Location.get(WIRE_RUNTIME_JAR, ANY_PROTO),
            Location.get(WIRE_RUNTIME_JAR, DURATION_PROTO),
            Location.get(WIRE_RUNTIME_JAR, EMPTY_PROTO),
            Location.get(WIRE_RUNTIME_JAR, STRUCT_PROTO),
            Location.get(WIRE_RUNTIME_JAR, TIMESTAMP_PROTO),
            Location.get(WIRE_RUNTIME_JAR, WRAPPERS_PROTO)), Collections.emptyList());
        return schemaLoader.loadSchema();
    }


    private Schema loadLocalSchemaForValidation(final ValidationContext validationContext) {
        final String protoDirectory = validationContext.getProperty(ProtobufReader.PROTOBUF_DIRECTORY).evaluateAttributeExpressions().getValue();

        ProtoValidationResource validationResource = validationResourceHolder.get();
        if (validationResource == null || !protoDirectory.equals(validationResource.getProtoDirectory())) {
            validationResource = new ProtoValidationResource(protoDirectory, loadProtoSchema(protoDirectory));
            validationResourceHolder.set(validationResource);
        }

        return validationResource.getProtoSchema();
    }


    private Collection<ValidationResult> validateRequirementsForLocalLoading(final ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>();
        final String protoDirectory = validationContext.getProperty(ProtobufReader.PROTOBUF_DIRECTORY).evaluateAttributeExpressions().getValue();
        final String messageType = validationContext.getProperty(ProtobufReader.MESSAGE_TYPE).evaluateAttributeExpressions().getValue();

        if (protoDirectory != null && messageType != null) {
            final Schema protoSchema = loadLocalSchemaForValidation(validationContext);
            if (protoSchema.getType(messageType) == null) {
                problems.add(new ValidationResult.Builder()
                    .subject(ProtobufReader.MESSAGE_TYPE.getDisplayName())
                    .valid(false)
                    .explanation(String.format("'%s' message type cannot be found in the provided proto files.", messageType))
                    .build());
            }
        }

        return problems;
    }

    private SchemaDefinition retrieveSchemaDefinition(SchemaIdentifier schemaIdentifier) {
        try {
            return schemaRegistry.retrieveSchemaRaw(schemaIdentifier);
        } catch (final Exception e) {
            throw new RuntimeException("Could not retrieve schema for identifier: " + schemaIdentifier, e);
        }
    }

    /**
     * Compiles a SchemaDefinition structure into a Schema using the wire library.
     * Creates a temporary directory structure that mirrors the package structure and
     * places all schemas in their appropriate directories.
     *
     * @param schemaDefinition the main schema definition to compile
     * @return the compiled Schema
     * @throws IOException if unable to create temporary files or compile schema
     */
    private Schema compileSchemaDefinition(SchemaDefinition schemaDefinition) throws IOException {
        getLogger().debug("Starting schema compilation for identifier: {}", schemaDefinition.identifier());

        // Validate that all schema identifiers end with .proto extension
        validateSchemaDefinitionIdentifiers(schemaDefinition);

        // Create temporary directory for schema compilation
        Path tempDir = Files.createTempDirectory("nifi-protobuf-schema");
        getLogger().debug("Created temporary directory for schema compilation: {}", tempDir);

        try {
            // Process main schema definition
            writeSchemaToTempDirectory(tempDir, schemaDefinition);

            // Process all referenced schemas recursively
            processSchemaReferences(tempDir, schemaDefinition.getReferences());

            // Create SchemaLoader with temporary directory and standard protobuf libraries
            SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());

            // Initialize roots with temp directory and standard protobuf libraries
            List<Location> roots = new ArrayList<>();
            roots.add(Location.get(tempDir.toString()));

            // Add standard protobuf libraries
            roots.addAll(Arrays.asList(
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/any.proto"),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/duration.proto"),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/empty.proto"),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/struct.proto"),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/timestamp.proto"),
                Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/wrappers.proto")
            ));

            schemaLoader.initRoots(roots, Collections.emptyList());

            // Load and return the compiled schema
            Schema compiledSchema = schemaLoader.loadSchema();
            getLogger().debug("Successfully compiled schema for identifier: {}", schemaDefinition.identifier());
            return compiledSchema;

        } catch (Exception e) {
            getLogger().error("Failed to compile schema for identifier: {}, cleaning up temporary directory: {}",
                schemaDefinition.identifier(), tempDir);
            // Clean up temporary directory on failure
            deleteDirectory(tempDir);
            throw new RuntimeException("Failed to compile Protobuf schema for identifier: " + schemaDefinition.identifier(), e);
        } finally {
            // Clean up temporary directory
            deleteDirectory(tempDir);
        }
    }

    /**
     * Validates that all SchemaDefinition identifiers end with .proto extension.
     * Performs recursive validation on all referenced schemas.
     *
     * @param schemaDefinition the schema definition to validate
     * @throws IllegalArgumentException if any identifier does not end with .proto extension
     */
    void validateSchemaDefinitionIdentifiers(SchemaDefinition schemaDefinition) {
        validateSchemaIdentifier(schemaDefinition.identifier());
        
        // Recursively validate all referenced schemas
        for (SchemaDefinition referencedSchema : schemaDefinition.getReferences().values()) {
            validateSchemaDefinitionIdentifiers(referencedSchema);
        }
    }

    /**
     * Validates that a single SchemaIdentifier has a name ending with .proto extension.
     *
     * @param schemaIdentifier the schema identifier to validate
     * @throws IllegalArgumentException if the identifier name does not end with .proto extension
     */
    private void validateSchemaIdentifier(SchemaIdentifier schemaIdentifier) {
        Optional<String> nameOptional = schemaIdentifier.getName();
        if (nameOptional.isPresent()) {
            String name = nameOptional.get();
            if (!name.endsWith(".proto")) {
                throw new IllegalArgumentException(
                    String.format("Schema identifier name '%s' must end with .proto extension", name));
            }
        } else {
            throw new IllegalArgumentException("Schema identifier must have a name that ends with .proto extension");
        }
    }


    /**
     * Writes a schema definition to the temporary directory structure.
     * If package name is present, creates the appropriate directory structure.
     *
     * @param tempDir          the temporary directory root
     * @param schemaDefinition the schema definition to write
     * @throws IOException if unable to create directories or write files
     */
    private void writeSchemaToTempDirectory(Path tempDir, SchemaDefinition schemaDefinition) throws IOException {
        getLogger().debug("Writing schema definition to temporary directory. Identifier: {}", schemaDefinition.identifier());
        // Generate filename from schema identifier
        Path schemaFile = tempDir.resolve(schemaDefinition.identifier().getName().orElseThrow(
            () -> new IllegalArgumentException("Schema identifier must have a name that ends with .proto extension")));

        // Write schema text to file
        Files.write(schemaFile, schemaDefinition.text().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        getLogger().debug("Successfully wrote schema to file: {} (size: {} bytes)",
            schemaFile, schemaDefinition.text().length());
    }

    /**
     * Processes schema references recursively, placing each referenced schema
     * in its appropriate directory based on its identifier.
     *
     * @param tempDir    the temporary directory root
     * @param references the map of schema references
     * @throws IOException if unable to process references
     */
    private void processSchemaReferences(Path tempDir, Map<String, SchemaDefinition> references) throws IOException {
        getLogger().debug("Processing schema references. Count: {}, Temp directory: {}",
            references.size(), tempDir);

        for (Map.Entry<String, SchemaDefinition> entry : references.entrySet()) {
            String referenceKey = entry.getKey();
            SchemaDefinition referencedSchema = entry.getValue();

            getLogger().debug("Processing schema reference - Key: {}, Identifier: {}",
                referenceKey, referencedSchema.identifier());

            // Write referenced schema to appropriate directory
            writeSchemaToTempDirectory(tempDir, referencedSchema);

            // Process nested references recursively
            if (!referencedSchema.getReferences().isEmpty()) {
                getLogger().debug("Processing {} nested references for schema reference: {}",
                    referencedSchema.getReferences().size(), referenceKey);
                processSchemaReferences(tempDir, referencedSchema.getReferences());
            } else {
                getLogger().debug("No nested references found for schema reference: {}", referenceKey);
            }
        }
    }


    private void deleteDirectory(Path directory) throws IOException {
        if (Files.exists(directory)) {
            try {
                FileUtils.deleteDirectory(directory.toFile());
            } catch (IOException e) {
                getLogger().warn("Failed to delete temporary directory: " + directory, e);
            }
        }
    }
}
