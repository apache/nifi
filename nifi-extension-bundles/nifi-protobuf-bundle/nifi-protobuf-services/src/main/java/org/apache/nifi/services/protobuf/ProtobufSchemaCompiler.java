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
import org.apache.commons.io.FileUtils;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.serialization.record.SchemaIdentifier;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Handles Protocol Buffer schema compilation, caching, and temporary directory operations.
 * This class is responsible for compiling schema definitions into Wire Schema objects,
 * managing a cache of compiled schemas, and handling temporary directory operations
 * required during the compilation process.
 */
final class ProtobufSchemaCompiler {

    private static final List<Location> STANDARD_PROTOBUF_LOCATIONS = Arrays.asList(
        Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/any.proto"),
        Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/duration.proto"),
        Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/empty.proto"),
        Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/struct.proto"),
        Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/timestamp.proto"),
        Location.get(CoreLoaderKt.WIRE_RUNTIME_JAR, "google/protobuf/wrappers.proto")
    );
    private static final int CACHE_EXPIRE_HOURS = 1;
    private static final int COMPILED_SCHEMAS_CACHE_SIZE = 200;

    private final Cache<SchemaIdentifier, Schema> compiledSchemaCache;
    private final ComponentLog logger;

    /**
     * Creates a new ProtobufSchemaCompiler with default cache settings.
     *
     * @param logger the component logger for logging compilation activities
     */
    public ProtobufSchemaCompiler(final ComponentLog logger) {
        this.logger = logger;
        this.compiledSchemaCache = Caffeine.newBuilder()
            .expireAfterAccess(CACHE_EXPIRE_HOURS, TimeUnit.HOURS)
            .maximumSize(COMPILED_SCHEMAS_CACHE_SIZE)
            .build();
    }

    /**
     * Compiles a schema definition or retrieves it from cache.
     *
     * @param schemaDefinition the schema definition to compile
     * @return the compiled Schema
     */
    public Schema compileOrGetFromCache(final SchemaDefinition schemaDefinition) {
        return compiledSchemaCache.get(schemaDefinition.getIdentifier(),
            identifier -> {
                try {
                    return compileSchemaDefinition(schemaDefinition);
                } catch (final IOException e) {
                    throw new RuntimeException("Could not compile schema for identifier: " + identifier, e);
                }
            });
    }

    /**
     * Invalidates all cached schemas.
     */
    public void invalidateCache() {
        if (compiledSchemaCache != null) {
            compiledSchemaCache.invalidateAll();
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
    private Schema compileSchemaDefinition(final SchemaDefinition schemaDefinition) throws IOException {
        logger.debug("Starting schema compilation for identifier: {}", schemaDefinition.getIdentifier());

        // Validate that all schema identifiers end with .proto extension
        validateSchemaDefinitionIdentifiers(schemaDefinition, true);

        return executeWithTemporaryDirectory(tempDir -> {
            try {
                // Process main schema definition
                writeSchemaToTempDirectory(tempDir, schemaDefinition);

                // Process all referenced schemas recursively
                processSchemaReferences(tempDir, schemaDefinition.getReferences());

                // Create and configure schema loader
                final Schema compiledSchema = createAndLoadSchema(tempDir);
                logger.debug("Successfully compiled schema for identifier: {}", schemaDefinition.getIdentifier());
                return compiledSchema;

            } catch (final Exception e) {
                throw new RuntimeException("Failed to compile Protobuf schema for identifier: " + schemaDefinition.getIdentifier(), e);
            }
        });
    }

    /**
     * Executes a function with a temporary directory, ensuring proper cleanup.
     *
     * @param function the function to execute with the temporary directory
     * @return the result of the function
     * @throws IOException if unable to create or manage temporary directory
     */
    private <T> T executeWithTemporaryDirectory(final WithTemporaryDirectory<T> function) throws IOException {
        final Path tempDir = Files.createTempDirectory("nifi-protobuf-schema");
        logger.debug("Created temporary directory for schema compilation: {}", tempDir);

        try {
            return function.apply(tempDir);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            safeDeleteDirectory(tempDir);
        }
    }

    private Schema createAndLoadSchema(final Path tempDir) {
        final SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());

        final List<Location> roots = new ArrayList<>();
        roots.add(Location.get(tempDir.toString()));

        // Add standard protobuf libraries
        roots.addAll(STANDARD_PROTOBUF_LOCATIONS);

        schemaLoader.initRoots(roots, Collections.emptyList());

        // Load and return the compiled schema
        return schemaLoader.loadSchema();
    }

    private void safeDeleteDirectory(final Path directory) {
        if (Files.exists(directory)) {
            try {
                FileUtils.deleteDirectory(directory.toFile());
            } catch (final IOException | IllegalArgumentException e) {
                logger.warn("Failed to delete temporary directory: {}", directory, e);
            }
        }
    }

    /**
     * Validates that all SchemaDefinition identifiers end with .proto extension.
     * Performs recursive validation on all referenced schemas.
     *
     * @param schemaDefinition       the schema definition to validate
     * @param isRootSchemaDefinition set to true if schema definition is a root definition, false otherwise
     * @throws IllegalArgumentException if any identifier does not end with .proto extension
     */
    void validateSchemaDefinitionIdentifiers(final SchemaDefinition schemaDefinition, final boolean isRootSchemaDefinition) {
        // do not validate schema identifier names for root schema definitions. They might be coming from sources like text fields,
        // flow file attributes and other sources that do not support naming.
        if (!isRootSchemaDefinition) {
            validateSchemaIdentifier(schemaDefinition.getIdentifier());
        }

        // Recursively validate all referenced schemas
        // schema references have to end with .proto extension.
        for (final SchemaDefinition referencedSchema : schemaDefinition.getReferences().values()) {
            validateSchemaDefinitionIdentifiers(referencedSchema, false);
        }
    }

    /**
     * Validates that a single SchemaIdentifier has a name ending with .proto extension.
     *
     * @param schemaIdentifier the schema identifier to validate
     * @throws IllegalArgumentException if the identifier name does not end with .proto extension
     */
    private void validateSchemaIdentifier(final SchemaIdentifier schemaIdentifier) {
        schemaIdentifier.getName()
            .filter(name -> name.endsWith(".proto"))
            .orElseThrow(() -> new IllegalArgumentException("Schema identifier must have a name that ends with .proto extension"));
    }

    /**
     * Writes a schema definition to the temporary directory structure.
     * If package name is present, creates the appropriate directory structure.
     *
     * @param tempDir          the temporary directory root
     * @param schemaDefinition the schema definition to write
     * @throws IOException if unable to create directories or write files
     */
    private void writeSchemaToTempDirectory(final Path tempDir, final SchemaDefinition schemaDefinition) throws IOException {
        logger.debug("Writing schema definition to temporary directory. Identifier: {}", schemaDefinition.getIdentifier());

        final String schemaFileName = generateSchemaFileName(schemaDefinition);
        final Path schemaFile = tempDir.resolve(schemaFileName);

        // Write schema text to file
        Files.write(schemaFile, schemaDefinition.getText().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        logger.debug("Successfully wrote schema to file: {} (string length: {})",
            schemaFile, schemaDefinition.getText().length());
    }

    /**
     * Generates a filename for a schema definition, ensuring it has a .proto extension.
     *
     * @param schemaDefinition the schema definition
     * @return the generated filename
     */
    private String generateSchemaFileName(final SchemaDefinition schemaDefinition) {
        String schemaFileName = schemaDefinition.getIdentifier().getName().orElseGet(
            () -> String.valueOf(schemaDefinition.getIdentifier().getSchemaVersionId().orElse(0L))
        );

        if (!schemaFileName.endsWith(".proto")) {
            schemaFileName += ".proto"; // Ensure the file ends with .proto, otherwise the wire library will not recognize it
        }

        return schemaFileName;
    }

    private void processSchemaReferences(final Path tempDir, final Map<String, SchemaDefinition> references) throws IOException {
        logger.debug("Processing schema references. Count: {}, Temp directory: {}",
            references.size(), tempDir);

        for (final Map.Entry<String, SchemaDefinition> entry : references.entrySet()) {
            final String referenceKey = entry.getKey();
            final SchemaDefinition referencedSchema = entry.getValue();

            logger.debug("Processing schema reference - Key: {}, Identifier: {}",
                referenceKey, referencedSchema.getIdentifier());

            // Write referenced schema to appropriate directory
            writeSchemaToTempDirectory(tempDir, referencedSchema);

            // Process nested references recursively
            if (!referencedSchema.getReferences().isEmpty()) {
                logger.debug("Processing {} nested references for schema reference: {}", referencedSchema.getReferences().size(), referenceKey);
                processSchemaReferences(tempDir, referencedSchema.getReferences());
            } else {
                logger.debug("No nested references found for schema reference: {}", referenceKey);
            }
        }
    }


    @FunctionalInterface
    private interface WithTemporaryDirectory<T> {
        T apply(Path tempDir) throws Exception;
    }
}
