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
package org.apache.nifi.schemaregistry.services;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.file.monitor.LastModifiedMonitor;
import org.apache.nifi.util.file.monitor.SynchronousFileWatcher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Tags({"schema", "registry", "directory", "avro", "json", "csv"})
@CapabilityDescription("Provides a service for registering and accessing schemas. " +
        "Schemas are loaded from a directory on disk into memory and are easily accessible per a " +
        "naming strategy. The schema directory is scanned at intervals for schema " +
        "updates, deletions and additions thereby allowing for dynamic schema changes without " +
        "the need to start and stop this service.")
public class AvroSchemaDirectoryRegistry extends AbstractControllerService implements SchemaRegistry {
    private static final Set<SchemaField> SCHEMA_FIELDS =
            EnumSet.of(SchemaField.SCHEMA_NAME, SchemaField.SCHEMA_TEXT, SchemaField.SCHEMA_TEXT_FORMAT);

    enum NamingStrategy implements DescribedValue {
        FULL_PATH("full path", "Full path of schema"),
        FILE_NAME("file name", "File name of schema with a file extension"),
        BASE_NAME("base name", "File name of schema without a file extension");

        private final String displayName;
        private final String description;

        NamingStrategy(String displayName, String description) {
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

    static final PropertyDescriptor SCHEMA_DIRECTORY = new PropertyDescriptor.Builder()
            .name("schema-directory")
            .displayName("Schema Directory")
            .description("Existing directory where schema files are stored.")
            .required(true)
            .addValidator(StandardValidators.createDirectoryExistsValidator(false, false))
            .build();

    static final PropertyDescriptor RELOAD_INTERVAL = new PropertyDescriptor.Builder()
            .name("Reload Interval")
            .description("Allowed elapse time before checking for schema updates")
            .defaultValue("60 min")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor VALIDATE_FIELD_NAMES = new PropertyDescriptor.Builder()
            .name("avro-reg-validated-field-names")
            .displayName("Validate Field Names")
            .description("Whether or not to validate the field names in the Avro schema based on Avro naming rules. If set to true, all field names must be valid Avro names, "
                    + "which must begin with [A-Za-z_], and subsequently contain only [A-Za-z0-9_]. If set to false, no validation will be performed on the field names.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    static final PropertyDescriptor SCHEMA_NAMING_STRATEGY = new PropertyDescriptor.Builder()
            .name("schema-naming-strategy")
            .displayName("Schema Naming Strategy")
            .description("Strategy for choosing how to name a schema which is used as part of the schema identifier" +
                    " and the name used to reference the schema from the in memory cache of schemas.")
            .allowableValues(NamingStrategy.class)
            .defaultValue(NamingStrategy.BASE_NAME.getValue())
            .required(true)
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
            Arrays.asList(SCHEMA_DIRECTORY, RELOAD_INTERVAL, VALIDATE_FIELD_NAMES, SCHEMA_NAMING_STRATEGY);

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ConcurrentMap<String, RecordSchema> recordSchemas = new ConcurrentHashMap<>();
    private ScheduledExecutorService scheduledExecutorService;
    private Path schemaDirectory;
    private NamingStrategy schemaNamingStrategy;
    private SynchronousFileWatcher synchronousFileWatcher;


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> validationResults = new HashSet<>();
        boolean validateFieldNames = validationContext.getProperty(VALIDATE_FIELD_NAMES).asBoolean();
        schemaDirectory = Paths.get(validationContext.getProperty(SCHEMA_DIRECTORY).getValue());
        List<Path> files;

        readWriteLock.readLock().lock();
        try {
            files = getSchemaDirectoryFiles();
            getLogger().info("Schema directory {} has {} JSON file(s)", schemaDirectory, files.size());
            if(files.isEmpty()) {
                validationResults.add(new ValidationResult.Builder()
                        .subject(schemaDirectory.toString())
                        .valid(false)
                        .explanation(String.format("Directory %s has no JSON files", schemaDirectory))
                        .build());
                return validationResults;
            }

            files.forEach(path -> {
                try {
                    String text = getText(path);
                    final Schema schema = getSchema(text, validateFieldNames);
                    AvroTypeUtil.createSchema(schema, text, SchemaIdentifier.EMPTY);
                } catch (final Exception e) {
                    String commonMsg = String.format("%s is not a valid Avro schema", path);
                    getLogger().error("{}", commonMsg, e);
                    validationResults.add(new ValidationResult.Builder()
                            .subject(path.toString())
                            .valid(false)
                            .explanation(commonMsg + ", cause: " + e.getMessage())
                            .build());
                }
            });
        } catch (Exception e) {
            String msg = String.format("Could not get file(s) from path %s; error: %s", schemaDirectory, e.getMessage());
            getLogger().error("{}", msg, e);
            validationResults.add(new ValidationResult.Builder()
                    .subject(schemaDirectory.toString())
                    .valid(false)
                    .explanation(msg)
                    .build());
            return validationResults;
        } finally {
            readWriteLock.readLock().unlock();
        }

        return validationResults;
    }

    /**
     * Method which retrieves all JSON files from the specified directory and all children directories.
     * @return java.io.List of java.nio.file.Path objects of JSON files from
     * the schema directory and all its children directories.
     * @throws IOException if an I/O error occurs when opening the schema directory.
     */
    private List<Path> getSchemaDirectoryFiles() throws IOException {
        try (Stream<Path> walk = Files.walk(schemaDirectory)) {
            return walk.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".json"))
                    .collect(Collectors.toList());
        }
    }

    @SuppressWarnings("ReadWriteStringCanBeUsed")
    private String getText(Path schemaPath) throws IOException {
        return new String(Files.readAllBytes(schemaPath), StandardCharsets.UTF_8);
    }

    private Schema getSchema(String text, boolean validate) throws SchemaParseException {
        return new Schema.Parser().setValidate(validate).parse(text);
    }

    @SuppressWarnings("unused")
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        schemaDirectory = Paths.get(context.getProperty(SCHEMA_DIRECTORY).getValue());
        schemaNamingStrategy = NamingStrategy.valueOf(context.getProperty(SCHEMA_NAMING_STRATEGY).getValue());
        loadRecordSchemas();
        synchronousFileWatcher = new SynchronousFileWatcher(schemaDirectory, new LastModifiedMonitor());
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        SchemaLoader schemaLoader = new SchemaLoader();
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        long reloadInterval = context.getProperty(RELOAD_INTERVAL).asTimePeriod(timeUnit);
        scheduledExecutorService.scheduleWithFixedDelay(schemaLoader, 0, reloadInterval, timeUnit);
    }

    /**
     * Method which loads all the schemas which are located in the specified directory.
     * <b>NOTE:</b> The original cache is only cleared if there can be schemas loaded at the given intervals
     * otherwise the cache remains the same thereby never allowing this controller service to be in
     * an unstable state after attempting to reload at a given interval.
     */
    private void loadRecordSchemas() {
        readWriteLock.readLock().lock();
        try {
            List<Path> files = getSchemaDirectoryFiles();
            Map<String, RecordSchema> latestRecordSchemas = getLatestRecordSchemas(files);
            int totalRecordSchemas = recordSchemas.size();

            if (!latestRecordSchemas.isEmpty()) {
                recordSchemas.clear();
                recordSchemas.putAll(latestRecordSchemas);
                getLogger().info("Cleared {} record schema(s) and replaced with {} record schema(s)",
                        totalRecordSchemas, recordSchemas.size());
                //TODO Change log level to debug
                getLogger().info("Latest record schema names are {}",
                        getCommaSeparatedSchemaNames());
            } else {
                getLogger().warn("Left {} record schema(s) in place as there were no latest ones to load."
                        + " Listing of record schema names left in place are:\n {}",
                        totalRecordSchemas, getCommaSeparatedSchemaNames());
            }
        } catch (Exception e) {
            getLogger().error("Could not get files from directory {}; error: ", schemaDirectory, e);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private Map<String, RecordSchema> getLatestRecordSchemas(List<Path> files) {
        final Map<String, RecordSchema> latestRecordSchemas = new HashMap<>();
        files.forEach(path -> addRecordSchema(latestRecordSchemas, path));

        return latestRecordSchemas;
    }

    private void addRecordSchema(Map<String, RecordSchema> recordSchemas, Path schemaPath) {
        try {
            String text = getText(schemaPath);
            Schema schema = getSchema(text, false);
            String schemaName = getSchemaName(schemaPath);
            RecordSchema recordSchema = AvroTypeUtil.createSchema(schema, text,
                    SchemaIdentifier.builder().name(schemaName).build());
            recordSchemas.put(schemaName, recordSchema);
        } catch (Exception e) {
            getLogger().error("Could not create {} from {}, details:", Schema.class, schemaPath, e);
        }
    }

    private String getSchemaName(Path schemaPath) {
        String fullPath = schemaPath.toString();
        switch(schemaNamingStrategy) {
            case FULL_PATH:
                return fullPath;
            case FILE_NAME:
                return FilenameUtils.getName(fullPath);
            case BASE_NAME:
                return FilenameUtils.getBaseName(fullPath);
            default:
                return "";
        }
    }

    private String getCommaSeparatedSchemaNames() {
        return String.join(", ", recordSchemas.keySet());
    }

    @SuppressWarnings("unused")
    @OnDisabled
    public void onDisabled() {
        scheduledExecutorService.shutdown();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    public RecordSchema retrieveSchema(SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
        final Optional<String> schemaName = schemaIdentifier.getName();
        if (schemaName.isPresent()) {
            return retrieveSchemaByName(schemaName.get());
        } else {
            throw new SchemaNotFoundException("This Schema Registry only supports retrieving a schema by name.");
        }
    }

    private RecordSchema retrieveSchemaByName(final String schemaName) throws SchemaNotFoundException {
        final RecordSchema recordSchema = recordSchemas.get(schemaName);
        if (recordSchema == null) {
            throw new SchemaNotFoundException("Unable to find schema with name '" + schemaName + "'");
        }
        return recordSchema;
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return new HashSet<>(SCHEMA_FIELDS);
    }

    private class SchemaLoader implements Runnable {
        @Override
        public void run() {
            try {
                if (synchronousFileWatcher.checkAndReset()) {
                    loadRecordSchemas();
                }
            } catch (IOException e) {
                getLogger().error("Failed to check file watcher!", e);
            }
        }
    }
}
