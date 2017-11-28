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
package org.apache.nifi.lookup;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.file.monitor.LastModifiedMonitor;
import org.apache.nifi.util.file.monitor.SynchronousFileWatcher;

@Tags({"lookup", "cache", "enrich", "join", "csv", "reloadable", "key", "value"})
@CapabilityDescription("A reloadable CSV file-based lookup service")
public class SimpleCsvFileLookupService extends AbstractControllerService implements StringLookupService {

    private static final String KEY = "key";

    private static final Set<String> REQUIRED_KEYS = Collections.unmodifiableSet(Stream.of(KEY).collect(Collectors.toSet()));

    public static final PropertyDescriptor CSV_FILE =
        new PropertyDescriptor.Builder()
            .name("csv-file")
            .displayName("CSV File")
            .description("A CSV file.")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final PropertyDescriptor CSV_FORMAT = new PropertyDescriptor.Builder()
        .name("CSV Format")
        .description("Specifies which \"format\" the CSV data is in, or specifies if custom formatting should be used.")
        .expressionLanguageSupported(false)
        .allowableValues(Arrays.asList(CSVFormat.Predefined.values()).stream().map(e -> e.toString()).collect(Collectors.toSet()))
        .defaultValue(CSVFormat.Predefined.Default.toString())
        .required(true)
        .build();

    public static final PropertyDescriptor LOOKUP_KEY_COLUMN =
        new PropertyDescriptor.Builder()
            .name("lookup-key-column")
            .displayName("Lookup Key Column")
            .description("Lookup key column.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor LOOKUP_VALUE_COLUMN =
        new PropertyDescriptor.Builder()
            .name("lookup-value-column")
            .displayName("Lookup Value Column")
            .description("Lookup value column.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor IGNORE_DUPLICATES =
        new PropertyDescriptor.Builder()
            .name("ignore-duplicates")
            .displayName("Ignore Duplicates")
            .description("Ignore duplicate keys for records in the CSV file.")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    private List<PropertyDescriptor> properties;

    private volatile ConcurrentMap<String, String> cache;

    private volatile String csvFile;

    private volatile CSVFormat csvFormat;

    private volatile String lookupKeyColumn;

    private volatile String lookupValueColumn;

    private volatile boolean ignoreDuplicates;

    private volatile SynchronousFileWatcher watcher;

    private final ReentrantLock lock = new ReentrantLock();

    private void loadCache() throws IllegalStateException, IOException {
        if (lock.tryLock()) {
            try {
                final ComponentLog logger = getLogger();
                if (logger.isDebugEnabled()) {
                    logger.debug("Loading lookup table from file: " + csvFile);
                }

                final Map<String, String> properties = new HashMap<>();
                final FileReader reader = new FileReader(csvFile);
                final Iterable<CSVRecord> records = csvFormat.withFirstRecordAsHeader().parse(reader);
                for (final CSVRecord record : records) {
                    final String key = record.get(lookupKeyColumn);
                    final String value = record.get(lookupValueColumn);
                    if (StringUtils.isBlank(key)) {
                        throw new IllegalStateException("Empty lookup key encountered in: " + csvFile);
                    } else if (!ignoreDuplicates && properties.containsKey(key)) {
                        throw new IllegalStateException("Duplicate lookup key encountered: " + key + " in " + csvFile);
                    } else if (ignoreDuplicates && properties.containsKey(key)) {
                        logger.warn("Duplicate lookup key encountered: {} in {}", new Object[]{key, csvFile});
                    }
                    properties.put(key, value);
                }

                this.cache = new ConcurrentHashMap<>(properties);

                if (cache.isEmpty()) {
                    logger.warn("Lookup table is empty after reading file: " + csvFile);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) throws InitializationException {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CSV_FILE);
        properties.add(CSV_FORMAT);
        properties.add(LOOKUP_KEY_COLUMN);
        properties.add(LOOKUP_VALUE_COLUMN);
        properties.add(IGNORE_DUPLICATES);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException, FileNotFoundException {
        this.csvFile = context.getProperty(CSV_FILE).getValue();
        this.csvFormat = CSVFormat.Predefined.valueOf(context.getProperty(CSV_FORMAT).getValue()).getFormat();
        this.lookupKeyColumn = context.getProperty(LOOKUP_KEY_COLUMN).getValue();
        this.lookupValueColumn = context.getProperty(LOOKUP_VALUE_COLUMN).getValue();
        this.ignoreDuplicates = context.getProperty(IGNORE_DUPLICATES).asBoolean();
        this.watcher = new SynchronousFileWatcher(Paths.get(csvFile), new LastModifiedMonitor(), 30000L);
        try {
            loadCache();
        } catch (final IllegalStateException e) {
            throw new InitializationException(e.getMessage(), e);
        }
    }

    @Override
    public Optional<String> lookup(final Map<String, Object> coordinates) throws LookupFailureException {
        if (coordinates == null) {
            return Optional.empty();
        }

        final String key = coordinates.get(KEY).toString();
        if (StringUtils.isBlank(key)) {
            return Optional.empty();
        }

        try {
            if (watcher != null && watcher.checkAndReset()) {
                loadCache();
            }
        } catch (final IllegalStateException | IOException e) {
            throw new LookupFailureException(e.getMessage(), e);
        }

        return Optional.ofNullable(cache.get(key));
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }

}
