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

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tags({"lookup", "cache", "enrich", "join", "csv", "reloadable", "key", "value", "record"})
@CapabilityDescription(
        "A reloadable CSV file-based lookup service. When the lookup key is found in the CSV file, " +
        "the columns are returned as a Record. All returned fields will be strings. The first line of the csv file " +
        "is considered as header."
)
public class CSVRecordLookupService extends AbstractCSVLookupService implements RecordLookupService {

    private static final Set<String> REQUIRED_KEYS = Collections.unmodifiableSet(Stream.of(KEY).collect(Collectors.toSet()));

    static final PropertyDescriptor CSV_FORMAT = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractCSVLookupService.CSV_FORMAT)
            .name("csv-format")
            .build();

    private volatile ConcurrentMap<String, Record> cache;

    @Override
    protected void loadCache() throws IllegalStateException, IOException {
        if (lock.tryLock()) {
            try {
                final ComponentLog logger = getLogger();
                if (logger.isDebugEnabled()) {
                    logger.debug("Loading lookup table from file: " + csvFile);
                }

                ConcurrentHashMap<String, Record> cache = new ConcurrentHashMap<>();
                try (final InputStream is = new FileInputStream(csvFile)) {
                    try (final InputStreamReader reader = new InputStreamReader(is, charset)) {
                        final CSVParser records = csvFormat.withFirstRecordAsHeader().parse(reader);
                        RecordSchema lookupRecordSchema = null;
                        for (final CSVRecord record : records) {
                            final String key = record.get(lookupKeyColumn);

                            if (StringUtils.isBlank(key)) {
                                throw new IllegalStateException("Empty lookup key encountered in: " + csvFile);
                            } else if (!ignoreDuplicates && cache.containsKey(key)) {
                                throw new IllegalStateException("Duplicate lookup key encountered: " + key + " in " + csvFile);
                            } else if (ignoreDuplicates && cache.containsKey(key)) {
                                logger.warn("Duplicate lookup key encountered: {} in {}", new Object[]{key, csvFile});
                            }

                            // Put each key/value pair (except the lookup) into the properties
                            final Map<String, Object> properties = new HashMap<>();
                            record.toMap().forEach((k, v) -> {
                                if (!lookupKeyColumn.equals(k)) {
                                    properties.put(k, v);
                                }
                            });

                            if (lookupRecordSchema == null) {
                                List<RecordField> recordFields = new ArrayList<>(properties.size());
                                properties.forEach((k, v) -> recordFields.add(new RecordField(k, RecordFieldType.STRING.getDataType())));
                                lookupRecordSchema = new SimpleRecordSchema(recordFields);
                            }

                            cache.put(key, new MapRecord(lookupRecordSchema, properties));
                        }
                    }
                }

                this.cache = cache;

                if (cache.isEmpty()) {
                    logger.warn("Lookup table is empty after reading file: " + csvFile);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @OnEnabled
    @Override
    public void onEnabled(final ConfigurationContext context) throws InitializationException, IOException {
        super.onEnabled(context);
        try {
            loadCache();
        } catch (final IllegalStateException e) {
            throw new InitializationException(e.getMessage(), e);
        }
    }

    @Override
    public Optional<Record> lookup(final Map<String, Object> coordinates) throws LookupFailureException {
        if (coordinates == null) {
            return Optional.empty();
        }

        final String key = (String)coordinates.get(KEY);
        if (StringUtils.isBlank(key)) {
            return Optional.empty();
        }

        try {
            if (watcher.checkAndReset()) {
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
