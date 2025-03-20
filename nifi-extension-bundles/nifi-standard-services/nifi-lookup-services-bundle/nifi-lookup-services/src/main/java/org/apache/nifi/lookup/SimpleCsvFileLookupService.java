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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

@Tags({"lookup", "cache", "enrich", "join", "csv", "reloadable", "key", "value"})
@CapabilityDescription("A reloadable CSV file-based lookup service. The first line of the csv file is considered as " +
        "header.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.READ_FILESYSTEM,
                        explanation = "Provides operator the ability to read from any file that NiFi has access to.")
        }
)
public class SimpleCsvFileLookupService extends AbstractCSVLookupService implements StringLookupService {

    private static final Set<String> REQUIRED_KEYS = Set.of(KEY);

    public static final PropertyDescriptor LOOKUP_VALUE_COLUMN =
        new PropertyDescriptor.Builder()
            .name("lookup-value-column")
            .displayName("Lookup Value Column")
            .description("Lookup value column.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    private volatile ConcurrentMap<String, String> cache;

    private volatile String lookupValueColumn;

    @Override
    protected void loadCache() throws IllegalStateException, IOException {
        if (lock.tryLock()) {
            try {
                final ComponentLog logger = getLogger();
                if (logger.isDebugEnabled()) {
                    logger.debug("Loading lookup table from file: {}", csvFile);
                }

                final Map<String, String> properties = new HashMap<>();
                try (final InputStream is = new FileInputStream(csvFile)) {
                    try (final InputStreamReader reader = new InputStreamReader(is, charset)) {
                        final Iterable<CSVRecord> records = csvFormat.builder().setHeader().setSkipHeaderRecord(true).get().parse(reader);
                        for (final CSVRecord record : records) {
                            final String key = record.get(lookupKeyColumn);
                            final String value = record.get(lookupValueColumn);
                            if (StringUtils.isBlank(key)) {
                                throw new IllegalStateException("Empty lookup key encountered in: " + csvFile);
                            } else if (!ignoreDuplicates && properties.containsKey(key)) {
                                throw new IllegalStateException("Duplicate lookup key encountered: " + key + " in " + csvFile);
                            } else if (ignoreDuplicates && properties.containsKey(key)) {
                                logger.warn("Duplicate lookup key encountered: {} in {}", key, csvFile);
                            }
                            properties.put(key, value);
                        }
                    }
                }

                this.cache = new ConcurrentHashMap<>(properties);

                if (cache.isEmpty()) {
                    logger.warn("Lookup table is empty after reading file: {}", csvFile);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        super.init(context);
        properties.add(LOOKUP_VALUE_COLUMN);
    }

    @Override
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws IOException, InitializationException {
        super.onEnabled(context);
        this.lookupValueColumn = context.getProperty(LOOKUP_VALUE_COLUMN).evaluateAttributeExpressions().getValue();
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

    @OnDisabled
    public void onDisabled() {
        cache = null;
    }

    // VisibleForTesting
    boolean isCaching() {
        return cache != null;
    }
}
