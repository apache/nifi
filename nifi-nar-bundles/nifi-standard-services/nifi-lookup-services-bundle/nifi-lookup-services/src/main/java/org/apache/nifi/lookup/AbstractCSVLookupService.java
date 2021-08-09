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

import org.apache.commons.csv.CSVFormat;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.file.monitor.LastModifiedMonitor;
import org.apache.nifi.util.file.monitor.SynchronousFileWatcher;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

public abstract class AbstractCSVLookupService extends AbstractControllerService {

    protected static final String KEY = "key";
    public static final AllowableValue RFC4180 = new AllowableValue("RFC4180", "RFC4180",
            "Same as RFC 4180. Available for compatibility reasons.");
    public static final AllowableValue DEFAULT = new AllowableValue("default", "Default Format",
            "Same as custom format. Available for compatibility reasons.");

    public static final PropertyDescriptor CSV_FILE =
            new PropertyDescriptor.Builder()
                    .name("csv-file")
                    .displayName("CSV File")
                    .description("Path to a CSV File in which the key value pairs can be looked up.")
                    .required(true)
                    .identifiesExternalResource(ResourceCardinality.SINGLE, ResourceType.FILE)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                    .build();

    public static final PropertyDescriptor CHARSET =
            new PropertyDescriptor.Builder()
                    .fromPropertyDescriptor(CSVUtils.CHARSET)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                    .name("Character Set")
                    .description("The Character Encoding that is used to decode the CSV file.")
                    .build();

    public static final PropertyDescriptor CSV_FORMAT =
            new PropertyDescriptor.Builder()
                    .fromPropertyDescriptor(CSVUtils.CSV_FORMAT)
                    .allowableValues(Stream.concat(
                            CSVUtils.CSV_FORMAT.getAllowableValues().stream(),
                            Stream.of(DEFAULT, RFC4180)).toArray(AllowableValue[]::new))
                    .defaultValue(DEFAULT.getValue())
                    .build();

    public static final PropertyDescriptor LOOKUP_KEY_COLUMN =
            new PropertyDescriptor.Builder()
                    .name("lookup-key-column")
                    .displayName("Lookup Key Column")
                    .description("The field in the CSV file that will serve as the lookup key. " +
                            "This is the field that will be matched against the property specified in the lookup processor.")
                    .required(true)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
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

    protected List<PropertyDescriptor> properties;

    protected volatile String csvFile;

    protected volatile CSVFormat csvFormat;

    protected volatile String charset;

    protected volatile String lookupKeyColumn;

    protected volatile boolean ignoreDuplicates;

    protected volatile SynchronousFileWatcher watcher;

    protected final ReentrantLock lock = new ReentrantLock();

    protected abstract void loadCache() throws IllegalStateException, IOException;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        this.properties = new ArrayList<>();
        properties.add(CSV_FILE);
        properties.add(CSV_FORMAT);
        properties.add(CHARSET);
        properties.add(LOOKUP_KEY_COLUMN);
        properties.add(IGNORE_DUPLICATES);

        properties.add(CSVUtils.VALUE_SEPARATOR);
        properties.add(CSVUtils.QUOTE_CHAR);
        properties.add(CSVUtils.QUOTE_MODE);
        properties.add(CSVUtils.COMMENT_MARKER);
        properties.add(CSVUtils.ESCAPE_CHAR);
        properties.add(CSVUtils.TRIM_FIELDS);
    }

    public void onEnabled(final ConfigurationContext context) throws IOException, InitializationException {
        this.csvFile = context.getProperty(CSV_FILE).evaluateAttributeExpressions().getValue();
        if( context.getProperty(CSV_FORMAT).getValue().equalsIgnoreCase(RFC4180.getValue()) ) {
            this.csvFormat = CSVFormat.RFC4180;
        } else {
            this.csvFormat = CSVUtils.createCSVFormat(context, Collections.emptyMap());
        }
        this.charset = context.getProperty(CHARSET).evaluateAttributeExpressions().getValue();
        this.lookupKeyColumn = context.getProperty(LOOKUP_KEY_COLUMN).evaluateAttributeExpressions().getValue();
        this.ignoreDuplicates = context.getProperty(IGNORE_DUPLICATES).asBoolean();
        this.watcher = new SynchronousFileWatcher(Paths.get(csvFile), new LastModifiedMonitor(), 30000L);
    }
}

