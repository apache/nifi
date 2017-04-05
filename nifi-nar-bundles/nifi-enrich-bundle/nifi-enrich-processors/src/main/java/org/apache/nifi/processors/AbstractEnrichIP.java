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
package org.apache.nifi.processors;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.maxmind.DatabaseReader;
import org.apache.nifi.util.StopWatch;

public abstract class AbstractEnrichIP extends AbstractProcessor {

    public static final PropertyDescriptor GEO_DATABASE_FILE = new PropertyDescriptor.Builder()
            // Name has been left untouched so that we don't cause a breaking change
            // but ideally this should be renamed to MaxMind Database File or something similar
            .name("Geo Database File")
            .displayName("MaxMind Database File")
            .description("Path to Maxmind IP Enrichment Database File")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor IP_ADDRESS_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("IP Address Attribute")
            .displayName("IP Address Attribute")
            .required(true)
            .description("The name of an attribute whose value is a dotted decimal IP address for which enrichment should occur")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .build();

    public static final Relationship REL_FOUND = new Relationship.Builder()
            .name("found")
            .description("Where to route flow files after successfully enriching attributes with data provided by database")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not found")
            .description("Where to route flow files after unsuccessfully enriching attributes because no data was found")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> propertyDescriptors;
    final AtomicReference<DatabaseReader> databaseReaderRef = new AtomicReference<>(null);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final String dbFileString = context.getProperty(GEO_DATABASE_FILE).getValue();
        final File dbFile = new File(dbFileString);
        final StopWatch stopWatch = new StopWatch(true);
        final DatabaseReader reader = new DatabaseReader.Builder(dbFile).build();
        stopWatch.stop();
        getLogger().info("Completed loading of Maxmind Database.  Elapsed time was {} milliseconds.", new Object[]{stopWatch.getDuration(TimeUnit.MILLISECONDS)});
        databaseReaderRef.set(reader);
    }

    @OnStopped
    public void closeReader() throws IOException {
        final DatabaseReader reader = databaseReaderRef.get();
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_FOUND);
        rels.add(REL_NOT_FOUND);
        this.relationships = Collections.unmodifiableSet(rels);

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(GEO_DATABASE_FILE);
        props.add(IP_ADDRESS_ATTRIBUTE);
        this.propertyDescriptors = Collections.unmodifiableList(props);
    }

}
