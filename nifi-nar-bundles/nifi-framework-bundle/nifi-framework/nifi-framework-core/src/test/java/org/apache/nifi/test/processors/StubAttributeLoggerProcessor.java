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
package org.apache.nifi.test.processors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;

@SideEffectFree
public class StubAttributeLoggerProcessor extends AbstractProcessor {

    //@formatter:off
    public static final PropertyDescriptor LOG_LEVEL = new PropertyDescriptor.Builder()
            .required(false)
            .description("log.level")
            .defaultValue("debug")
            .name("log.level")
            .allowableValues("trace", "info", "warn", "debug", "error")
            .sensitive(false)
            .build();
    public static final PropertyDescriptor ATTRIBUTES_TO_LOG_CSV = new PropertyDescriptor.Builder()
            .required(false)
            .description("attributes.to.log.csv")
            .name("attributes.to.log.csv")
            .sensitive(false)
            .build();
    public static final PropertyDescriptor ATTRIBUTES_TO_IGNORE_CSV = new PropertyDescriptor.Builder()
            .required(false)
            .description("attributes.to.ignore.csv")
            .name("attributes.to.ignore.csv")
            .sensitive(false)
            .build();
    public static final PropertyDescriptor LOG_PAYLOAD = new PropertyDescriptor.Builder()
            .required(false)
            .description("log.payload")
            .defaultValue("false")
            .allowableValues("true", "false")
            .name("log.payload")
            .sensitive(false)
            .build();
    // @formatter:on

    public static enum DebugLevels {

        trace, info, warn, debug, error
    }

    public static final long ONE_MB = 1024 * 1024;
    private final Set<Relationship> relationships;
    public static final Relationship REL_SUCCESS = new Relationship.Builder().description("success").name("success").build();

    private final List<PropertyDescriptor> supportedDescriptors;

    public StubAttributeLoggerProcessor() {
        // relationships
        final Set<Relationship> procRels = new HashSet<>();
        procRels.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(procRels);

        // descriptors
        final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
        supDescriptors.add(ATTRIBUTES_TO_IGNORE_CSV);
        supDescriptors.add(ATTRIBUTES_TO_LOG_CSV);
        supDescriptors.add(LOG_PAYLOAD);
        supDescriptors.add(LOG_LEVEL);
        supportedDescriptors = Collections.unmodifiableList(supDescriptors);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return supportedDescriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
    }

}
