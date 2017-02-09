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
package org.apache.nifi.schemaregistry.processors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;

/**
 * Base processor for implementing transform-like processors for CSV
 * transformations that integrate with Schema Registry (see
 * {@link SchemaRegistry})
 */
abstract class AbstractCSVTransformer extends AbstractContentTransformer {

    static final List<PropertyDescriptor> BASE_CSV_DESCRIPTORS;

    static {
        List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.addAll(BASE_DESCRIPTORS);
        descriptors.add(DELIMITER);
        BASE_CSV_DESCRIPTORS = Collections.unmodifiableList(descriptors);
    }

    protected volatile char delimiter;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return BASE_CSV_DESCRIPTORS;
    }

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
        this.delimiter = context.getProperty(DELIMITER).getValue().charAt(0);
    }
}
