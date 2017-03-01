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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;

@Tags({ "csv", "json", "transform", "registry", "schema" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Transforms CSV content of the Flow File to JSON using the schema provided by the Schema Registry Service.")
public final class TransformCSVToJson extends AbstractCSVTransformer {

    private static final List<PropertyDescriptor> DESCRIPTORS;

    static {
        List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.addAll(BASE_CSV_DESCRIPTORS);
        descriptors.add(QUOTE);
        DESCRIPTORS = Collections.unmodifiableList(descriptors);
    }

    private volatile char quoteChar;

    /**
     *
     */
    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
        this.quoteChar = context.getProperty(QUOTE).getValue().charAt(0);
    }

    /**
     *
     */
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    /**
     *
     */
    @Override
    protected Map<String, String> transform(InputStream in, OutputStream out, InvocationContextProperties contextProperties, Schema schema) {
        GenericRecord avroRecord = CSVUtils.read(in, this.delimiter, schema, this.quoteChar);
        JsonUtils.write(avroRecord, out);
        return Collections.singletonMap(CoreAttributes.MIME_TYPE.key(), "application/json");
    }
}
