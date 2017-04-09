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
package org.apache.nifi.processors.avro;

import io.confluent.connect.avro.AvroConverter;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SideEffectFree
@SupportsBatching
@Tags({"kafka", "avro", "convert", "json"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts a Kafka Avro record into a JSON object using Confluent schema registry")
@WritesAttribute(attribute = "mime.type", description = "Sets the mime type to application/json")
public class ConvertKafkaAvroToJSON extends AbstractProcessor {

    private static final byte[] EMPTY_JSON_OBJECT = "{}".getBytes(StandardCharsets.UTF_8);

    static final PropertyDescriptor SCHEMA_REGISTRY_URL = new PropertyDescriptor.Builder()
        .name("Schema registry URL")
        .description("Comma-separated list of URLs for schema registry instances that can be used to register or look up schemas.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .build();
    static final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
        .name("Topic Name")
        .description("Kafka topic to convert")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .expressionLanguageSupported(true)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("A FlowFile is routed to this relationship after it has been converted to JSON")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("A FlowFile is routed to this relationship if it cannot be parsed as Avro or cannot be converted to JSON for any reason")
        .build();

    private List<PropertyDescriptor> properties;
    private AvroConverter avroConverter;
    private JsonConverter jsonConverter;

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SCHEMA_REGISTRY_URL);
        properties.add(TOPIC);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @OnScheduled
    public final void createClient(ProcessContext context) throws InitializationException {
        avroConverter = createAvroConverter();
        jsonConverter = new JsonConverter();
        avroConverter.configure(Collections.singletonMap("schema.registry.url", context.getProperty(SCHEMA_REGISTRY_URL).getValue()), false);
        jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    }

    protected AvroConverter createAvroConverter() {
        return new AvroConverter();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            String topic = context.getProperty(TOPIC).evaluateAttributeExpressions().getValue();
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream rawOut) throws IOException {
                    try (final InputStream in = new BufferedInputStream(rawIn);
                         final OutputStream out = new BufferedOutputStream(rawOut)) {

                        SchemaAndValue schemaAndValue = avroConverter.toConnectData(topic, IOUtils.toByteArray(in));

                        final byte[] outputBytes = (schemaAndValue == null || schemaAndValue.value() == null) ? EMPTY_JSON_OBJECT :
                                jsonConverter.fromConnectData(topic, schemaAndValue.schema(), schemaAndValue.value());
                        out.write(outputBytes);
                    } catch (Exception e) {
                        throw new ProcessException(e);
                    }
                }
            });
        } catch (final ProcessException pe) {
            getLogger().error("Failed to convert {} from Avro to JSON due to {}; transferring to failure", new Object[]{flowFile, pe});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.transfer(flowFile, REL_SUCCESS);
    }

}
