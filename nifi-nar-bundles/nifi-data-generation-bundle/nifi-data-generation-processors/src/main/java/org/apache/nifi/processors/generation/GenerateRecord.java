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

package org.apache.nifi.processors.generation;

import io.confluent.avro.random.generator.Generator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
public class GenerateRecord extends AbstractProcessor {
    static final PropertyDescriptor WRITER = new PropertyDescriptor.Builder()
        .name("generate-record-writer")
        .displayName("Record Writer")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .description("The record writer to use for serializing generated records.")
        .required(true)
        .build();

    static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
        .name("generate-record-schema")
        .displayName("Schema")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .description("An Avro schema to use for generating records.")
        .required(false)
        .addValidator(Validator.VALID)
        .build();

    static final PropertyDescriptor LIMIT = new PropertyDescriptor.Builder()
        .name("generate-record-limit")
        .displayName("Limit")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .description("The maximum number of records to generate per run. It is regulated by the Fixed Size configuration " +
                "property.")
        .defaultValue("25")
        .required(false)
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    static final PropertyDescriptor FIXED_SIZE = new PropertyDescriptor.Builder()
        .name("generate-record-fixed-size")
        .displayName("Fixed Size")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .description("If true, the limit configuration will be used to generate a consistently sized record set. If false " +
                "the limit value will be the ceiling of a random number range from 1 to that value.")
        .defaultValue("true")
        .allowableValues("true", "false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If there is an error building a record set, the input will go here.")
        .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Generated results go to this relationship.")
        .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("If input is provided, the input flowfile gets routed to this relationship.")
        .build();

    static final List<PropertyDescriptor> PROPS;
    static final Set<Relationship> RELS;

    static {
        List<PropertyDescriptor> _temp = new ArrayList<>();
        _temp.add(WRITER);
        _temp.add(SCHEMA);
        _temp.add(LIMIT);
        _temp.add(FIXED_SIZE);

        Set<Relationship> _rels = new HashSet<>();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        _rels.add(REL_ORIGINAL);

        PROPS = Collections.unmodifiableList(_temp);
        RELS  = Collections.unmodifiableSet(_rels);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELS;
    }

    private volatile RecordSetWriterFactory writerFactory;

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        writerFactory = context.getProperty(WRITER).asControllerService(RecordSetWriterFactory.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = null;
        if (context.hasIncomingConnection()) {
            input = session.get();

            if (input == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final int limit;
        final boolean fixed = context.getProperty(FIXED_SIZE).asBoolean();
        if (fixed) {
            limit = context.getProperty(LIMIT).evaluateAttributeExpressions(input).asInteger();
        } else {
            int ceiling = context.getProperty(LIMIT).evaluateAttributeExpressions(input).asInteger();
            limit = new Random().nextInt(ceiling) + 1;
        }

        final Generator generator;
        final RecordSchema schema;

        FlowFile out = session.create(input);

        try {
            if (!context.getProperty(SCHEMA).isSet() && input != null) {
                schema = writerFactory.getSchema(input.getAttributes(), null);
                String text = schema.getSchemaText().get();
                generator = new Generator(text, new Random());
            } else if (!context.getProperty(SCHEMA).isSet() && input == null) {
                throw new ProcessException("When there is no incoming connection, a avro schema must be set " +
                        "in the Schema configuration property for this processor.");
            } else {
                Schema parsed = new Schema.Parser().parse(context.getProperty(SCHEMA).getValue());
                schema = AvroTypeUtil.createSchema(parsed);
                generator = new Generator(parsed, new Random());
            }

            List<Record> records = new ArrayList<>();
            for (int x = 0; x < limit; x++) {
                GenericData.Record o = (GenericData.Record) generator.generate();
                Map y = AvroTypeUtil.convertAvroRecordToMap(o, schema);
                records.add(new MapRecord(schema, y));
            }

            final AtomicInteger integer = new AtomicInteger();
            out = session.write(out, outputStream -> {
                try {
                    RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, outputStream);
                    writer.beginRecordSet();
                    for (int x = 0; x < records.size(); x++) {
                        writer.write(records.get(x));
                    }
                    WriteResult result = writer.finishRecordSet();
                    writer.close();

                    integer.set(result.getRecordCount());
                } catch (SchemaNotFoundException e) {
                    getLogger().error(e.getMessage());
                    throw new ProcessException(e);
                }
            });

            out = session.putAttribute(out, "record.count", String.valueOf(integer.get()));

            session.transfer(out, REL_SUCCESS);
            if (input != null) {
                session.transfer(input, REL_ORIGINAL);
            }
        } catch (Exception ex) {
            getLogger().error(ex.getMessage());
            if (input != null) {
                session.transfer(input, REL_FAILURE);
            }

            session.remove(out);
        }
    }
}
