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
package org.apache.nifi.rules.processors;

import org.apache.avro.Schema;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.rules.Action;
import org.apache.nifi.rules.engine.RulesEngineService;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.ListRecordSet;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"record", "rules", "action", "rules engine"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "The number of records in an outgoing FlowFile"),
        @WritesAttribute(attribute = "mime.type", description = "The MIME Type that the configured Record Writer indicates is appropriate"),
})
@CapabilityDescription("Submits record values to a rules engine and returns actions determined by the engine as records ")
public class RulesRecordProcessor extends AbstractProcessor {

    static final PropertyDescriptor FACTS_RECORD_READER = new PropertyDescriptor.Builder()
            .name("fact-record-reader")
            .displayName("Fact Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming fact data and determining the data's schema.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor ACTION_RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("action-record-writer")
            .displayName("Action Record Writer")
            .description("Specifies the Controller Service to use for writing out action records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RULES_ENGINE = new PropertyDescriptor.Builder()
            .name("rules-engine-service")
            .displayName("Rules Engine Service")
            .description("Specifies the Controller Service to use for applying rules to metrics.")
            .identifiesControllerService(RulesEngineService.class)
            .required(true)
            .build();

    static final Relationship REL_ACTIONS = new Relationship.Builder()
            .name("actions")
            .description("If an incoming FlowFile triggers actions then action FlowFiles will be routed " +
                    "to this relationship")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If an error occurs when processing a FlowFile against the rules engine then the FlowFile " +
                    "will be routed to this relationship")
            .build();
    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile received. If the FlowFile fails processing, nothing will be sent to this relationship")
            .build();

    protected volatile RecordSchema recordSchema;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(FACTS_RECORD_READER);
        properties.add(ACTION_RECORD_WRITER);
        properties.add(RULES_ENGINE);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ACTIONS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        return relationships;
    }

    @OnScheduled
    public void setup(final ProcessContext context) throws IOException {
        final InputStream schema = getClass().getClassLoader().getResourceAsStream("schema-actions.avsc");
        recordSchema = AvroTypeUtil.createSchema(new Schema.Parser().parse(schema));
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final RecordReaderFactory readerFactory = context.getProperty(FACTS_RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(ACTION_RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final RulesEngineService rulesEngineService = context.getProperty(RULES_ENGINE).asControllerService(RulesEngineService.class);

        try (final InputStream in = session.read(original);
             final RecordReader reader = readerFactory.createRecordReader(original, in, getLogger())) {
            FlowFile action = session.create(original);
            final Map<String, String> attributes = new HashMap<>();
            final WriteResult writeResult;

            try {
                final Record firstRecord = reader.nextRecord();
                if (firstRecord == null) {
                    try (final OutputStream out = session.write(action);
                         final RecordSetWriter writer = writerFactory.createWriter(getLogger(), recordSchema, out, action)) {

                        writer.beginRecordSet();
                        writeResult = writer.finishRecordSet();

                        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        attributes.putAll(writeResult.getAttributes());
                    }

                    action = session.putAllAttributes(action, attributes);
                    session.transfer(action, REL_ACTIONS);
                    logger.info("{} had no Records to transform", new Object[]{original});
                } else {

                    final RecordSet transformedFirstRecordSet = processRecord(firstRecord, rulesEngineService);
                    final RecordSchema writeSchema = writerFactory.getSchema(original.getAttributes(), recordSchema);
                    Record currentRecord;
                    try (final OutputStream out = session.write(action);
                         final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, action)) {

                        writer.beginRecordSet();

                        while ((currentRecord = transformedFirstRecordSet.next()) != null) {
                            writer.write(currentRecord);
                        }

                        Record record;
                        while ((record = reader.nextRecord()) != null) {
                            final RecordSet transformedRecordSet = processRecord(record, rulesEngineService);
                            while ((currentRecord = transformedRecordSet.next()) != null) {
                                writer.write(currentRecord);
                            }
                        }

                        writeResult = writer.finishRecordSet();
                        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        attributes.putAll(writeResult.getAttributes());
                    }

                    action = session.putAllAttributes(action, attributes);
                    session.transfer(action, REL_ACTIONS);
                    session.getProvenanceReporter().modifyContent(action, "Generated Actions");
                    logger.debug("Transformed {}", new Object[]{original});
                }
            } catch (Exception e) {
                logger.error("Unable to write records {} due to {}", new Object[]{original, e.toString(), e});
                session.remove(action);
                session.transfer(original, REL_FAILURE);
                return;
            }

        } catch (final Exception ex) {
            logger.error("Unable to process {} due to {}", new Object[]{original, ex.toString(), ex});
            session.transfer(original, REL_FAILURE);
            return;
        }
        session.transfer(original, REL_ORIGINAL);
    }

    protected RecordSet processRecord(Record record, RulesEngineService rulesEngineService) {
        Set<String> fieldNames = record.getRawFieldNames();
        Map<String, Object> facts = new HashMap<>();
        fieldNames.forEach(name -> facts.put(name, record.getValue(name)));
        List<Action> actions = rulesEngineService.fireRules(facts);
        final List<Record> records = new ArrayList<>();

        if (actions != null) {
            actions.forEach(action -> {
                Map<String, Object> actionMap = new HashMap<>();
                actionMap.put("type", action.getType());
                actionMap.put("attributes", action.getAttributes());
                actionMap.put("metrics", facts.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));
                records.add(new MapRecord(recordSchema, actionMap));
            });
        }

        return new ListRecordSet(recordSchema, records);
    }

}
