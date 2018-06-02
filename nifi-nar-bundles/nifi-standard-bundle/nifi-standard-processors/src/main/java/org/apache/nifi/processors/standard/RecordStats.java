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

package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;

import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Tags({ "record", "stats", "metrics" })
@CapabilityDescription("A processor that can count the number of items in a record set, as well as provide counts based on " +
        "user-defined criteria on subsets of the record set.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = RecordStats.RECORD_COUNT_ATTR, description = "A count of the records in the record set in the flowfile.")
})
public class RecordStats extends AbstractProcessor {
    static final String RECORD_COUNT_ATTR = "record_count";

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-stats-reader")
        .displayName("Record Reader")
        .description("A record reader to use for reading the records.")
        .addValidator(Validator.VALID)
        .identifiesControllerService(RecordReaderFactory.class)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("If a flowfile is successfully processed, it goes here.")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If a flowfile fails to be processed, it goes here.")
        .build();

    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .displayName(propertyDescriptorName)
            .dynamic(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    }

    private RecordPathCache cache;

    @OnScheduled
    public void onEnabled(ProcessContext context) {
        cache = new RecordPathCache(25);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return new HashSet<Relationship>() {{
            add(REL_SUCCESS);
            add(REL_FAILURE);
        }};
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        try {
            Map<String, RecordPath> paths = getRecordPaths(context);
            Map<String, String> stats = getStats(input, paths, context, session);

            input = session.putAllAttributes(input, stats);

            session.transfer(input, REL_SUCCESS);

        } catch (Exception ex) {
            getLogger().error("Error processing stats.", ex);
            session.transfer(input, REL_FAILURE);
        }

    }

    protected Map<String, RecordPath> getRecordPaths(ProcessContext context) {
        return context.getProperties().keySet()
            .stream().filter(p -> p.isDynamic() && !p.getName().contains(RECORD_READER.getName()))
            .collect(Collectors.toMap(
                e -> e.getName(),
                e ->  {
                    String val = context.getProperty(e).getValue();
                    return cache.getCompiled(val);
                })
            );
    }

    protected Map<String, String> getStats(FlowFile flowFile, Map<String, RecordPath> paths, ProcessContext context, ProcessSession session) {
        try (InputStream is = session.read(flowFile)) {
            RecordReaderFactory factory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            RecordReader reader = factory.createRecordReader(flowFile, is, getLogger());

            Map<String, Integer> retVal = new HashMap<>();
            Record record;

            int recordCount = 0;
            while ((record = reader.nextRecord()) != null) {
                for (Map.Entry<String, RecordPath> entry : paths.entrySet()) {
                    RecordPathResult result = entry.getValue().evaluate(record);
                    Optional<FieldValue> value = result.getSelectedFields().findFirst();
                    if (value.isPresent() && value.get().getValue() != null) {
                        String approxValue = value.get().getValue().toString();
                        String key = String.format("%s.%s", entry.getKey(), approxValue);
                        Integer stat = retVal.containsKey(key) ? retVal.get(key) : 0;
                        Integer baseStat = retVal.containsKey(entry.getKey()) ? retVal.get(entry.getKey()) : 0;
                        stat++;
                        baseStat++;

                        retVal.put(key, stat);
                        retVal.put(entry.getKey(), baseStat);
                    }
                }

                recordCount++;
            }
            retVal.put(RECORD_COUNT_ATTR, recordCount);

            return retVal.entrySet().stream()
                .collect(Collectors.toMap(
                    e -> e.getKey(),
                    e -> e.getValue().toString()
                ));
        } catch (Exception e) {
            getLogger().error("Could not read flowfile", e);
            throw new ProcessException(e);
        }
    }
}
