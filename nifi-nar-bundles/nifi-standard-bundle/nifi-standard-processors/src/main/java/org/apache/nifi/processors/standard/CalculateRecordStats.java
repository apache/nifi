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

import com.google.common.collect.Lists;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Tags({ "record", "stats", "metrics" })
@CapabilityDescription("A processor that can count the number of items in a record set, as well as provide counts based on " +
        "user-defined criteria on subsets of the record set.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
    @WritesAttribute(attribute = CalculateRecordStats.RECORD_COUNT_ATTR, description = "A count of the records in the record set in the flowfile."),
    @WritesAttribute(attribute = "recordStats.<User Defined Property Name>.count", description = "A count of the records that contain a value for the user defined property."),
    @WritesAttribute(attribute = "recordStats.<User Defined Property Name>.<value>.count",
            description = "Each value discovered for the user defined property will have its own count attribute. " +
                    "Total number of top N value counts to be added is defined by the limit configuration.")
})
public class CalculateRecordStats extends AbstractProcessor {
    static final String RECORD_COUNT_ATTR = "record.count";

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-stats-reader")
        .displayName("Record Reader")
        .description("A record reader to use for reading the records.")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    static final PropertyDescriptor LIMIT = new PropertyDescriptor.Builder()
        .name("record-stats-limit")
        .description("Limit the number of individual stats that are returned for each record path to the top N results.")
        .required(true)
        .defaultValue("10")
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("If a flowfile is successfully processed, it goes here.")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If a flowfile fails to be processed, it goes here.")
        .build();

    private RecordPathCache cache;

    static final Set RELATIONSHIPS;
    static final List<PropertyDescriptor> PROPERTIES;

    static {
        Set _rels = new HashSet();
        _rels.add(REL_SUCCESS);
        _rels.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(_rels);
        List<PropertyDescriptor> _temp = new ArrayList<>();
        _temp.add(RECORD_READER);
        _temp.add(LIMIT);
        PROPERTIES = Collections.unmodifiableList(_temp);
    }

    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .displayName(propertyDescriptorName)
            .dynamic(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    }

    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @OnScheduled
    public void onEnabled(ProcessContext context) {
        cache = new RecordPathCache(25);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        try {
            Map<String, RecordPath> paths = getRecordPaths(context, input);
            Map<String, String> stats = getStats(input, paths, context, session);

            input = session.putAllAttributes(input, stats);

            session.transfer(input, REL_SUCCESS);

        } catch (Exception ex) {
            getLogger().error("Error processing stats.", ex);
            session.transfer(input, REL_FAILURE);
        }

    }

    protected Map<String, RecordPath> getRecordPaths(ProcessContext context, FlowFile flowFile) {
        return context.getProperties().keySet()
            .stream().filter(p -> p.isDynamic())
            .collect(Collectors.toMap(
                e -> e.getName(),
                e ->  {
                    String val = context.getProperty(e).evaluateAttributeExpressions(flowFile).getValue();
                    return cache.getCompiled(val);
                })
            );
    }

    protected Map<String, String> getStats(FlowFile flowFile, Map<String, RecordPath> paths, ProcessContext context, ProcessSession session) {
        try (InputStream is = session.read(flowFile)) {
            RecordReaderFactory factory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            final Integer limit = context.getProperty(LIMIT).evaluateAttributeExpressions(flowFile).asInteger();
            RecordReader reader = factory.createRecordReader(flowFile, is, getLogger());

            Map<String, Integer> retVal = new HashMap<>();
            Record record;

            int recordCount = 0;
            List<String> baseKeys = new ArrayList<>();
            while ((record = reader.nextRecord()) != null) {
                for (Map.Entry<String, RecordPath> entry : paths.entrySet()) {
                    RecordPathResult result = entry.getValue().evaluate(record);
                    Optional<FieldValue> value = result.getSelectedFields().findFirst();
                    if (value.isPresent() && value.get().getValue() != null) {
                        String approxValue = value.get().getValue().toString();
                        String baseKey = String.format("recordStats.%s", entry.getKey());
                        String key = String.format("%s.%s", baseKey, approxValue);
                        Integer stat = retVal.containsKey(key) ? retVal.get(key) : 0;
                        Integer baseStat = retVal.getOrDefault(baseKey, 0);
                        stat++;
                        baseStat++;

                        retVal.put(key, stat);
                        retVal.put(baseKey, baseStat);

                        if (!baseKeys.contains(baseKey)) {
                            baseKeys.add(baseKey);
                        }
                    }
                }

                recordCount++;
            }

            retVal = filterBySize(retVal, limit, baseKeys);

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

    protected Map filterBySize(Map<String, Integer> values, Integer limit, List<String> baseKeys) {
        Map<String, Integer> toFilter = values.entrySet().stream()
            .filter(e -> !baseKeys.contains(e.getKey()))
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        Map<String, Integer> retVal = values.entrySet().stream()
            .filter((e -> baseKeys.contains(e.getKey())))
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        List<Map.Entry<String, Integer>> _flat = new ArrayList<>(toFilter.entrySet());
        _flat.sort(Map.Entry.comparingByValue());
        _flat = Lists.reverse(_flat);
        for (int index = 0; index < _flat.size() && index < limit; index++) {
            retVal.put(_flat.get(index).getKey(), _flat.get(index).getValue());
        }

        return retVal;
    }
}
