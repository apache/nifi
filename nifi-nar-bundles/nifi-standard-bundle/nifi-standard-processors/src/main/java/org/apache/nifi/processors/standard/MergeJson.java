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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"json", "merge"})
@CapabilityDescription("This process provides schema-less, free form merging of JSON input. This processor is schema-less " +
        "and should be used primarily for simple operations involving merging blocks of JSON together across multiple flowfiles.")
public class MergeJson extends AbstractProcessor {
    public static final ObjectMapper MAPPER = new ObjectMapper();

    public static final PropertyDescriptor MAX_SIZE = new PropertyDescriptor.Builder()
            .name("merge-json-max-size")
            .displayName("Max Output Size")
            .description("This controls the rough estimate for the maximum output size.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("500 MB")
            .build();
    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("merge-json-timeout")
            .displayName("Processing Timeout")
            .description("How long the processing session should wait for new inputs to reach its maximum output size " +
                    "before writing the output.")
            .required(true)
            .defaultValue("5 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor MERGE_TOP_LEVEL_ARRAYS = new PropertyDescriptor.Builder()
            .name("merge-json-merge-top-level-arrays")
            .displayName("Merge Top Level Arrays")
            .description("When the input JSON is an array, merge the elements of the input into the output array. If " +
                    "disabled, this option will cause arrays to be merged as array elements of the parent output array.")
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("merged")
            .description("When merging is successful, merged content is routed to this relationship.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failed")
            .description("When merging fails, all input flowfiles that failed to be merged are sent to this relationship.")
            .build();
    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("When merging is successful, all input flowfiles are sent to this relationship.")
            .build();

    public static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            MAX_SIZE, TIMEOUT, MERGE_TOP_LEVEL_ARRAYS
    ));

    public static final Set<Relationship> RELATIONSHIP_SET = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            REL_SUCCESS, REL_FAILURE, REL_ORIGINAL
    )));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIP_SET;
    }

    private long maxSize;
    private long timeoutMillis;
    private boolean mergeTopLevelArrays;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        maxSize = context.getProperty(MAX_SIZE).asDataSize(DataUnit.B).longValue();
        timeoutMillis = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
        mergeTopLevelArrays = context.getProperty(MERGE_TOP_LEVEL_ARRAYS).asBoolean();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        long processed = 0l;
        long expiryTime = System.currentTimeMillis() + timeoutMillis;
        List parsedObjects = new ArrayList();
        List<FlowFile> inputFlowFiles = new ArrayList<>();
        boolean error = false;

        while (input != null && processed < maxSize && System.currentTimeMillis() < expiryTime && !error) {
            try {
                inputFlowFiles.add(input);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                session.exportTo(input, out);
                out.close();

                byte[] array = out.toByteArray();
                processed += array.length;

                String asString = new String(array);
                Object parsed = parse(asString);

                if (parsed instanceof List && mergeTopLevelArrays) {
                    parsedObjects.addAll((List)parsed);
                } else {
                    parsedObjects.add(parsed);
                }

                if (processed < maxSize && System.currentTimeMillis() < expiryTime && !error) {
                    input = session.get();
                }
            } catch (IOException e) {
                error = true;
                getLogger().error("", e);
            }
        }

        if (error) {
            inputFlowFiles.forEach(ff -> session.transfer(ff, REL_FAILURE));
        } else {
            FlowFile output = session.create();
            output = session.write(output, (os) -> os.write(MAPPER.writeValueAsBytes(parsedObjects)));
            output = session.putAttribute(output, "record.count", String.valueOf(parsedObjects.size()));
            session.transfer(output, REL_SUCCESS);
            session.transfer(inputFlowFiles, REL_ORIGINAL);
        }
    }

    private Object parse(String str) throws JsonProcessingException {
        if (str.startsWith("[")) {
            return MAPPER.readValue(str, List.class);
        } else {
            return MAPPER.readValue(str, Map.class);
        }
    }
}
