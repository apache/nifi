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
package org.apache.nifi.processors.tests.system;

import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Random;
import java.util.Set;

@DefaultSchedule(period = "10 mins")
public class GenerateTruncatableFlowFiles extends AbstractProcessor {

    static final PropertyDescriptor BATCH_COUNT = new PropertyDescriptor.Builder()
        .name("Batch Count")
        .description("The maximum number of batches to generate. Each batch produces 10 FlowFiles (9 small + 1 large). "
                     + "Once this many batches have been generated, no more FlowFiles will be produced until the processor is stopped and restarted.")
        .required(true)
        .defaultValue("10")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    static final PropertyDescriptor SMALL_FILE_SIZE = new PropertyDescriptor.Builder()
        .name("Small File Size")
        .description("Size of each small FlowFile in bytes")
        .required(true)
        .defaultValue("1 KB")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .build();

    static final PropertyDescriptor LARGE_FILE_SIZE = new PropertyDescriptor.Builder()
        .name("Large File Size")
        .description("Size of each large FlowFile in bytes")
        .required(true)
        .defaultValue("10 MB")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .build();

    static final PropertyDescriptor SMALL_FILES_PER_BATCH = new PropertyDescriptor.Builder()
        .name("Small Files Per Batch")
        .description("Number of small FlowFiles to generate per batch")
        .required(true)
        .defaultValue("9")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(BATCH_COUNT,
            SMALL_FILE_SIZE,
            LARGE_FILE_SIZE,
            SMALL_FILES_PER_BATCH);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS);
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final int batchCount = context.getProperty(BATCH_COUNT).asInteger();
        final Random random = new Random();
        final int smallFileSize = context.getProperty(SMALL_FILE_SIZE).asDataSize(DataUnit.B).intValue();
        final int largeFileSize = context.getProperty(LARGE_FILE_SIZE).asDataSize(DataUnit.B).intValue();
        final int smallFilesPerBatch = context.getProperty(SMALL_FILES_PER_BATCH).asInteger();

        for (int batch = 0; batch < batchCount; batch++) {
            // Generate small FlowFiles with priority = 10 (low priority, processed last by PriorityAttributePrioritizer)
            for (int i = 0; i < smallFilesPerBatch; i++) {
                createFlowFile(session, random, smallFileSize, "10");
            }

            // Generate one large FlowFile with priority = 1 (high priority, processed first by PriorityAttributePrioritizer)
            createFlowFile(session, random, largeFileSize, "1");
        }
    }

    private void createFlowFile(final ProcessSession session, final Random random, final int fileSize, final String priority) {
        FlowFile flowFile = session.create();
        flowFile = session.putAttribute(flowFile, "priority", priority);
        final byte[] data = new byte[fileSize];
        random.nextBytes(data);
        flowFile = session.write(flowFile, out -> out.write(data));
        session.transfer(flowFile, REL_SUCCESS);
    }
}
