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

import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.bin.Bin;
import org.apache.nifi.processor.util.bin.BinFiles;
import org.apache.nifi.processor.util.bin.BinManager;
import org.apache.nifi.processor.util.bin.BinProcessingResult;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@TriggerSerially
@TriggerWhenEmpty
public class ConcatenateRangeOfFlowFiles extends BinFiles {
    public static final Relationship REL_MERGED = new Relationship.Builder()
        .name("merged")
        .description("The FlowFile containing the merged content")
        .build();

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_FAILURE);
        relationships.add(REL_MERGED);
        return relationships;
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(MIN_ENTRIES);
        descriptors.add(MAX_ENTRIES);
        descriptors.add(MIN_SIZE);
        descriptors.add(MAX_SIZE);
        descriptors.add(MAX_BIN_AGE);
        descriptors.add(MAX_BIN_COUNT);
        return descriptors;
    }

    @Override
    protected FlowFile preprocessFlowFile(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        return flowFile;
    }

    @Override
    protected String getGroupId(final ProcessContext context, final FlowFile flowFile, final ProcessSession session) {
        return null;
    }

    @Override
    protected void setUpBinManager(final BinManager binManager, final ProcessContext context) {

    }

    @Override
    protected BinProcessingResult processBin(final Bin bin, final ProcessContext context) throws ProcessException {
        final ProcessSession session = bin.getSession();
        final List<FlowFile> flowFiles = bin.getContents();
        FlowFile merged = session.create(flowFiles);
        merged = session.merge(flowFiles, merged);
        session.transfer(merged, REL_MERGED);

        getLogger().info("Concatenated {} FlowFiles into {}", flowFiles.size(), merged);

        final BinProcessingResult binProcessingResult = new BinProcessingResult(true);
        binProcessingResult.setCommitted(false);
        return binProcessingResult;
    }
}
