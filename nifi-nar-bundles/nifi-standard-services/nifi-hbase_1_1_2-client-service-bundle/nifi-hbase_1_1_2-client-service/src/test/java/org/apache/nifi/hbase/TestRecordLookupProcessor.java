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

package org.apache.nifi.hbase;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TestRecordLookupProcessor extends AbstractProcessor {

    static final PropertyDescriptor HBASE_LOOKUP_SERVICE = new PropertyDescriptor.Builder()
            .name("HBase Lookup Service")
            .description("HBaseLookupService")
            .identifiesControllerService(LookupService.class)
            .required(true)
            .build();

    static final PropertyDescriptor HBASE_ROW = new PropertyDescriptor.Builder()
            .name("HBase Row Id")
            .description("The Row Id to Lookup.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All success FlowFiles are routed to this relationship")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All failed FlowFiles are routed to this relationship")
            .build();

    private List<Record> lookedupRecords = new ArrayList<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> propDescs = new ArrayList<>();
        propDescs.add(HBASE_LOOKUP_SERVICE);
        propDescs.add(HBASE_ROW);
        return propDescs;
    }

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String rowKey = context.getProperty(HBASE_ROW).getValue();

        final Map<String,Object> coordinates = new HashMap<>();
        coordinates.put(HBase_1_1_2_RecordLookupService.ROW_KEY_KEY, rowKey);

        final LookupService<Record> lookupService = context.getProperty(HBASE_LOOKUP_SERVICE).asControllerService(LookupService.class);
        try {
            final Optional<Record> record = lookupService.lookup(coordinates);
            if (record.isPresent()) {
                lookedupRecords.add(record.get());
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                session.transfer(flowFile, REL_FAILURE);
            }

        } catch (LookupFailureException e) {
            session.transfer(flowFile, REL_FAILURE);
        }

    }

    public List<Record> getLookedupRecords() {
        return new ArrayList<>(lookedupRecords);
    }

    public void clearLookedupRecords() {
        this.lookedupRecords.clear();
    }

}
