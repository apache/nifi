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

package org.apache.nifi.processors.standard.sql;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processors.standard.AbstractExecuteSQL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.singletonMap;

public class ResultSetFragments {

    public final String fragmentId = UUID.randomUUID().toString();
    private final List<FlowFile> flowFiles = new ArrayList<>();
    private final ProcessSession session;
    private final ExecuteSQLConfiguration config;

    private FlowFile inputFlowFile;

    private final Map<String, String> inputFileAttrMap;
    private final ProcessSessionFactory sessionFactory;

    private long fragmentIndex = 0;

    public ResultSetFragments(ProcessSession session, ExecuteSQLConfiguration config, FlowFile inputFlowFile, ProcessSessionFactory sessionFactory) {
        this.session = session;
        this.config = config;
        this.inputFlowFile = inputFlowFile;
        this.inputFileAttrMap = inputFlowFile == null ? null : inputFlowFile.getAttributes();
        this.sessionFactory = sessionFactory;
    }

    public FlowFile createNewFlowFile() {
        FlowFile resultSetFF;
        if (inputFlowFile == null) {
            resultSetFF = session.create();
        } else {
            resultSetFF = session.create(inputFlowFile);
        }
        return resultSetFF;
    }

    public void add(FlowFile flowFile) {
        if (config.isMaxRowsPerFlowFileSet()) {
            // if fragmented ResultSet, set fragment attributes
            final Map<String, String> attributesToAdd = new HashMap<>();
            attributesToAdd.put(AbstractExecuteSQL.FRAGMENT_ID, fragmentId);
            attributesToAdd.put(AbstractExecuteSQL.FRAGMENT_INDEX, String.valueOf(fragmentIndex++));
            flowFile = session.putAllAttributes(flowFile, attributesToAdd);
        }

        flowFiles.addLast(flowFile);

        // If we've reached the batch size, send out the flow files
        if (config.isOutputBatchSizeSet() && flowFiles.size() > config.getOutputBatchSize()) {
            final FlowFile lastFlowFile = flowFiles.removeLast();
            session.transfer(flowFiles, AbstractExecuteSQL.REL_SUCCESS);
            // Need to remove the original input file if it exists
            if (inputFlowFile != null) {
                session.remove(inputFlowFile);
                inputFlowFile = null;
            }
            flowFiles.clear();
            flowFiles.add(lastFlowFile);

            // Migrate the single remaining file to a new session, commit the main session, then migrate back.
            // This is required, because otherwise NiFi will complain about the remaining flow file, that was created
            // in the main session, but not transferred, neither removed. --> Error.
            final ProcessSession tempSession = sessionFactory.createSession();
            try {
                session.migrate(tempSession, flowFiles);
                session.commitAsync();
                tempSession.migrate(session, flowFiles);
                tempSession.commitAsync();
            } catch (final Throwable throwable) {
                tempSession.rollback(true);
                throw throwable;
            }
        }
    }

    public void removeAll() {
        session.remove(flowFiles);
        flowFiles.clear();
    }

    public void transferAllToSuccess() {
        if (flowFiles.isEmpty()) {
            return;
        }
        // If we are splitting results but not outputting batches, set count on all FlowFiles
        if (!config.isOutputBatchSizeSet() && config.isMaxRowsPerFlowFileSet()) {
            putAttributesToAll(singletonMap(AbstractExecuteSQL.FRAGMENT_COUNT, Long.toString(fragmentIndex)));
        }
        flowFiles.addLast(session.putAttribute(flowFiles.removeLast(), AbstractExecuteSQL.END_OF_RESULTSET_FLAG, "true"));
        session.transfer(flowFiles, AbstractExecuteSQL.REL_SUCCESS);
        flowFiles.clear();
    }

    public void putAttributesToAll(Map<String, String> attributes) {
        flowFiles.replaceAll(flowFile -> session.putAllAttributes(flowFile, attributes));
    }

    public FlowFile getInputFlowFile() {
        return inputFlowFile;
    }

    public Map<String, String> getInputFileAttributeMap() {
        return inputFileAttrMap;
    }

    public String getInputFileUUID() {
        return inputFileAttrMap == null ? null : inputFileAttrMap.get(CoreAttributes.UUID.key());
    }

    public boolean isFirst() {
        return fragmentIndex == 0;
    }
}
