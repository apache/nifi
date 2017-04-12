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
package org.apache.nifi.processor.util.pattern;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.Objects;

/**
 * Extended Put pattern capable of handling FlowFile groups.
 * @param <FC> Function context class.
 * @param <C> Connection class.
 * @param <FFG> FlowFileGroup class.
 */
public class PutGroup<FC, C extends AutoCloseable, FFG extends PartialFunctions.FlowFileGroup> extends Put<FC, C> {


    public PutGroup() {
        // Just to make a composition valid.
        this.putFlowFile = (context, session, functionContext, connection, inputFlowFile, result) -> {
            throw new UnsupportedOperationException();
        };
    }

    @FunctionalInterface
    public interface PutFlowFiles<FC, C, FFG> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, C connection,
                            FFG inputFlowFileGroup, RoutingResult result) throws ProcessException;
    }

    @Override
    protected void validateCompositePattern() {
        super.validateCompositePattern();
        Objects.requireNonNull(groupFlowFiles, "GroupFlowFiles function is required.");
    }

    /**
     * PutGroup does not support PutFileFile function for single FlowFile.
     * Throws UnsupportedOperationException if called.
     */
    @Override
    public void putFlowFile(PutFlowFile<FC, C> putFlowFile) {
        throw new UnsupportedOperationException("PutFlowFile can not be used with PutGroup pattern. Specify PutFlowFiles instead.");
    }

    @FunctionalInterface
    public interface GroupFlowFiles<FC, C, FFG> {
        List<FFG> apply(ProcessContext context, ProcessSession session, FC functionContext, C connection, List<FlowFile> flowFiles, RoutingResult result) throws ProcessException;
    }

    private GroupFlowFiles<FC, C, FFG> groupFlowFiles;
    private PutFlowFiles<FC, C, FFG> putFlowFiles;

    /**
     * Specify a function that groups input FlowFiles into FlowFile groups.
     */
    public void groupFetchedFlowFiles(GroupFlowFiles<FC, C, FFG> f) {
        groupFlowFiles = f;
    }

    /**
     * Specify a function that puts an input FlowFile group to a target storage using a given connection.
     */
    public void putFlowFiles(PutFlowFiles<FC, C, FFG> f) {
        putFlowFiles = f;
    }


    @Override
    protected void putFlowFiles(ProcessContext context, ProcessSession session, FC functionContext,
                               C connection, List<FlowFile> flowFiles, RoutingResult result) throws ProcessException {
        final List<FFG> flowFileGroups = groupFlowFiles
                .apply(context, session, functionContext, connection, flowFiles, result);

        for (FFG group : flowFileGroups) {
            putFlowFiles.apply(context, session, functionContext, connection, group, result);
        }
    }
}
