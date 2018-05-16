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
package org.apache.nifi.controller.repository;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.provenance.ProvenanceReporter;

public class BatchingSessionFactory implements ProcessSessionFactory {

    private final HighThroughputSession highThroughputSession;

    public BatchingSessionFactory(final StandardProcessSession standardProcessSession) {
        highThroughputSession = new HighThroughputSession(standardProcessSession);
    }

    @Override
    public ProcessSession createSession() {
        return highThroughputSession;
    }


    private class HighThroughputSession implements ProcessSession {
        private final StandardProcessSession session;

        public HighThroughputSession(final StandardProcessSession session) {
            this.session = session;
        }

        @Override
        public void commit() {
            session.checkpoint();
        }

        @Override
        public void rollback() {
            session.rollback();
        }

        @Override
        public void rollback(boolean penalize) {
            session.rollback(penalize);
        }

        @Override
        public void migrate(ProcessSession newOwner, Collection<FlowFile> flowFiles) {
            session.migrate(newOwner, flowFiles);
        }

        @Override
        public void adjustCounter(String name, long delta, boolean immediate) {
            session.adjustCounter(name, delta, immediate);
        }

        @Override
        public FlowFile get() {
            return session.get();
        }

        @Override
        public List<FlowFile> get(int maxResults) {
            return session.get(maxResults);
        }

        @Override
        public List<FlowFile> get(FlowFileFilter filter) {
            return session.get(filter);
        }

        @Override
        public QueueSize getQueueSize() {
            return session.getQueueSize();
        }

        @Override
        public FlowFile create() {
            return session.create();
        }

        @Override
        public FlowFile create(FlowFile parent) {
            return session.create(parent);
        }

        @Override
        public FlowFile create(Collection<FlowFile> parents) {
            return session.create(parents);
        }

        @Override
        public FlowFile clone(FlowFile example) {
            return session.clone(example);
        }

        @Override
        public FlowFile clone(FlowFile example, long offset, long size) {
            return session.clone(example, offset, size);
        }

        @Override
        public FlowFile penalize(FlowFile flowFile) {
            return session.penalize(flowFile);
        }

        @Override
        public FlowFile putAttribute(FlowFile flowFile, String key, String value) {
            return session.putAttribute(flowFile, key, value);
        }

        @Override
        public FlowFile putAllAttributes(FlowFile flowFile, Map<String, String> attributes) {
            return session.putAllAttributes(flowFile, attributes);
        }

        @Override
        public FlowFile removeAttribute(FlowFile flowFile, String key) {
            return session.removeAttribute(flowFile, key);
        }

        @Override
        public FlowFile removeAllAttributes(FlowFile flowFile, Set<String> keys) {
            return session.removeAllAttributes(flowFile, keys);
        }

        @Override
        public FlowFile removeAllAttributes(FlowFile flowFile, Pattern keyPattern) {
            return session.removeAllAttributes(flowFile, keyPattern);
        }

        @Override
        public void transfer(FlowFile flowFile, Relationship relationship) {
            session.transfer(flowFile, relationship);
        }

        @Override
        public void transfer(FlowFile flowFile) {
            session.transfer(flowFile);
        }

        @Override
        public void transfer(Collection<FlowFile> flowFiles) {
            session.transfer(flowFiles);
        }

        @Override
        public void transfer(Collection<FlowFile> flowFiles, Relationship relationship) {
            session.transfer(flowFiles, relationship);
        }

        @Override
        public void remove(FlowFile flowFile) {
            session.remove(flowFile);
        }

        @Override
        public void remove(Collection<FlowFile> flowFiles) {
            session.remove(flowFiles);
        }

        @Override
        public void read(FlowFile source, InputStreamCallback reader) {
            session.read(source, reader);
        }

        @Override
        public void read(FlowFile source, boolean allowSessionStreamManagement, InputStreamCallback reader) {
            session.read(source, allowSessionStreamManagement, reader);
        }

        @Override
        public InputStream read(FlowFile flowFile) {
            return session.read(flowFile);
        }

        @Override
        public FlowFile merge(Collection<FlowFile> sources, FlowFile destination) {
            return session.merge(sources, destination);
        }

        @Override
        public FlowFile merge(Collection<FlowFile> sources, FlowFile destination, byte[] header, byte[] footer, byte[] demarcator) {
            return session.merge(sources, destination, header, footer, demarcator);
        }

        @Override
        public FlowFile write(FlowFile source, OutputStreamCallback writer) {
            return session.write(source, writer);
        }

        @Override
        public FlowFile write(FlowFile source, StreamCallback writer) {
            return session.write(source, writer);
        }

        @Override
        public FlowFile append(FlowFile source, OutputStreamCallback writer) {
            return session.append(source, writer);
        }

        @Override
        public FlowFile importFrom(Path source, boolean keepSourceFile, FlowFile destination) {
            return session.importFrom(source, keepSourceFile, destination);
        }

        @Override
        public FlowFile importFrom(InputStream source, FlowFile destination) {
            return session.importFrom(source, destination);
        }

        @Override
        public void exportTo(FlowFile flowFile, Path destination, boolean append) {
            session.exportTo(flowFile, destination, append);
        }

        @Override
        public void exportTo(FlowFile flowFile, OutputStream destination) {
            session.exportTo(flowFile, destination);
        }

        @Override
        public ProvenanceReporter getProvenanceReporter() {
            return session.getProvenanceReporter();
        }

        @Override
        public OutputStream write(final FlowFile source) {
            return session.write(source);
        }
    }
}
