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

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.metrics.CommitTiming;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class BatchingSessionFactory implements ProcessSessionFactory {
    private static final Logger logger = LoggerFactory.getLogger(BatchingSessionFactory.class);

    private final HighThroughputSession highThroughputSession;

    public BatchingSessionFactory(final StandardProcessSession standardProcessSession) {
        highThroughputSession = new HighThroughputSession(standardProcessSession);
    }

    @Override
    public ProcessSession createSession() {
        return highThroughputSession;
    }

    private static class HighThroughputSession implements ProcessSession {
        private final StandardProcessSession session;

        public HighThroughputSession(final StandardProcessSession session) {
            this.session = session;
        }

        @Override
        public void commit() {
            session.checkpoint();
        }

        @Override
        public void commitAsync() {
            commit();
        }

        @Override
        public void commitAsync(final Runnable onSuccess, final Consumer<Throwable> onFailure) {
            try {
                commit();
            } catch (final Throwable t) {
                rollback();
                logger.error("Failed to asynchronously commit session", t);
                onFailure.accept(t);
            }

            try {
                onSuccess.run();
            } catch (final Throwable t) {
                logger.error("Successfully committed session asynchronously but failed to trigger success callback", t);
            }
        }

        @Override
        public void rollback() {
            session.rollback();
        }

        @Override
        public void rollback(final boolean penalize) {
            session.rollback(penalize);
        }

        @Override
        public void migrate(final ProcessSession newOwner, final Collection<FlowFile> flowFiles) {
            session.migrate(newOwner, flowFiles);
        }

        @Override
        public void migrate(final ProcessSession newOwner) {
            session.migrate(newOwner);
        }

        @Override
        public void adjustCounter(final String name, final long delta, final boolean immediate) {
            session.adjustCounter(name, delta, immediate);
        }

        @Override
        public void recordGauge(final String name, final double value, final CommitTiming commitTiming) {
            session.recordGauge(name, value, commitTiming);
        }

        @Override
        public FlowFile get() {
            return session.get();
        }

        @Override
        public List<FlowFile> get(final int maxResults) {
            return session.get(maxResults);
        }

        @Override
        public List<FlowFile> get(final FlowFileFilter filter) {
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
        public FlowFile create(final FlowFile parent) {
            return session.create(parent);
        }

        @Override
        public FlowFile create(final Collection<FlowFile> parents) {
            return session.create(parents);
        }

        @Override
        public FlowFile clone(final FlowFile example) {
            return session.clone(example);
        }

        @Override
        public FlowFile clone(final FlowFile example, final long offset, final long size) {
            return session.clone(example, offset, size);
        }

        @Override
        public FlowFile penalize(final FlowFile flowFile) {
            return session.penalize(flowFile);
        }

        @Override
        public FlowFile putAttribute(final FlowFile flowFile, final String key, final String value) {
            return session.putAttribute(flowFile, key, value);
        }

        @Override
        public FlowFile putAllAttributes(final FlowFile flowFile, final Map<String, String> attributes) {
            return session.putAllAttributes(flowFile, attributes);
        }

        @Override
        public FlowFile removeAttribute(final FlowFile flowFile, final String key) {
            return session.removeAttribute(flowFile, key);
        }

        @Override
        public FlowFile removeAllAttributes(final FlowFile flowFile, final Set<String> keys) {
            return session.removeAllAttributes(flowFile, keys);
        }

        @Override
        public FlowFile removeAllAttributes(final FlowFile flowFile, final Pattern keyPattern) {
            return session.removeAllAttributes(flowFile, keyPattern);
        }

        @Override
        public void transfer(final FlowFile flowFile, final Relationship relationship) {
            session.transfer(flowFile, relationship);
        }

        @Override
        public void transfer(final FlowFile flowFile) {
            session.transfer(flowFile);
        }

        @Override
        public void transfer(final Collection<FlowFile> flowFiles) {
            session.transfer(flowFiles);
        }

        @Override
        public void transfer(final Collection<FlowFile> flowFiles, final Relationship relationship) {
            session.transfer(flowFiles, relationship);
        }

        @Override
        public void remove(final FlowFile flowFile) {
            session.remove(flowFile);
        }

        @Override
        public void remove(final Collection<FlowFile> flowFiles) {
            session.remove(flowFiles);
        }

        @Override
        public void read(final FlowFile source, final InputStreamCallback reader) {
            session.read(source, reader);
        }

        @Override
        public InputStream read(final FlowFile flowFile) {
            return session.read(flowFile);
        }

        @Override
        public FlowFile merge(final Collection<FlowFile> sources, final FlowFile destination) {
            return session.merge(sources, destination);
        }

        @Override
        public FlowFile merge(final Collection<FlowFile> sources, final FlowFile destination, final byte[] header, final byte[] footer, final byte[] demarcator) {
            return session.merge(sources, destination, header, footer, demarcator);
        }

        @Override
        public FlowFile write(final FlowFile source, final OutputStreamCallback writer) {
            return session.write(source, writer);
        }

        @Override
        public FlowFile write(final FlowFile source, final StreamCallback writer) {
            return session.write(source, writer);
        }

        @Override
        public FlowFile append(final FlowFile source, final OutputStreamCallback writer) {
            return session.append(source, writer);
        }

        @Override
        public FlowFile importFrom(final Path source, final boolean keepSourceFile, final FlowFile destination) {
            return session.importFrom(source, keepSourceFile, destination);
        }

        @Override
        public FlowFile importFrom(final InputStream source, final FlowFile destination) {
            return session.importFrom(source, destination);
        }

        @Override
        public void exportTo(final FlowFile flowFile, final Path destination, final boolean append) {
            session.exportTo(flowFile, destination, append);
        }

        @Override
        public void exportTo(final FlowFile flowFile, final OutputStream destination) {
            session.exportTo(flowFile, destination);
        }

        @Override
        public ProvenanceReporter getProvenanceReporter() {
            return session.getProvenanceReporter();
        }

        @Override
        public void setState(final Map<String, String> state, final Scope scope) throws IOException {
            session.setState(state, scope);
        }

        @Override
        public StateMap getState(final Scope scope) throws IOException {
            return session.getState(scope);
        }

        @Override
        public boolean replaceState(final StateMap oldValue, final Map<String, String> newValue, final Scope scope) throws IOException {
            return session.replaceState(oldValue, newValue, scope);
        }

        @Override
        public void clearState(final Scope scope) {
            session.clearState(scope);
        }

        @Override
        public OutputStream write(final FlowFile source) {
            return session.write(source);
        }
    }
}
