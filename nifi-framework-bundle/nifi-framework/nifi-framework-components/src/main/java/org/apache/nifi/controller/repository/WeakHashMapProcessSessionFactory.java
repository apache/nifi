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
import org.apache.nifi.processor.exception.TerminatedTaskException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.metrics.CommitTiming;
import org.apache.nifi.provenance.ProvenanceReporter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class WeakHashMapProcessSessionFactory implements ActiveProcessSessionFactory {
    private final ProcessSessionFactory delegate;
    private final Map<ProcessSession, Boolean> sessionMap = new WeakHashMap<>();
    private boolean terminated = false;

    public WeakHashMapProcessSessionFactory(final ProcessSessionFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public synchronized ProcessSession createSession() {
        if (terminated) {
            throw new TerminatedTaskException();
        }

        final ProcessSession delegateSession = delegate.createSession();

        // Wrap the delegate Session so that the returned Session strongly references this factory. The
        // LifecycleState tracks ActiveProcessSessionFactory instances in a WeakHashMap, so without a
        // back-reference from the Session a Processor that retains the Session but not the factory across
        // onTrigger invocations would let the WeakHashMap entry clear, leaving terminate() unable to roll
        // back the orphaned Session and preventing offload from completing.
        final ProcessSession wrapper = new FactoryRetainingProcessSession(delegateSession, this);
        sessionMap.put(wrapper, Boolean.TRUE);
        return wrapper;
    }

    @Override
    public synchronized void terminateActiveSessions() {
        terminated = true;
        for (final ProcessSession session : sessionMap.keySet()) {
            try {
                session.rollback();
            } catch (final TerminatedTaskException ignored) {
            }
        }

        sessionMap.clear();
    }

    /**
     * A delegating {@link ProcessSession} that strongly references the factory that produced it. This
     * back-reference exists solely to keep the factory reachable so that the {@code LifecycleState}
     * WeakHashMap entry tracking the factory is not cleared while the Session is still in use. All
     * operations are forwarded to the underlying delegate Session.
     */
    private static final class FactoryRetainingProcessSession implements DelegatingProcessSession {
        private final ProcessSession delegate;
        @SuppressWarnings("unused") // Retained only to keep the factory reachable; see class Javadoc.
        private final WeakHashMapProcessSessionFactory factoryRetention;

        private FactoryRetainingProcessSession(final ProcessSession delegate, final WeakHashMapProcessSessionFactory factoryRetention) {
            this.delegate = delegate;
            this.factoryRetention = factoryRetention;
        }

        @Override
        public ProcessSession getDelegate() {
            return delegate;
        }

        @Override
        public void commit() {
            delegate.commit();
        }

        @Override
        public void commitAsync() {
            delegate.commitAsync();
        }

        @Override
        public void commitAsync(final Runnable onSuccess, final Consumer<Throwable> onFailure) {
            delegate.commitAsync(onSuccess, onFailure);
        }

        @Override
        public void rollback() {
            delegate.rollback();
        }

        @Override
        public void rollback(final boolean penalize) {
            delegate.rollback(penalize);
        }

        @Override
        public void migrate(final ProcessSession newOwner, final Collection<FlowFile> flowFiles) {
            delegate.migrate(newOwner, flowFiles);
        }

        @Override
        public void migrate(final ProcessSession newOwner) {
            delegate.migrate(newOwner);
        }

        @Override
        public void adjustCounter(final String name, final long delta, final boolean immediate) {
            delegate.adjustCounter(name, delta, immediate);
        }

        @Override
        public void recordGauge(final String name, final double value, final CommitTiming commitTiming) {
            delegate.recordGauge(name, value, commitTiming);
        }

        @Override
        public FlowFile get() {
            return delegate.get();
        }

        @Override
        public List<FlowFile> get(final int maxResults) {
            return delegate.get(maxResults);
        }

        @Override
        public List<FlowFile> get(final FlowFileFilter filter) {
            return delegate.get(filter);
        }

        @Override
        public QueueSize getQueueSize() {
            return delegate.getQueueSize();
        }

        @Override
        public FlowFile create() {
            return delegate.create();
        }

        @Override
        public FlowFile create(final FlowFile parent) {
            return delegate.create(parent);
        }

        @Override
        public FlowFile create(final Collection<FlowFile> parents) {
            return delegate.create(parents);
        }

        @Override
        public FlowFile clone(final FlowFile example) {
            return delegate.clone(example);
        }

        @Override
        public FlowFile clone(final FlowFile example, final long offset, final long size) {
            return delegate.clone(example, offset, size);
        }

        @Override
        public FlowFile penalize(final FlowFile flowFile) {
            return delegate.penalize(flowFile);
        }

        @Override
        public FlowFile putAttribute(final FlowFile flowFile, final String key, final String value) {
            return delegate.putAttribute(flowFile, key, value);
        }

        @Override
        public FlowFile putAllAttributes(final FlowFile flowFile, final Map<String, String> attributes) {
            return delegate.putAllAttributes(flowFile, attributes);
        }

        @Override
        public FlowFile removeAttribute(final FlowFile flowFile, final String key) {
            return delegate.removeAttribute(flowFile, key);
        }

        @Override
        public FlowFile removeAllAttributes(final FlowFile flowFile, final Set<String> keys) {
            return delegate.removeAllAttributes(flowFile, keys);
        }

        @Override
        public FlowFile removeAllAttributes(final FlowFile flowFile, final Pattern keyPattern) {
            return delegate.removeAllAttributes(flowFile, keyPattern);
        }

        @Override
        public void transfer(final FlowFile flowFile, final Relationship relationship) {
            delegate.transfer(flowFile, relationship);
        }

        @Override
        public void transfer(final FlowFile flowFile) {
            delegate.transfer(flowFile);
        }

        @Override
        public void transfer(final Collection<FlowFile> flowFiles) {
            delegate.transfer(flowFiles);
        }

        @Override
        public void transfer(final Collection<FlowFile> flowFiles, final Relationship relationship) {
            delegate.transfer(flowFiles, relationship);
        }

        @Override
        public void remove(final FlowFile flowFile) {
            delegate.remove(flowFile);
        }

        @Override
        public void remove(final Collection<FlowFile> flowFiles) {
            delegate.remove(flowFiles);
        }

        @Override
        public void read(final FlowFile source, final InputStreamCallback reader) {
            delegate.read(source, reader);
        }

        @Override
        public InputStream read(final FlowFile flowFile) {
            return delegate.read(flowFile);
        }

        @Override
        public FlowFile merge(final Collection<FlowFile> sources, final FlowFile destination) {
            return delegate.merge(sources, destination);
        }

        @Override
        public FlowFile merge(final Collection<FlowFile> sources, final FlowFile destination, final byte[] header, final byte[] footer, final byte[] demarcator) {
            return delegate.merge(sources, destination, header, footer, demarcator);
        }

        @Override
        public FlowFile write(final FlowFile source, final OutputStreamCallback writer) {
            return delegate.write(source, writer);
        }

        @Override
        public FlowFile write(final FlowFile source, final StreamCallback writer) {
            return delegate.write(source, writer);
        }

        @Override
        public OutputStream write(final FlowFile source) {
            return delegate.write(source);
        }

        @Override
        public FlowFile append(final FlowFile source, final OutputStreamCallback writer) {
            return delegate.append(source, writer);
        }

        @Override
        public FlowFile importFrom(final Path source, final boolean keepSourceFile, final FlowFile destination) {
            return delegate.importFrom(source, keepSourceFile, destination);
        }

        @Override
        public FlowFile importFrom(final InputStream source, final FlowFile destination) {
            return delegate.importFrom(source, destination);
        }

        @Override
        public void exportTo(final FlowFile flowFile, final Path destination, final boolean append) {
            delegate.exportTo(flowFile, destination, append);
        }

        @Override
        public void exportTo(final FlowFile flowFile, final OutputStream destination) {
            delegate.exportTo(flowFile, destination);
        }

        @Override
        public ProvenanceReporter getProvenanceReporter() {
            return delegate.getProvenanceReporter();
        }

        @Override
        public void setState(final Map<String, String> state, final Scope scope) throws IOException {
            delegate.setState(state, scope);
        }

        @Override
        public StateMap getState(final Scope scope) throws IOException {
            return delegate.getState(scope);
        }

        @Override
        public boolean replaceState(final StateMap oldValue, final Map<String, String> newValue, final Scope scope) throws IOException {
            return delegate.replaceState(oldValue, newValue, scope);
        }

        @Override
        public void clearState(final Scope scope) throws IOException {
            delegate.clearState(scope);
        }
    }
}
