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
package org.apache.nifi.processors.groovyx.flow;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;

import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The Flow file implementation that contains reference to the session.
 * So all commands become easier. Example:
 * <code>flowFile.putAttribute("AttrName", "AttrValue");</code>
 */
@SuppressWarnings("unused")
public abstract class SessionFile implements FlowFile {

    FlowFile flowFile;
    ProcessSessionWrap session;

    protected SessionFile(final ProcessSessionWrap session, final FlowFile f) {
        if (f == null || session == null) {
            throw new NullPointerException("Session and FlowFile are mandatory session=" + session + " file=" + f);
        }
        if (f instanceof SessionFile) {
            throw new RuntimeException("file could be instanceof SessionFile");
        }
        this.flowFile = f;
        this.session = session;
    }

    /**
     * Returns original session.
     */
    public ProcessSessionWrap session() {
        return session;
    }

    /**
     * Clone flowfile with or without content.
     *
     * @param cloneContent clone content or not. attributes cloned in any case.
     * @return new flow file
     */
    public SessionFile clone(final boolean cloneContent) {
        if (cloneContent) {
            return session.clone(flowFile); //new SessionFile(session, session.clone(flowFile));
        }
        return session.create(flowFile); //session.wrap( session.create(flowFile) );
    }

    /**
     * Returns content of the flow file as InputStream.
     */
    public InputStream read() {
        return session.read(flowFile);
    }

    /**
     * read flowfile content.
     */
    public void read(final InputStreamCallback c) {
        session.read(flowFile, c);
    }

    /**
     * write flowfile content.
     *
     * @return reference to self
     */
    public SessionFile write(final StreamCallback c) {
        session.write(this, c);
        return this;
    }

    /**
     * write flowfile content.
     *
     * @return reference to self
     */
    public SessionFile write(final OutputStreamCallback c) {
        session.write(this, c);
        return this;
    }

    /**
     * append flowfile content.
     *
     * @return reference to self
     */
    public SessionFile append(final OutputStreamCallback c) {
        session.append(this, c);
        return this;
    }

    /**
     * set attribute value.
     *
     * @return reference to self
     */
    public SessionFile putAttribute(final String key, final String value) {
        session.putAttribute(this, key, value);
        return this;
    }

    /**
     * Copy attributes from map into flowfile.
     *
     * @return reference to self
     */
    public SessionFile putAllAttributes(final Map<String, String> m) {
        session.putAllAttributes(this, m);
        return this;
    }

    /**
     * Removes one attribute.
     *
     * @return reference to self
     */
    public SessionFile removeAttribute(final String key) {
        session.removeAttribute(this, key);
        return this;
    }

    /**
     * Removes attributes by list.
     *
     * @return reference to self
     */
    public SessionFile removeAllAttributes(final Collection<String> keys) {
        final Set<String> keySet = (Set<String>) (keys instanceof Set ? keys : new HashSet<>(keys));
        session.removeAllAttributes(this, keySet);
        return this;
    }

    /**
     * Transfers to defined relationship or to input relationship if parameter is null.
     */
    public void transfer(final Relationship r) {
        if (r == null) {
            session.transfer(this);
        } else {
            session.transfer(this, r);
        }
    }

    /**
     * Drops this flow file from session.
     */
    public void remove() {
        session.remove(this);
    }

    //OVERRIDE
    @Override
    public long getId() {
        return flowFile.getId();
    }

    @Override
    public long getEntryDate() {
        return flowFile.getEntryDate();
    }

    @Override
    public long getLineageStartDate() {
        return flowFile.getLineageStartDate();
    }

    @Override
    public long getLineageStartIndex() {
        return flowFile.getLineageStartIndex();
    }

    @Override
    public Long getLastQueueDate() {
        return flowFile.getLastQueueDate();
    }

    @Override
    public long getQueueDateIndex() {
        return flowFile.getQueueDateIndex();
    }

    @Override
    public boolean isPenalized() {
        return flowFile.isPenalized();
    }

    @Override
    public String getAttribute(final String key) {
        return flowFile.getAttribute(key);
    }

    @Override
    public long getSize() {
        return flowFile.getSize();
    }

    /**
     * @return an unmodifiable map of the flow file attributes
     */
    @Override
    public Map<String, String> getAttributes() {
        return flowFile.getAttributes();
    }

    @SuppressWarnings("NullableProblems")
    @Override
    public int compareTo(final FlowFile other) {
        final FlowFile compareTarget = other instanceof SessionFile ? ((SessionFile) other).flowFile : other;
        return flowFile.compareTo(compareTarget);
    }

    @Override
    public String toString() {
        return "WRAP[" + flowFile.toString() + "]";
    }

}
