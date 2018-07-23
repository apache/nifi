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

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.processor.exception.MissingFlowFileException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.provenance.ProvenanceReporter;

import org.apache.nifi.processors.groovyx.util.Throwables;

/**
 * wrapped session that collects all created/modified files if created with special flag
 * and able to execute method revertReceivedTo(Relationship r, Throwable t)
 * it will be good to create functionality with created file list and received file list in a standard session.
 * Those file lists will simplify error management.
 */
public abstract class ProcessSessionWrap implements ProcessSession {

    public static final String ERROR_STACKTRACE = "ERROR_STACKTRACE";
    public static final String ERROR_MESSAGE = "ERROR_MESSAGE";
    private ProcessSession s;
    private boolean foe;

    /*
    list of files to be sent to failure on error
    on get() we will store here clone
    */
    private List<FlowFile> toFail = new ArrayList<>();

    /*
    list of files to be dropped on error
    on get(),create(),write(),... we will store here last version of file by id
    */
    private Map<String, FlowFile> toDrop = new HashMap<>();

    public ProcessSessionWrap(ProcessSession s, boolean toFailureOnError) {
        if (s instanceof ProcessSessionWrap) {
            throw new RuntimeException("session could be instanceof ProcessSessionWrap");
        }
        if (s == null) {
            throw new NullPointerException("Session is mandatory session=null");
        }
        this.s = s;
        foe = toFailureOnError;
    }

    /**
     * function returns wrapped flowfile with session for the simplified script access.
     * The sample implementation: <code>
     * public SessionFile wrap(FlowFile f) {
     * if (f == null) {
     * return null;
     * }
     * if (f instanceof SessionFile) {
     * return ((SessionFile) f);
     * }
     * return new SessionFile(this, f);
     * }</code>
     */
    public abstract SessionFile wrap(FlowFile f);

    public List<FlowFile> wrap(List ff) {
        if (ff == null) {
            return null;
        }
        for (int i = 0; i < ff.size(); i++) {
            ff.set(i, wrap((FlowFile) ff.get(i)));
        }
        return ff;
    }

    public FlowFile unwrap(FlowFile f) {
        if (f == null) {
            return null;
        }
        if (f instanceof SessionFile) {
            return ((SessionFile) f).flowFile;
        }
        return f;
    }

    public List<FlowFile> unwrap(Collection<FlowFile> _ff) {
        if (_ff == null) {
            return null;
        }
        List<FlowFile> ff = new ArrayList(_ff);
        for (int i = 0; i < ff.size(); i++) {
            ff.set(i, unwrap(ff.get(i)));
        }
        return ff;
    }

    private void assertNotSessionFile(FlowFile f) {
        if (f instanceof SessionFile) {
            throw new RuntimeException("SessionFile not accepted at this point. " + this.getClass() + " developer failure.");
        }
    }

    /**
     * called when file created or modified
     */
    private FlowFile onMod(FlowFile f) {
        assertNotSessionFile(f);
        if (foe) {
            toDrop.put(f.getAttribute("uuid"), f);
        }
        return f;
    }

    /**
     * called when got file from incoming queue
     */
    private FlowFile onGet(FlowFile f) {
        assertNotSessionFile(f);
        if (f == null) {
            return null;
        }
        if (foe) {
            toFail.add(s.clone(f));
            onMod(f);
        }
        return f;
    }

    private List<FlowFile> onGet(List<FlowFile> ff) {
        if (ff == null) {
            return null;
        }
        if (foe) {
            for (FlowFile f : ff) {
                onGet(f);
            }
        }
        return ff;
    }

    /**
     * called when the file removed
     */
    private void onDrop(FlowFile f) {
        assertNotSessionFile(f);
        if (foe) {
            toDrop.remove(f.getAttribute("uuid"));
        }
    }

    private void onDrop(Collection<FlowFile> ff) {
        if (foe) {
            for (FlowFile f : ff) {
                onDrop(f);
            }
        }
    }

    private void onClear() {
        if (foe) {
            toDrop.clear();
            toFail.clear();
        }
    }

    /**
     * transfers all input files to relationship and drops other files.
     *
     * @param r where to transfer flow files, when null then transfers to input with penalize.
     * @param t the cause why we do this transfer, when relationship specified then additional properties populated: ERROR_MESSAGE and ERROR_STACKTRACE.
     */
    public void revertReceivedTo(Relationship r, Throwable t) {
        for (FlowFile f : toDrop.values()) {
            s.remove(f);
        }
        String errorMessage = Throwables.getMessage(t, null, 950);
        String stackTrace = Throwables.stringStackTrace(t);
        for (FlowFile f : toFail) {
            if (t != null && r != null) {
                f = s.putAttribute(f, ERROR_MESSAGE, errorMessage);
                f = s.putAttribute(f, ERROR_STACKTRACE, stackTrace);
            }
            if (r != null) {
                s.transfer(f, r);
            } else {
                f = s.penalize(f);
                s.transfer(f);
            }
        }
        s.commit();
        onClear();
    }
    /*============================================= NATIVE METHODS ================================================*/

    /**
     * <p>
     * Commits the current session ensuring all operations against FlowFiles
     * within this session are atomically persisted. All FlowFiles operated on
     * within this session must be accounted for by transfer or removal or the
     * commit will fail.
     * </p>
     * <p>
     * <p>
     * As soon as the commit completes the session is again ready to be used
     * </p>
     *
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session.
     * @throws FlowFileHandlingException if not all FlowFiles acted upon within this session are accounted for by user code such that they have a transfer identified or where marked for removal.
     *                                   Automated rollback occurs.
     * @throws ProcessException          if some general fault occurs while persisting the session. Initiates automatic rollback. The root cause can be obtained via <code>Exception.getCause()</code>
     */
    @Override
    public void commit() {
        for (FlowFile f : toFail) {
            s.remove(f);
        }
        s.commit();
        onClear();
    }

    /**
     * Reverts any changes made during this session. All FlowFiles are restored
     * back to their initial session state and back to their original queues. If
     * this session is already committed or rolled back then no changes will
     * occur. This method can be called any number of times. Calling this method
     * is identical to calling {@link #rollback(boolean)} passing
     * <code>false</code> as the parameter.
     */
    @Override
    public void rollback() {
        s.rollback();
        onClear();
    }

    /**
     * Reverts any changes made during this session. All FlowFiles are restored
     * back to their initial session state and back to their original queues,
     * after optionally being penalized. If this session is already committed or
     * rolled back then no changes will occur. This method can be called any
     * number of times.
     *
     * @param penalize whether or not the FlowFiles that are being restored back to their queues should be penalized
     */
    @Override
    public void rollback(boolean penalize) {
        s.rollback(penalize);
        onClear();
    }

    /**
     * Adjusts counter data for the given counter name and takes care of
     * registering the counter if not already present. The adjustment occurs
     * only if and when the ProcessSession is committed.
     *
     * @param name      the name of the counter
     * @param delta     the delta by which to modify the counter (+ or -)
     * @param immediate if true, the counter will be updated immediately, without regard to whether the ProcessSession is commit or rolled back; otherwise, the counter will be incremented only if and
     *                  when the ProcessSession is committed.
     */
    @Override
    public void adjustCounter(String name, long delta, boolean immediate) {
        s.adjustCounter(name, delta, immediate);
    }

    /**
     * @return FlowFile that is next highest priority FlowFile to process. Otherwise returns null.
     */
    @Override
    public SessionFile get() {
        return wrap(onGet(s.get()));
    }

    /**
     * Returns up to <code>maxResults</code> FlowFiles from the work queue. If
     * no FlowFiles are available, returns an empty list. Will not return null.
     * If multiple incoming queues are present, the behavior is unspecified in
     * terms of whether all queues or only a single queue will be polled in a
     * single call.
     *
     * @param maxResults the maximum number of FlowFiles to return
     * @return up to <code>maxResults</code> FlowFiles from the work queue. If no FlowFiles are available, returns an empty list. Will not return null.
     * @throws IllegalArgumentException if <code>maxResults</code> is less than 0
     */
    @Override
    public List<FlowFile> get(int maxResults) {
        return wrap(onGet(s.get(maxResults)));
    }

    /**
     * <p>
     * Returns all FlowFiles from all of the incoming queues for which the given
     * {@link FlowFileFilter} indicates should be accepted. Calls to this method
     * provide exclusive access to the underlying queues. I.e., no other thread
     * will be permitted to pull FlowFiles from this Processor's queues or add
     * FlowFiles to this Processor's incoming queues until this method call has
     * returned.
     * </p>
     *
     * @param filter to limit which flow files are returned
     * @return all FlowFiles from all of the incoming queues for which the given {@link FlowFileFilter} indicates should be accepted.
     */
    @Override
    public List<FlowFile> get(FlowFileFilter filter) {
        return wrap(onGet(s.get(filter)));
    }

    /**
     * @return the QueueSize that represents the number of FlowFiles and their combined data size for all FlowFiles waiting to be processed by the Processor that owns this ProcessSession, regardless
     * of which Connection the FlowFiles live on
     */
    @Override
    public QueueSize getQueueSize() {
        return s.getQueueSize();
    }

    /**
     * Creates a new FlowFile in the repository with no content and without any
     * linkage to a parent FlowFile. This method is appropriate only when data
     * is received or created from an external system. Otherwise, this method
     * should be avoided and should instead use {@link #create(FlowFile)} or
     * {@see #create(Collection)}.
     * <p>
     * When this method is used, a Provenance CREATE or RECEIVE Event should be
     * generated. See the {@link #getProvenanceReporter()} method and
     * {@link ProvenanceReporter} class for more information
     *
     * @return newly created FlowFile
     */
    @Override
    public SessionFile create() {
        return wrap(onMod(s.create()));
    }

    /**
     * Creates a new FlowFile in the repository with no content but with a
     * parent linkage to <code>parent</code>. The newly created FlowFile will
     * inherit all of the parent's attributes except for the UUID. This method
     * will automatically generate a Provenance FORK event or a Provenance JOIN
     * event, depending on whether or not other FlowFiles are generated from the
     * same parent before the ProcessSession is committed.
     *
     * @param parent to base the new flowfile on
     * @return newly created flowfile
     */
    @Override
    public SessionFile create(FlowFile parent) {
        return wrap(onMod(s.create(unwrap(parent))));
    }

    /**
     * Creates a new FlowFile in the repository with no content but with a
     * parent linkage to the FlowFiles specified by the parents Collection. The
     * newly created FlowFile will inherit all of the attributes that are in
     * common to all parents (except for the UUID, which will be in common if
     * only a single parent exists). This method will automatically generate a
     * Provenance JOIN event.
     *
     * @param parents which the new flowfile should inherit shared attributes from
     * @return new flowfile
     */
    @Override
    public SessionFile create(Collection<FlowFile> parents) {
        return wrap(onMod(s.create(unwrap(parents))));
    }

    /**
     * Creates a new FlowFile that is a clone of the given FlowFile as of the
     * time this is called, both in content and attributes. This method
     * automatically emits a Provenance CLONE Event.
     *
     * @param example FlowFile to be the source of cloning - given FlowFile must be a part of the given session
     * @return FlowFile that is a clone of the given example
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws MissingFlowFileException  if the given FlowFile content cannot be found. The FlowFile should no longer be reference, will be internally destroyed, and the session is automatically
     *                                    rolled back and what is left of the FlowFile is destroyed.
     * @throws FlowFileAccessException   if some IO problem occurs accessing FlowFile content
     * @throws NullPointerException      if the argument null
     */
    @Override
    public SessionFile clone(FlowFile example) {
        return wrap(onMod(s.clone(unwrap(example))));
    }

    /**
     * Creates a new FlowFile whose parent is the given FlowFile. The content of
     * the new FlowFile will be a subset of the byte sequence of the given
     * FlowFile starting at the specified offset and with the length specified.
     * The new FlowFile will contain all of the attributes of the original. This
     * method automatically emits a Provenance FORK Event (or a Provenance CLONE
     * Event, if the offset is 0 and the size is exactly equal to the size of
     * the example FlowFile).
     *
     * @param parent to base the new flowfile attributes on
     * @param offset of the parent flowfile to base the child flowfile content on
     * @param size   of the new flowfile from the offset
     * @return a FlowFile with the specified size whose parent is first argument to this function
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session, or if the specified offset + size exceeds that of the size of the
     *                                   parent FlowFile. Automatic rollback will occur.
     * @throws MissingFlowFileException  if the given FlowFile content cannot be found. The FlowFile should no longer be reference, will be internally destroyed, and the session is automatically
     *                                    rolled back and what is left of the FlowFile is destroyed.
     */
    @Override
    public SessionFile clone(FlowFile parent, long offset, long size) {
        return wrap(onMod(s.clone(unwrap(parent), offset, size)));
    }

    /**
     * Sets a penalty for the given FlowFile which will make it unavailable to
     * be operated on any further during the penalty period.
     *
     * @param flowFile to penalize
     * @return FlowFile the new FlowFile reference to use
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws NullPointerException      if the argument null
     */
    @Override
    public SessionFile penalize(FlowFile flowFile) {
        SessionFile sf = wrap(flowFile);
        sf.flowFile = onMod(s.penalize(sf.flowFile));
        return sf;
    }

    /**
     * Updates the given FlowFiles attributes with the given key/value pair. If
     * the key is named {@code uuid}, this attribute will be ignored.
     *
     * @param flowFile to update
     * @param key      of attribute
     * @param value    of attribute
     * @return FlowFile the updated FlowFile
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws NullPointerException      if an argument is null
     */
    @Override
    public SessionFile putAttribute(FlowFile flowFile, String key, String value) {
        SessionFile sf = wrap(flowFile);
        sf.flowFile = onMod(s.putAttribute(sf.flowFile, key, value));
        return sf;
    }

    /**
     * Updates the given FlowFiles attributes with the given key/value pairs. If
     * the map contains a key named {@code uuid}, this attribute will be
     * ignored.
     *
     * @param flowFile   to update
     * @param attributes the attributes to add to the given FlowFile
     * @return FlowFile the updated FlowFile
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws NullPointerException      if an argument is null
     */
    @Override
    public SessionFile putAllAttributes(FlowFile flowFile, Map<String, String> attributes) {
        SessionFile sf = wrap(flowFile);
        sf.flowFile = onMod(s.putAllAttributes(sf.flowFile, attributes));
        return sf;
    }

    /**
     * Removes the given FlowFile attribute with the given key. If the key is
     * named {@code uuid}, this method will return the same FlowFile without
     * removing any attribute.
     *
     * @param flowFile to update
     * @param key      of attribute
     * @return FlowFile the updated FlowFile
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws NullPointerException      if the argument null
     */
    @Override
    public SessionFile removeAttribute(FlowFile flowFile, String key) {
        SessionFile sf = wrap(flowFile);
        sf.flowFile = onMod(s.removeAttribute(sf.flowFile, key));
        return sf;
    }

    /**
     * Removes the attributes with the given keys from the given FlowFile. If
     * the set of keys contains the value {@code uuid}, this key will be ignored
     *
     * @param flowFile to update
     * @param keys     of attribute
     * @return FlowFile the updated FlowFile
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws NullPointerException      if the argument null
     */
    @Override
    public SessionFile removeAllAttributes(FlowFile flowFile, Set<String> keys) {
        SessionFile sf = wrap(flowFile);
        sf.flowFile = onMod(s.removeAllAttributes(sf.flowFile, keys));
        return sf;
    }

    /**
     * Remove all attributes from the given FlowFile that have keys which match
     * the given pattern. If the pattern matches the key {@code uuid}, this key
     * will not be removed.
     *
     * @param flowFile   to update
     * @param keyPattern may be null; if supplied is matched against each of the FlowFile attribute keys
     * @return FlowFile containing only attributes which did not meet the key pattern
     */
    @Override
    public SessionFile removeAllAttributes(FlowFile flowFile, Pattern keyPattern) {
        SessionFile sf = wrap(flowFile);
        sf.flowFile = onMod(s.removeAllAttributes(sf.flowFile, keyPattern));
        return sf;
    }

    /**
     * Transfers the given FlowFile to the appropriate destination processor
     * work queue(s) based on the given relationship. If the relationship leads
     * to more than one destination the state of the FlowFile is replicated such
     * that each destination receives an exact copy of the FlowFile though each
     * will have its own unique identity. The destination processors will not be
     * able to operate on the given FlowFile until this session is committed or
     * until the ownership of the session is migrated to another processor. If
     * ownership of the session is passed to a destination processor then that
     * destination processor will have immediate visibility of the transferred
     * FlowFiles within the session.
     *
     * @param flowFile     to transfer
     * @param relationship to transfer to
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws NullPointerException      if the argument null
     * @throws IllegalArgumentException  if given relationship is not a known or registered relationship
     */
    @Override
    public void transfer(FlowFile flowFile, Relationship relationship) {
        flowFile = unwrap(flowFile);
        s.transfer(flowFile, relationship);
    }

    /**
     * Transfers the given FlowFile back to the work queue from which it was
     * pulled. The processor will not be able to operate on the given FlowFile
     * until this session is committed. Any modifications that have been made to
     * the FlowFile will be maintained. FlowFiles that are created by the
     * processor cannot be transferred back to themselves via this method.
     *
     * @param flowFile to transfer
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws IllegalArgumentException  if the FlowFile was created by this processor
     * @throws NullPointerException      if the argument null
     */
    @Override
    public void transfer(FlowFile flowFile) {
        flowFile = unwrap(flowFile);
        s.transfer(flowFile);
    }

    /**
     * Transfers the given FlowFiles back to the work queues from which the
     * FlowFiles were pulled. The processor will not be able to operate on the
     * given FlowFile until this session is committed. Any modifications that
     * have been made to the FlowFile will be maintained. FlowFiles that are
     * created by the processor cannot be transferred back to themselves via
     * this method.
     *
     * @param flowFiles to transfer
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFiles are already transferred or removed or don't belong to this session. Automatic rollback will occur.
     * @throws IllegalArgumentException  if the FlowFile was created by this processor
     * @throws NullPointerException      if the argument null
     */
    @Override
    public void transfer(Collection<FlowFile> flowFiles) {
        flowFiles = unwrap(flowFiles);
        s.transfer(flowFiles);
    }

    /**
     * Transfers the given FlowFile to the appropriate destination processor
     * work queue(s) based on the given relationship. If the relationship leads
     * to more than one destination the state of the FlowFile is replicated such
     * that each destination receives an exact copy of the FlowFile though each
     * will have its own unique identity. The destination processors will not be
     * able to operate on the given FlowFile until this session is committed or
     * until the ownership of the session is migrated to another processor. If
     * ownership of the session is passed to a destination processor then that
     * destination processor will have immediate visibility of the transferred
     * FlowFiles within the session.
     *
     * @param flowFiles    to transfer
     * @param relationship to transfer to
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws NullPointerException      if the argument null
     * @throws IllegalArgumentException  if given relationship is not a known or registered relationship
     */
    @Override
    public void transfer(Collection<FlowFile> flowFiles, Relationship relationship) {
        flowFiles = unwrap(flowFiles);
        s.transfer(flowFiles, relationship);
    }

    /**
     * Ends the managed persistence for the given FlowFile. The persistent
     * attributes for the FlowFile are deleted and so is the content assuming
     * nothing else references it and this FlowFile will no longer be available
     * for further operation.
     *
     * @param flowFile to remove
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     */
    @Override
    public void remove(FlowFile flowFile) {
        flowFile = unwrap(flowFile);
        s.remove(flowFile);
        onDrop(flowFile);
    }

    /**
     * Ends the managed persistence for the given FlowFiles. The persistent
     * attributes for the FlowFile are deleted and so is the content assuming
     * nothing else references it and this FlowFile will no longer be available
     * for further operation.
     *
     * @param flowFiles to remove
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if any of the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     */
    @Override
    public void remove(Collection<FlowFile> flowFiles) {
        flowFiles = unwrap(flowFiles);
        s.remove(flowFiles);
        onDrop(flowFiles);
    }

    /**
     * Executes the given callback against the contents corresponding to the
     * given FlowFile.
     *
     * @param flowFile flow file to retrieve content of
     * @param reader callback that will be called to read the flow file content
     * @throws FlowFileAccessException   if some IO problem occurs accessing FlowFile content; if an attempt is made to access the InputStream provided to the given InputStreamCallback after
     *                                    this method completed its execution
     */
    @Override
    public void read(FlowFile flowFile, InputStreamCallback reader) throws FlowFileAccessException {
        flowFile = unwrap(flowFile);
        s.read(flowFile, reader);
    }

    /**
     * Provides an InputStream that can be used to read the contents of the given FlowFile.
     * This method differs from those that make use of callbacks in that this method returns
     * an InputStream and expects the caller to properly handle the lifecycle of the InputStream
     * (i.e., the caller is responsible for ensuring that the InputStream is closed appropriately).
     * The Process Session may or may not handle closing the stream when {@link #commit()} or {@link #rollback()}
     * is called, but the responsibility of doing so belongs to the caller. The InputStream will throw
     * an IOException if an attempt is made to read from the stream after the session is committed or
     * rolled back.
     *
     * @param flowFile the FlowFile to read
     * @return an InputStream that can be used to read the contents of the FlowFile
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws MissingFlowFileException  if the given FlowFile content cannot be found. The FlowFile should no longer be referenced, will be internally destroyed, and the session is automatically
     *                                   rolled back and what is left of the FlowFile is destroyed.
     */
    @Override
    public InputStream read(FlowFile flowFile) {
        flowFile = unwrap(flowFile);
        return s.read(flowFile);
    }

    /**
     * Executes the given callback against the contents corresponding to the
     * given FlowFile.
     * <p>
     * <i>Note</i>: The OutputStream provided to the given OutputStreamCallback
     * will not be accessible once this method has completed its execution.
     *
     * @param flowFile                     flow file to retrieve content of
     * @param allowSessionStreamManagement allow session to hold the stream open for performance reasons
     * @param reader                       that will be called to read the flow file content
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws MissingFlowFileException  if the given FlowFile content cannot be found. The FlowFile should no longer be reference, will be internally destroyed, and the session is automatically
     *                                    rolled back and what is left of the FlowFile is destroyed.
     * @throws FlowFileAccessException   if some IO problem occurs accessing FlowFile content; if an attempt is made to access the InputStream provided to the given InputStreamCallback after this
     *                                    method completed its execution
     */
    @Override
    public void read(FlowFile flowFile, boolean allowSessionStreamManagement, InputStreamCallback reader) throws FlowFileAccessException {
        flowFile = unwrap(flowFile);
        s.read(flowFile, allowSessionStreamManagement, reader);
    }

    /**
     * Combines the content of all given source FlowFiles into a single given
     * destination FlowFile.
     *
     * @param sources     the flowfiles to merge
     * @param destination the flowfile to use as the merged result
     * @return updated destination FlowFile (new size, etc...)
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws IllegalArgumentException  if the given destination is contained within the sources
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws MissingFlowFileException  if the given FlowFile content cannot be found. The FlowFile should no longer be reference, will be internally destroyed, and the session is automatically
     *                                    rolled back and what is left of the FlowFile is destroyed.
     * @throws FlowFileAccessException   if some IO problem occurs accessing FlowFile content. The state of the destination will be as it was prior to this call.
     */
    @Override
    public SessionFile merge(Collection<FlowFile> sources, FlowFile destination) {
        SessionFile sfDestination = wrap(destination);
        sources = unwrap(sources);
        sfDestination.flowFile = onMod(s.merge(sources, sfDestination.flowFile));
        return sfDestination;
    }

    /**
     * Combines the content of all given source FlowFiles into a single given
     * destination FlowFile.
     *
     * @param sources     to merge together
     * @param destination to merge to
     * @param header      bytes that will be added to the beginning of the merged output. May be null or empty.
     * @param footer      bytes that will be added to the end of the merged output. May be null or empty.
     * @param demarcator  bytes that will be placed in between each object merged together. May be null or empty.
     * @return updated destination FlowFile (new size, etc...)
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws IllegalArgumentException  if the given destination is contained within the sources
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws MissingFlowFileException  if the given FlowFile content cannot be found. The FlowFile should no longer be reference, will be internally destroyed, and the session is automatically
     *                                    rolled back and what is left of the FlowFile is destroyed.
     * @throws FlowFileAccessException   if some IO problem occurs accessing FlowFile content. The state of the destination will be as it was prior to this call.
     */
    @Override
    public SessionFile merge(Collection<FlowFile> sources, FlowFile destination, byte[] header, byte[] footer, byte[] demarcator) {
        SessionFile sfDestination = wrap(destination);
        sources = unwrap(sources);
        sfDestination.flowFile = onMod(s.merge(sources, sfDestination.flowFile, header, footer, demarcator));
        return sfDestination;
    }

    /**
     * Executes the given callback against the content corresponding to the
     * given FlowFile.
     * <p>
     * <i>Note</i>: The OutputStream provided to the given OutputStreamCallback
     * will not be accessible once this method has completed its execution.
     *
     * @param flowFile to write to
     * @param writer used to write new content
     * @return updated FlowFile
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws MissingFlowFileException  if the given FlowFile content cannot be found. The FlowFile should no longer be referenced, will be internally destroyed, and the session is automatically
     *                                   rolled back and what is left of the FlowFile is destroyed.
     * @throws FlowFileAccessException   if some IO problem occurs accessing FlowFile content; if an attempt is made to access the OutputStream provided to the given OutputStreamCallback after this
     *                                   method completed its execution
     */
    @Override
    public SessionFile write(FlowFile flowFile, OutputStreamCallback writer) throws FlowFileAccessException {
        SessionFile sf = wrap(flowFile);
        sf.flowFile = onMod(s.write(sf.flowFile, writer));
        return sf;
    }

    /**
     * Executes the given callback against the content corresponding to the
     * given flow file.
     * <p>
     * <i>Note</i>: The InputStream & OutputStream provided to the given
     * StreamCallback will not be accessible once this method has completed its
     * execution.
     *
     * @param flowFile to read from and write to
     * @param writer used to read the old content and write new content
     * @return updated FlowFile
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws MissingFlowFileException  if the given FlowFile content cannot be found. The FlowFile should no longer be reference, will be internally destroyed, and the session is automatically
     *                                    rolled back and what is left of the FlowFile is destroyed.
     * @throws FlowFileAccessException   if some IO problem occurs accessing FlowFile content; if an attempt is made to access the InputStream or OutputStream provided to the given StreamCallback
     *                                    after  this method completed its execution
     */
    @Override
    public SessionFile write(FlowFile flowFile, StreamCallback writer) throws FlowFileAccessException {
        SessionFile sf = wrap(flowFile);
        sf.flowFile = onMod(s.write(sf.flowFile, writer));
        return sf;
    }

    /**
     * Executes the given callback against the content corresponding to the
     * given FlowFile, such that any data written to the OutputStream of the
     * content will be appended to the end of FlowFile.
     * <p>
     * <i>Note</i>: The OutputStream provided to the given OutputStreamCallback
     * will not be accessible once this method has completed its execution.
     *
     * @param flowFile the flowfile for which content should be appended
     * @param writer used to write new bytes to the flowfile content
     * @return the updated flowfile reference for the new content
     * @throws FlowFileAccessException if an attempt is made to access the OutputStream provided to the given OutputStreamCallback after this method completed its execution
     */
    @Override
    public SessionFile append(FlowFile flowFile, OutputStreamCallback writer) throws FlowFileAccessException {
        SessionFile sf = wrap(flowFile);
        sf.flowFile = onMod(s.append(sf.flowFile, writer));
        return sf;
    }

    /**
     * Writes to the given FlowFile all content from the given content path.
     *
     * @param source         the file from which content will be obtained
     * @param keepSourceFile if true the content is simply copied; if false the original content might be used in a destructive way for efficiency such that the repository will have the data but the
     *                       original data will be gone. If false the source object will be removed or gone once imported. It will not be restored if the session is rolled back
     *                       so this must be used with caution. In some cases it can result in tremendous efficiency gains but is also dangerous.
     * @param flowFile    the FlowFile whose content will be updated
     * @return the updated destination FlowFile (new size)
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws MissingFlowFileException  if the given FlowFile content cannot be found. The FlowFile should no longer be reference, will be internally destroyed, and the session is automatically
     *                                    rolled back and what is left of the FlowFile is destroyed.
     * @throws FlowFileAccessException   if some IO problem occurs accessing FlowFile content
     */
    @Override
    public SessionFile importFrom(Path source, boolean keepSourceFile, FlowFile flowFile) {
        SessionFile sf = wrap(flowFile);
        sf.flowFile = onMod(s.importFrom(source, keepSourceFile, sf.flowFile));
        return sf;
    }

    /**
     * Writes to the given FlowFile all content from the given content path.
     *
     * @param source      the file from which content will be obtained
     * @param flowFile the FlowFile whose content will be updated
     * @return the updated destination FlowFile (new size)
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws MissingFlowFileException  if the given FlowFile content cannot be found. The FlowFile should no longer be reference, will be internally destroyed, and the session is automatically
     *                                   rolled back and what is left of the FlowFile is destroyed.
     * @throws FlowFileAccessException   if some IO problem occurs accessing FlowFile content
     */
    @Override
    public SessionFile importFrom(InputStream source, FlowFile flowFile) {
        SessionFile sf = wrap(flowFile);
        sf.flowFile = onMod(s.importFrom(source, sf.flowFile));
        return sf;
    }

    /**
     * Writes the content of the given FlowFile to the given destination path.
     *
     * @param flowFile    to export the content of
     * @param destination to export the content to
     * @param append      if true will append to the current content at the given path; if false will replace any current content
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws MissingFlowFileException  if the given FlowFile content cannot be found. The FlowFile should no longer be reference, will be internally destroyed, and the session is automatically
     *                                   rolled back and what is left of the FlowFile is destroyed.
     * @throws FlowFileAccessException   if some IO problem occurs accessing FlowFile content
     */
    @Override
    public void exportTo(FlowFile flowFile, Path destination, boolean append) {
        flowFile = unwrap(flowFile);
        s.exportTo(flowFile, destination, append);
    }

    /**
     * Writes the content of the given FlowFile to the given destination stream
     *
     * @param flowFile    to export the content of
     * @param destination to export the content to
     * @throws IllegalStateException     if detected that this method is being called from within a callback of another method in this session and for the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already transferred or removed or doesn't belong to this session. Automatic rollback will occur.
     * @throws MissingFlowFileException  if the given FlowFile content cannot be found. The FlowFile should no longer be reference, will be internally destroyed, and the session is automatically
     *                                   rolled back and what is left of the FlowFile is destroyed.
     * @throws FlowFileAccessException   if some IO problem occurs accessing FlowFile content
     */
    @Override
    public void exportTo(FlowFile flowFile, OutputStream destination) {
        flowFile = unwrap(flowFile);
        s.exportTo(flowFile, destination);
    }

    /**
     * Returns a ProvenanceReporter that is tied to this ProcessSession.
     *
     * @return the provenance reporter
     */
    @Override
    public ProvenanceReporter getProvenanceReporter() {
        return s.getProvenanceReporter();
    }

    @Override
    public void migrate(ProcessSession newOwner, Collection<FlowFile> flowFiles) {
        flowFiles = unwrap(flowFiles);
        s.migrate(newOwner, flowFiles);
    }

    /**
     * Provides an OutputStream that can be used to write to the contents of the
     * given FlowFile.
     *
     * @param source to write to
     *
     * @return an OutputStream that can be used to write to the contents of the FlowFile
     *
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s), or if there is an open InputStream or OutputStream for the FlowFile's content
     * (see {@link #read(FlowFile)}).
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws MissingFlowFileException if the given FlowFile content cannot be
     * found. The FlowFile should no longer be referenced, will be internally
     * destroyed, and the session is automatically rolled back and what is left
     * of the FlowFile is destroyed.
     * @throws FlowFileAccessException if some IO problem occurs accessing
     * FlowFile content; if an attempt is made to access the OutputStream
     * provided to the given OutputStreamCallaback after this method completed
     * its execution
     */
    @Override
    public OutputStream write(FlowFile source) {
        source = unwrap(source);
        return s.write(source);
    }

}
