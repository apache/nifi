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
package org.apache.nifi.processor;

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
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.processor.exception.MissingFlowFileException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.provenance.ProvenanceReporter;

/**
 * <p>
 * A process session encompasses all the behaviors a processor can perform to
 * obtain, clone, read, modify remove FlowFiles in an atomic unit. A process
 * session is always tied to a single processor at any one time and ensures no
 * FlowFile can ever be accessed by any more than one processor at a given time.
 * The session also ensures that all FlowFiles are always accounted for. The
 * creator of a ProcessSession is always required to manage the session.
 * </p>
 *
 * <p>
 * A session is not considered thread safe. The session supports a unit of work
 * that is either committed or rolled back
 * </p>
 *
 * <p>
 * As noted on specific methods and for specific exceptions automated rollback
 * will occur to ensure consistency of the repository. However, several
 * situations can result in exceptions yet not cause automated rollback. In
 * these cases the consistency of the repository will be retained but callers
 * will be able to indicate whether it should result in rollback or continue on
 * toward a commit.
 * </p>
 *
 * <p>
 * A process session has two 'terminal' methods that will result in the process session
 * being in a 'fresh', containing no knowledge or any FlowFile, as if the session were newly
 * created. After one of these methods is called, the instance may be used again. The terminal
 * methods for a Process Session are the {@link #commit()} and {@link #rollback()}. Additionally,
 * the {@link #migrate(ProcessSession, Collection)} method results in {@code this} containing
 * no knowledge of any of the FlowFiles that are provided, as if the FlowFiles never existed in
 * this ProcessSession. After each commit or rollback, the session can be used again. Note, however,
 * that even if all FlowFiles are migrated via the {@link #migrate(ProcessSession, Collection)} method,
 * this Process Session is not entirely cleared, as it still has knowledge of Counters that were adjusted
 * via the {@link #adjustCounter(String, long, boolean)} method. A commit or rollback will clear these
 * counters, as well.
 * </p>
 */
public interface ProcessSession {

    /**
     * <p>
     * Commits the current session ensuring all operations against FlowFiles
     * within this session are atomically persisted. All FlowFiles operated on
     * within this session must be accounted for by transfer or removal or the
     * commit will fail.</p>
     *
     * <p>
     * As soon as the commit completes the session is again ready to be used</p>
     *
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session.
     * @throws FlowFileHandlingException if not all FlowFiles acted upon within
     * this session are accounted for by user code such that they have a
     * transfer identified or where marked for removal. Automated rollback
     * occurs.
     * @throws ProcessException if some general fault occurs while persisting
     * the session. Initiates automatic rollback. The root cause can be obtained
     * via <code>Exception.getCause()</code>
     */
    void commit();

    /**
     * Reverts any changes made during this session. All FlowFiles are restored
     * back to their initial session state and back to their original queues. If
     * this session is already committed or rolled back then no changes will
     * occur. This method can be called any number of times. Calling this method
     * is identical to calling {@link #rollback(boolean)} passing
     * <code>false</code> as the parameter.
     */
    void rollback();

    /**
     * Reverts any changes made during this session. All FlowFiles are restored
     * back to their initial session state and back to their original queues,
     * after optionally being penalized. If this session is already committed or
     * rolled back then no changes will occur. This method can be called any
     * number of times.
     *
     * @param penalize whether or not the FlowFiles that are being restored back
     * to their queues should be penalized
     */
    void rollback(boolean penalize);

    /**
     * <p>
     * Migrates ownership of the given FlowFiles from {@code this} to the given {@code newOwner}.
     * </p>
     *
     * <p>
     * When calling this method, all of the following pre-conditions must be met:
     * </p>
     *
     * <ul>
     * <li>This method cannot be called from within a callback
     * (see {@link #write(FlowFile, OutputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     * {@link #read(FlowFile, InputStreamCallback)}, {@link #read(FlowFile, boolean, InputStreamCallback)} for any of
     * the given FlowFiles.</li>
     * <li>No InputStream can be open for the content of any of the given FlowFiles (see {@link #read(FlowFile)}).</li>
     * <li>Each of the FlowFiles provided must be the most up-to-date copy of the FlowFile.</li>
     * <li>For any provided FlowFile, if the FlowFile has any child (e.g., by calling {@link #create(FlowFile)} and passing the FlowFile
     * as the argument), then all children that were created must also be in the Collection of provided FlowFiles.</li>
     * </ul>
     *
     * @param newOwner the ProcessSession that is to become the new owner of all FlowFiles
     *            that currently belong to {@code this}.
     * @param flowFiles the FlowFiles to migrate
     */
    void migrate(ProcessSession newOwner, Collection<FlowFile> flowFiles);

    /**
     * Adjusts counter data for the given counter name and takes care of
     * registering the counter if not already present. The adjustment occurs
     * only if and when the ProcessSession is committed.
     *
     * @param name the name of the counter
     * @param delta the delta by which to modify the counter (+ or -)
     * @param immediate if true, the counter will be updated immediately,
     *            without regard to whether the ProcessSession is commit or rolled back;
     *            otherwise, the counter will be incremented only if and when the
     *            ProcessSession is committed.
     */
    void adjustCounter(String name, long delta, boolean immediate);

    /**
     * @return FlowFile that is next highest priority FlowFile to process.
     * Otherwise returns null.
     */
    FlowFile get();

    /**
     * Returns up to <code>maxResults</code> FlowFiles from the work queue. If
     * no FlowFiles are available, returns an empty list. Will not return null.
     * If multiple incoming queues are present, the behavior is unspecified in
     * terms of whether all queues or only a single queue will be polled in a
     * single call.
     *
     * @param maxResults the maximum number of FlowFiles to return
     * @return up to <code>maxResults</code> FlowFiles from the work queue. If
     * no FlowFiles are available, returns an empty list. Will not return null.
     * @throws IllegalArgumentException if <code>maxResults</code> is less than
     * 0
     */
    List<FlowFile> get(int maxResults);

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
     * @return all FlowFiles from all of the incoming queues for which the given
     * {@link FlowFileFilter} indicates should be accepted.
     */
    List<FlowFile> get(FlowFileFilter filter);

    /**
     * @return the QueueSize that represents the number of FlowFiles and their
     * combined data size for all FlowFiles waiting to be processed by the
     * Processor that owns this ProcessSession, regardless of which Connection
     * the FlowFiles live on
     */
    QueueSize getQueueSize();

    /**
     * Creates a new FlowFile in the repository with no content and without any
     * linkage to a parent FlowFile. This method is appropriate only when data
     * is received or created from an external system. Otherwise, this method
     * should be avoided and should instead use {@link #create(FlowFile)} or
     * {@see #create(Collection)}.
     *
     * When this method is used, a Provenance CREATE or RECEIVE Event should be
     * generated. See the {@link #getProvenanceReporter()} method and
     * {@link ProvenanceReporter} class for more information
     *
     * @return newly created FlowFile
     */
    FlowFile create();

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
    FlowFile create(FlowFile parent);

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
    FlowFile create(Collection<FlowFile> parents);

    /**
     * Creates a new FlowFile that is a clone of the given FlowFile as of the
     * time this is called, both in content and attributes. This method
     * automatically emits a Provenance CLONE Event.
     *
     * @param example FlowFile to be the source of cloning - given FlowFile must
     * be a part of the given session
     * @return FlowFile that is a clone of the given example
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws MissingFlowFileException if the given FlowFile content cannot be
     * found. The FlowFile should no longer be reference, will be internally
     * destroyed, and the session is automatically rolled back and what is left
     * of the FlowFile is destroyed.
     * @throws FlowFileAccessException if some IO problem occurs accessing
     * FlowFile content
     * @throws NullPointerException if the argument null
     */
    FlowFile clone(FlowFile example);

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
     * @param size of the new flowfile from the offset
     * @return a FlowFile with the specified size whose parent is first argument
     * to this function
     *
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session, or if the
     * specified offset + size exceeds that of the size of the parent FlowFile.
     * Automatic rollback will occur.
     * @throws MissingFlowFileException if the given FlowFile content cannot be
     * found. The FlowFile should no longer be reference, will be internally
     * destroyed, and the session is automatically rolled back and what is left
     * of the FlowFile is destroyed.
     */
    FlowFile clone(FlowFile parent, long offset, long size);

    /**
     * Sets a penalty for the given FlowFile which will make it unavailable to
     * be operated on any further during the penalty period.
     *
     * @param flowFile to penalize
     * @return FlowFile the new FlowFile reference to use
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws NullPointerException if the argument null
     */
    FlowFile penalize(FlowFile flowFile);

    /**
     * Updates the given FlowFiles attributes with the given key/value pair. If
     * the key is named {@code uuid}, this attribute will be ignored.
     *
     * @param flowFile to update
     * @param key of attribute
     * @param value of attribute
     * @return FlowFile the updated FlowFile
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws NullPointerException if an argument is null
     */
    FlowFile putAttribute(FlowFile flowFile, String key, String value);

    /**
     * Updates the given FlowFiles attributes with the given key/value pairs. If
     * the map contains a key named {@code uuid}, this attribute will be
     * ignored.
     *
     * @param flowFile to update
     * @param attributes the attributes to add to the given FlowFile
     * @return FlowFile the updated FlowFile
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws NullPointerException if an argument is null
     */
    FlowFile putAllAttributes(FlowFile flowFile, Map<String, String> attributes);

    /**
     * Removes the given FlowFile attribute with the given key. If the key is
     * named {@code uuid}, this method will return the same FlowFile without
     * removing any attribute.
     *
     * @param flowFile to update
     * @param key of attribute
     * @return FlowFile the updated FlowFile
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws NullPointerException if the argument null
     */
    FlowFile removeAttribute(FlowFile flowFile, String key);

    /**
     * Removes the attributes with the given keys from the given FlowFile. If
     * the set of keys contains the value {@code uuid}, this key will be ignored
     *
     * @param flowFile to update
     * @param keys of attribute
     * @return FlowFile the updated FlowFile
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws NullPointerException if the argument null
     */
    FlowFile removeAllAttributes(FlowFile flowFile, Set<String> keys);

    /**
     * Remove all attributes from the given FlowFile that have keys which match
     * the given pattern. If the pattern matches the key {@code uuid}, this key
     * will not be removed.
     *
     * @param flowFile to update
     * @param keyPattern may be null; if supplied is matched against each of the
     * FlowFile attribute keys
     * @return FlowFile containing only attributes which did not meet the key
     * pattern
     */
    FlowFile removeAllAttributes(FlowFile flowFile, Pattern keyPattern);

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
     * @param flowFile to transfer
     * @param relationship to transfer to
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws NullPointerException if the argument null
     * @throws IllegalArgumentException if given relationship is not a known or
     * registered relationship
     */
    void transfer(FlowFile flowFile, Relationship relationship);

    /**
     * Transfers the given FlowFile back to the work queue from which it was
     * pulled. The processor will not be able to operate on the given FlowFile
     * until this session is committed. Any modifications that have been made to
     * the FlowFile will be maintained. FlowFiles that are created by the
     * processor cannot be transferred back to themselves via this method.
     *
     * @param flowFile to transfer
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws IllegalArgumentException if the FlowFile was created by this
     * processor
     * @throws NullPointerException if the argument null
     */
    void transfer(FlowFile flowFile);

    /**
     * Transfers the given FlowFiles back to the work queues from which the
     * FlowFiles were pulled. The processor will not be able to operate on the
     * given FlowFile until this session is committed. Any modifications that
     * have been made to the FlowFile will be maintained. FlowFiles that are
     * created by the processor cannot be transferred back to themselves via
     * this method.
     *
     * @param flowFiles to transfer
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFiles are already
     * transferred or removed or don't belong to this session. Automatic
     * rollback will occur.
     * @throws IllegalArgumentException if the FlowFile was created by this
     * processor
     * @throws NullPointerException if the argument null
     */
    void transfer(Collection<FlowFile> flowFiles);

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
     * @param flowFiles to transfer
     * @param relationship to transfer to
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws NullPointerException if the argument null
     * @throws IllegalArgumentException if given relationship is not a known or
     * registered relationship
     */
    void transfer(Collection<FlowFile> flowFiles, Relationship relationship);

    /**
     * Ends the managed persistence for the given FlowFile. The persistent
     * attributes for the FlowFile are deleted and so is the content assuming
     * nothing else references it and this FlowFile will no longer be available
     * for further operation.
     *
     * @param flowFile to remove
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     */
    void remove(FlowFile flowFile);

    /**
     * Ends the managed persistence for the given FlowFiles. The persistent
     * attributes for the FlowFile are deleted and so is the content assuming
     * nothing else references it and this FlowFile will no longer be available
     * for further operation.
     *
     * @param flowFiles to remove
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if any of the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     */
    void remove(Collection<FlowFile> flowFiles);

    /**
     * Executes the given callback against the contents corresponding to the
     * given FlowFile.
     *
     * @param source flowfile to retrieve content of
     * @param reader that will be called to read the flowfile content
     * @throws IllegalStateException if detected that this method is being
     *             called from within a callback of another method in this session and for
     *             the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     *             transferred or removed or doesn't belong to this session. Automatic
     *             rollback will occur.
     * @throws MissingFlowFileException if the given FlowFile content cannot be
     *             found. The FlowFile should no longer be referenced, will be internally
     *             destroyed, and the session is automatically rolled back and what is left
     *             of the FlowFile is destroyed.
     * @throws FlowFileAccessException if some IO problem occurs accessing
     *             FlowFile content; if an attempt is made to access the InputStream
     *             provided to the given InputStreamCallback after this method completed its
     *             execution
     */
    void read(FlowFile source, InputStreamCallback reader) throws FlowFileAccessException;

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
     * @throws IllegalStateException if detected that this method is being
     *             called from within a callback of another method in this session and for
     *             the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     *             transferred or removed or doesn't belong to this session. Automatic
     *             rollback will occur.
     * @throws MissingFlowFileException if the given FlowFile content cannot be
     *             found. The FlowFile should no longer be referenced, will be internally
     *             destroyed, and the session is automatically rolled back and what is left
     *             of the FlowFile is destroyed.
     */
    InputStream read(FlowFile flowFile);

    /**
     * Executes the given callback against the contents corresponding to the
     * given FlowFile.
     *
     * <i>Note</i>: The OutputStream provided to the given OutputStreamCallback
     * will not be accessible once this method has completed its execution.
     *
     * @param source flowfile to retrieve content of
     * @param allowSessionStreamManagement allow session to hold the stream open for performance reasons
     * @param reader that will be called to read the flowfile content
     * @throws IllegalStateException if detected that this method is being
     *             called from within a callback of another method in this session and for
     *             the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     *             transferred or removed or doesn't belong to this session. Automatic
     *             rollback will occur.
     * @throws MissingFlowFileException if the given FlowFile content cannot be
     *             found. The FlowFile should no longer be reference, will be internally
     *             destroyed, and the session is automatically rolled back and what is left
     *             of the FlowFile is destroyed.
     * @throws FlowFileAccessException if some IO problem occurs accessing
     *             FlowFile content; if an attempt is made to access the InputStream
     *             provided to the given InputStreamCallback after this method completed its
     *             execution
     */
    void read(FlowFile source, boolean allowSessionStreamManagement, InputStreamCallback reader) throws FlowFileAccessException;

    /**
     * Combines the content of all given source FlowFiles into a single given
     * destination FlowFile.
     *
     * @param sources the flowfiles to merge
     * @param destination the flowfile to use as the merged result
     * @return updated destination FlowFile (new size, etc...)
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws IllegalArgumentException if the given destination is contained
     * within the sources
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws MissingFlowFileException if the given FlowFile content cannot be
     * found. The FlowFile should no longer be reference, will be internally
     * destroyed, and the session is automatically rolled back and what is left
     * of the FlowFile is destroyed.
     * @throws FlowFileAccessException if some IO problem occurs accessing
     * FlowFile content. The state of the destination will be as it was prior to
     * this call.
     */
    FlowFile merge(Collection<FlowFile> sources, FlowFile destination);

    /**
     * Combines the content of all given source FlowFiles into a single given
     * destination FlowFile.
     *
     * @param sources to merge together
     * @param destination to merge to
     * @param header bytes that will be added to the beginning of the merged
     * output. May be null or empty.
     * @param footer bytes that will be added to the end of the merged output.
     * May be null or empty.
     * @param demarcator bytes that will be placed in between each object merged
     * together. May be null or empty.
     * @return updated destination FlowFile (new size, etc...)
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws IllegalArgumentException if the given destination is contained
     * within the sources
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws MissingFlowFileException if the given FlowFile content cannot be
     * found. The FlowFile should no longer be reference, will be internally
     * destroyed, and the session is automatically rolled back and what is left
     * of the FlowFile is destroyed.
     * @throws FlowFileAccessException if some IO problem occurs accessing
     * FlowFile content. The state of the destination will be as it was prior to
     * this call.
     */
    FlowFile merge(Collection<FlowFile> sources, FlowFile destination, byte[] header, byte[] footer, byte[] demarcator);

    /**
     * Executes the given callback against the content corresponding to the
     * given FlowFile.
     *
     * <i>Note</i>: The OutputStream provided to the given OutputStreamCallback
     * will not be accessible once this method has completed its execution.
     *
     * @param source to write to
     * @param writer used to write new content
     * @return updated FlowFile
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
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
    FlowFile write(FlowFile source, OutputStreamCallback writer) throws FlowFileAccessException;

    /**
     * Executes the given callback against the content corresponding to the
     * given flow file.
     *
     * <i>Note</i>: The InputStream & OutputStream provided to the given
     * StreamCallback will not be accessible once this method has completed its
     * execution.
     *
     * @param source to read from and write to
     * @param writer used to read the old content and write new content
     * @return updated FlowFile
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws MissingFlowFileException if the given FlowFile content cannot be
     * found. The FlowFile should no longer be reference, will be internally
     * destroyed, and the session is automatically rolled back and what is left
     * of the FlowFile is destroyed.
     * @throws FlowFileAccessException if some IO problem occurs accessing
     * FlowFile content; if an attempt is made to access the InputStream or
     * OutputStream provided to the given StreamCallback after this method
     * completed its execution
     */
    FlowFile write(FlowFile source, StreamCallback writer) throws FlowFileAccessException;

    /**
     * Executes the given callback against the content corresponding to the
     * given FlowFile, such that any data written to the OutputStream of the
     * content will be appended to the end of FlowFile.
     *
     * <i>Note</i>: The OutputStream provided to the given OutputStreamCallback
     * will not be accessible once this method has completed its execution.
     *
     * @param source the flowfile for which content should be appended
     * @param writer used to write new bytes to the flowfile content
     * @return the updated flowfile reference for the new content
     * @throws FlowFileAccessException if an attempt is made to access the
     * OutputStream provided to the given OutputStreamCallaback after this
     * method completed its execution
     */
    FlowFile append(FlowFile source, OutputStreamCallback writer) throws FlowFileAccessException;

    /**
     * Writes to the given FlowFile all content from the given content path.
     *
     * @param source the file from which content will be obtained
     * @param keepSourceFile if true the content is simply copied; if false the
     * original content might be used in a destructive way for efficiency such
     * that the repository will have the data but the original data will be
     * gone. If false the source object will be removed or gone once imported.
     * It will not be restored if the session is rolled back so this must be
     * used with caution. In some cases it can result in tremendous efficiency
     * gains but is also dangerous.
     * @param destination the FlowFile whose content will be updated
     * @return the updated destination FlowFile (new size)
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws MissingFlowFileException if the given FlowFile content cannot be
     * found. The FlowFile should no longer be reference, will be internally
     * destroyed, and the session is automatically rolled back and what is left
     * of the FlowFile is destroyed.
     * @throws FlowFileAccessException if some IO problem occurs accessing
     * FlowFile content
     */
    FlowFile importFrom(Path source, boolean keepSourceFile, FlowFile destination);

    /**
     * Writes to the given FlowFile all content from the given content path.
     *
     * @param source the file from which content will be obtained
     * @param destination the FlowFile whose content will be updated
     * @return the updated destination FlowFile (new size)
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws MissingFlowFileException if the given FlowFile content cannot be
     * found. The FlowFile should no longer be reference, will be internally
     * destroyed, and the session is automatically rolled back and what is left
     * of the FlowFile is destroyed.
     * @throws FlowFileAccessException if some IO problem occurs accessing
     * FlowFile content
     */
    FlowFile importFrom(InputStream source, FlowFile destination);

    /**
     * Writes the content of the given FlowFile to the given destination path.
     *
     * @param flowFile to export the content of
     * @param destination to export the content to
     * @param append if true will append to the current content at the given
     * path; if false will replace any current content
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws MissingFlowFileException if the given FlowFile content cannot be
     * found. The FlowFile should no longer be reference, will be internally
     * destroyed, and the session is automatically rolled back and what is left
     * of the FlowFile is destroyed.
     * @throws FlowFileAccessException if some IO problem occurs accessing
     * FlowFile content
     */
    void exportTo(FlowFile flowFile, Path destination, boolean append);

    /**
     * Writes the content of the given FlowFile to the given destination stream
     *
     * @param flowFile to export the content of
     * @param destination to export the content to
     * @throws IllegalStateException if detected that this method is being
     * called from within a callback of another method in this session and for
     * the given FlowFile(s)
     * @throws FlowFileHandlingException if the given FlowFile is already
     * transferred or removed or doesn't belong to this session. Automatic
     * rollback will occur.
     * @throws MissingFlowFileException if the given FlowFile content cannot be
     * found. The FlowFile should no longer be reference, will be internally
     * destroyed, and the session is automatically rolled back and what is left
     * of the FlowFile is destroyed.
     * @throws FlowFileAccessException if some IO problem occurs accessing
     * FlowFile content
     */
    void exportTo(FlowFile flowFile, OutputStream destination);

    /**
     * Returns a ProvenanceReporter that is tied to this ProcessSession.
     *
     * @return the provenance reporter
     */
    ProvenanceReporter getProvenanceReporter();
}
