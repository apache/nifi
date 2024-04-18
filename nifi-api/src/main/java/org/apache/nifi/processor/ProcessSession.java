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
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
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
import org.apache.nifi.provenance.ProvenanceEventType;

/**
 * A process session encompasses all the behaviors a processor can perform to
 * obtain, clone, read, modify remove {@link FlowFile}s in an atomic unit.
 * A process session is always tied to a single {@link Processor} at any one time
 * and ensures no FlowFile can ever be accessed by any more than one processor at a given time.
 * The session also ensures that all FlowFiles are always accounted for.
 * The creator of a process session is always required to manage the session.
 * <p>
 * A session is not considered thread safe. The session supports a unit of work that is either committed or rolled back.
 * <p>
 * As noted on specific methods and for specific exceptions
 * automated rollback will occur to ensure consistency of the repository.
 * However, several situations can result in exceptions yet not cause automated rollback.
 * In these cases the consistency of the repository will be retained
 * but callers will be able to indicate whether it should result in rollback or continue on toward a commit.
 * <p>
 * A process session has two types of 'terminal' methods that will result in the session being in a 'fresh' state,
 * containing no knowledge or any FlowFile, as if the session were newly created.
 * After one of these methods is called, the instance may be used again.
 * The terminal methods for a process session are  {@link #commitAsync()} and {@link #rollback()} (and their overloads).
 * <p>
 * Additionally, the {@link #migrate(ProcessSession, Collection)} method transfers all knowledge of the provided FlowFiles
 * to the other process session, as if the FlowFiles never existed in this process session.
 * Note, however, that even if all FlowFiles are migrated via the {@link #migrate(ProcessSession, Collection)} method,
 * the session is not necessarily entirely cleared, as it still may have knowledge of counter adjustments or state changes,
 * e.g. see methods {@link #adjustCounter(String, long, boolean)} or {@link #setState(Map, Scope)}.
 * A commit or rollback will clear these changes as well.
 */
public interface ProcessSession {

    /**
     * Commits the current session ensuring all operations against {@link FlowFile}s within this session are atomically persisted.
     * All FlowFiles operated on within this session must be accounted for by transfer or removal or the commit will fail.
     * <p>
     * As soon as the commit completes the session is again ready to be used.
     *
     * @deprecated As of NiFi 1.14.0, replaced by {@link #commitAsync()}, {@link #commitAsync(Runnable)}, and {@link #commitAsync(Runnable, Consumer)}.
     *              The synchronous API is less suited for execution in different runtimes, e.g. MiNiFi or Stateless NiFi,
     *              and may cause the dataflow in such runtimes to get stuck.
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}).
     * @throws FlowFileHandlingException if not all {@link FlowFile}s acted upon within this session are accounted for
     *              such that they have a transfer identified or where marked for removal. Automated rollback occurs.
     * @throws ProcessException if some general fault occurs while persisting the session.
     *              Initiates automatic rollback. The root cause can be obtained via {@link Exception#getCause}.
     */
    void commit();

    /**
     * Commits the current session ensuring all operations against {@link FlowFile}s within this session are atomically persisted.
     * All FlowFiles operated on within this session must be accounted for by transfer or removal or the commit will fail.
     * <p>
     * Unlike the {@link #commit()} method, the persistence of data to the repositories
     * is not guaranteed to have occurred by the time that this method returns.
     * Therefore, if any follow-on actions are necessary after the data has been persisted to the repository
     * (for example, acknowledging receipt from a source system, removing a source file, etc.) that logic
     * should be performed only by invoking {@link #commitAsync(Runnable)} or {@link #commitAsync(Runnable, Consumer)}
     * and implementing that action in the provided callback.
     * <p>
     * As a result, the following very common idiom:
     * <pre><code>
     * getDataFromSource();
     * session.commit();
     * acknowledgeReceiptOfData();
     * </code></pre>
     * Cannot be simply changed to:
     * <pre><code>
     * getDataFromSource();
     * session.commitAsync();
     * acknowledgeReceiptOfData();
     * </code></pre>
     * Doing so could result in acknowledging receipt of data from the source system before data has been committed to the repositories.
     * If NiFi were to then be restarted, there is potential for data loss.
     * Rather, the following idiom should take its place to ensure that there is no data loss:
     * <pre><code>
     * getDataFromSource();
     * session.commitAsync( () -> acknowledgeReceiptOfData() );
     * </code></pre>
     * <p>
     * If the session cannot be committed, an error will be logged and the session will be rolled back instead.
     *
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}).
     * @throws FlowFileHandlingException if not all {@link FlowFile}s acted upon within this session are accounted for
     *              such that they have a transfer identified or where marked for removal. Automated rollback occurs.
     */
    void commitAsync();

    /**
     * Commits the current session ensuring all operations against {@link FlowFile}s within this session are atomically persisted.
     * All FlowFiles operated on within this session must be accounted for by transfer or removal or the commit will fail.
     * <p>
     * If the session is successfully committed, the given {@code onSuccess} {@link Runnable} will be called.
     * At the point that the session commit is completed, any calls to {@link #rollback()} / {@link #rollback(boolean)}
     * will not undo that session commit but instead roll back any changes that may have occurred since.
     * <p>
     * If, for any reason, the session could not be committed, an error-level log message will be generated,
     * but the caller will not have a chance to perform any cleanup logic.
     * If such logic is necessary, use {@link #commitAsync(Runnable, Consumer)} instead.
     * <p>
     * Unlike the {@link #commit()} method, the persistence of data to the repositories
     * is not guaranteed to have occurred by the time that this method returns.
     *
     * @param onSuccess {@link Runnable} that will be called if and when the session is successfully committed; may be null
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}).
     * @throws FlowFileHandlingException if not all {@link FlowFile}s acted upon within this session are accounted for
     *              such that they have a transfer identified or where marked for removal. Automated rollback occurs.
     */
    default void commitAsync(Runnable onSuccess) {
        commitAsync(onSuccess, null);
    }

    /**
     * Commits the current session ensuring all operations against FlowFiles within this session are atomically persisted.
     * All FlowFiles operated on within this session must be accounted for by transfer or removal or the commit will fail.
     * <p>
     * If the session is successfully committed, the given {@code onSuccess} {@link Runnable} will be called.
     * At the point that the session commit is completed, any calls to {@link #rollback()} / {@link #rollback(boolean)}
     * will not undo that session commit but instead roll back any changes that may have occurred since.
     * <p>
     * If, for any reason, the session could not be committed, the given {@code onFailure} {@link Consumer} will be called
     * instead of the {@code onSuccess} {@link Runnable}.
     * The Consumer will be provided the Throwable that prevented the session commit from completing.
     * <p>
     * Unlike the {@link #commit()} method, the persistence of data to the repositories
     * is not guaranteed to have occurred by the time that this method returns.
     *
     * @param onSuccess {@link Runnable} that will be called if and when the session is successfully committed; may be null
     * @param onFailure {@link Consumer} that will be called if, for any reason, the session could not be committed; may be null
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}).
     * @throws FlowFileHandlingException if not all {@link FlowFile}s acted upon within this session are accounted for
     *              such that they have a transfer identified or where marked for removal. Automated rollback occurs.
     */
    void commitAsync(Runnable onSuccess, Consumer<Throwable> onFailure);

    /**
     * Reverts any changes made during this session.
     * All {@link FlowFile}s are restored back to their initial session state and back to their original queues.
     * If no changes were made since this session was last committed or rolled back, then this method has no effect.
     * This method can be called any number of times.
     * Calling this method is identical to calling {@link #rollback(boolean)} passing {@code false} as the parameter.
     */
    void rollback();

    /**
     * Reverts any changes made during this session.
     * All {@link FlowFile}s are restored back to their initial session state and back to their original queues,
     * after optionally being penalized.
     * If no changes were made since this session was last committed or rolled back, then this method has no effect.
     * This method can be called any number of times.
     *
     * @param penalize whether the {@link FlowFile}s that are being restored back to their queues should be penalized
     */
    void rollback(boolean penalize);

    /**
     * Migrates ownership of the given {@code flowFiles} {@link FlowFile}s from {@code this} session to the given {@code newOwner} {@link ProcessSession}.
     * <p>
     * Note, that for any provided FlowFile, if the FlowFile has any child (e.g., by calling {@link #create(FlowFile)}
     * and passing the FlowFile as the argument), then all children that were created must also be in the Collection of provided FlowFiles.
     * <p>
     * Also note, that if any FlowFile given is not the most up-to-date version of that FlowFile,
     * then the most up-to-date version of the FlowFile will be migrated to the new owner.
     * For example, if a call to {@link #putAttribute(FlowFile, String, String)} is made,
     * passing {@code flowFile1} as the FlowFile, and then {@code flowFile1} is passed to this method,
     * then the newest version (including the newly added attribute) will be migrated,
     * not the outdated version of the FlowFile that {@code flowFile1} points to.
     *
     * @param newOwner the {@link ProcessSession} that is to become the new owner of the given {@link FlowFile}s
     * @param flowFiles the {@link FlowFile}s to migrate
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for any of the affected {@link FlowFile}s
     */
    void migrate(ProcessSession newOwner, Collection<FlowFile> flowFiles);

    /**
     * Migrates ownership of all {@link FlowFile}s from {@code this} session to the given {@code newOwner} {@link ProcessSession}.
     * Calling this method is identical to calling {@link #migrate(ProcessSession, Collection)}
     * passing all FlowFiles owned by this session as the parameter,
     * this encompasses both FlowFiles retrieved from the work queue and newly created or cloned ones.
     *
     * @param newOwner the {@link ProcessSession} that is to become the new owner of all {@link FlowFile}s
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for any of the affected {@link FlowFile}s
     */
    void migrate(ProcessSession newOwner);

    /**
     * Adjusts counter data for the given counter name and takes care of registering the counter if not already present.
     * The adjustment occurs only if and when the process session is committed.
     *
     * @param name the name of the counter
     * @param delta the delta by which to modify the counter (+ or -)
     * @param immediate if true, the counter will be updated immediately, without regard to whether the session is committed or rolled back;
     *            otherwise, the counter will be incremented only if and when the session is committed.
     */
    void adjustCounter(String name, long delta, boolean immediate);

    /**
     * Returns the {@link FlowFile} from the work queue that is next highest priority to process.
     * If no FlowFiles are available, returns {@code null}.
     *
     * @return the {@link FlowFile} from the work queue that is next highest priority to process or {@code null}, if none available
     */
    FlowFile get();

    /**
     * Returns the next up to {@code maxResults} {@link FlowFile}s from the work queue that are the highest priority to process.
     * If no FlowFiles are available, returns an empty list. Will not return {@code null}.
     * <p>
     * If multiple incoming queues are present, the behavior is unspecified in terms of
     * whether all queues or only a single queue will be polled in a single call.
     *
     * @param maxResults the maximum number of {@link FlowFile}s to return
     * @return up to {@code maxResults} {@link FlowFile}s from the work queue
     * @throws IllegalArgumentException if {@code maxResults} is less than 0
     */
    List<FlowFile> get(int maxResults);

    /**
     * Returns all {@link FlowFile}s from all the incoming queues which the given {@link FlowFileFilter} accepts.
     * <p>
     * Calls to this method provide exclusive access to the underlying queues.
     * That is, no other thread will be permitted to pull FlowFiles from or add FlowFiles
     * to this {@link Processor}'s incoming queues until this method call has returned.
     *
     * @param filter a {@link FlowFileFilter} to limit which {@link FlowFile}s are returned
     * @return all {@link FlowFile}s from all the incoming queues which the given {@link FlowFileFilter} {@code filter} accepts.
     */
    List<FlowFile> get(FlowFileFilter filter);

    /**
     * Returns the {@link QueueSize} that represents the number of {@link FlowFile}s and their combined data size
     * for all FlowFiles waiting to be processed by the {@link Processor} that owns {@code this} {@link ProcessSession},
     * regardless of which connection the FlowFiles live on.
     *
     * @return the number of {@link FlowFile}s and their combined data size in the work queue
     */
    QueueSize getQueueSize();

    /**
     * Creates a new {@link FlowFile} in the repository with no content and without any linkage to a parent FlowFile.
     * <p>
     * This method is appropriate only when data is received or created from an external system.
     * Otherwise, this method should be avoided and instead {@link #create(FlowFile)} or {@link #create(Collection)} be used.
     * <p>
     * When this method is used, a {@link ProvenanceEventType#CREATE} or {@link ProvenanceEventType#RECEIVE} event should be generated.
     * See the {@link #getProvenanceReporter()} method and {@link ProvenanceReporter} class for more information.
     *
     * @return newly created FlowFile
     */
    FlowFile create();

    /**
     * Creates a new {@link FlowFile} in the repository with no content but with a parent linkage to the {@code parent}.
     * The newly created FlowFile will inherit all the parent's attributes, except for the UUID.
     * <p>
     * This method will automatically generate a {@link ProvenanceEventType#FORK} or a {@link ProvenanceEventType#JOIN} event,
     * depending on whether other FlowFiles are generated from the same parent before the session is committed.
     *
     * @param parent to base the new {@link FlowFile} on, inheriting attributes from
     * @return newly created {@link FlowFile}
     */
    FlowFile create(FlowFile parent);

    /**
     * Creates a new {@link FlowFile} in the repository with no content but with a parent linkage to all {@code parents}.
     * The newly created FlowFile will inherit all the attributes that are in common to all parents, except for the UUID.
     * <p>
     * This method will automatically generate a {@link ProvenanceEventType#JOIN} event.
     *
     * @param parents to base the new {@link FlowFile} on, inheriting shared attributes from
     * @return newly created {@link FlowFile}
     */
    FlowFile create(Collection<FlowFile> parents);

    /**
     * Creates a new {@link FlowFile} with a parent linkage to the {@code example} FlowFile.
     * It is a clone of the given FlowFile as of the time this is called, both in attributes and content.
     * <p>
     * This method will automatically generate a {@link ProvenanceEventType#CLONE} event.
     *
     * @param example {@link FlowFile} to be the source of cloning - given FlowFile must be a part of the given session
     * @return {@link FlowFile} that is a clone of the given {@code example} FlowFile
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the {@code example} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if the given {@link FlowFile} content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content
     */
    FlowFile clone(FlowFile example);

    /**
     * Creates a new {@link FlowFile} with a parent linkage to the {@code parent} FlowFile.
     * It is a clone of the given FlowFile as of the time this is called, both in attributes and a subset of the content.
     * The content of the new FlowFile will be a subset of the byte sequence of the given FlowFile,
     * starting at the specified offset and with the length specified.
     * <p>
     * This method will automatically generate a {@link ProvenanceEventType#FORK} or a {@link ProvenanceEventType#CLONE} event,
     * if the offset is 0 and the size is exactly equal to the size of the example FlowFile.
     *
     * @param parent {@link FlowFile} to be the source of cloning - given FlowFile must be a part of the given session
     * @param offset of the parent {@link FlowFile}'s content to base the cloned FlowFile's content on
     * @param size in bytes of the parent {@link FlowFile}'s content to clone starting from the {@code offset}
     * @return {@link FlowFile} that is a partial clone of the given {@code parent} FlowFile whose content has the specified {@code size}
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the {@code parent} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Or if the specified {@code offset} + {@code size} exceeds the size of the {@code parent} FlowFile's content.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if the given {@link FlowFile} content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content
     */
    FlowFile clone(FlowFile parent, long offset, long size);

    /**
     * Sets a penalty for the given {@link FlowFile},
     * which will make it unavailable to be operated on any further during the penalty period.
     *
     * @param flowFile to penalize
     * @return the penalized {@link FlowFile}
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the {@code flowFile} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     */
    FlowFile penalize(FlowFile flowFile);

    /**
     * Updates the given {@link FlowFile}'s attributes with the given {@code key} / {@code value} pair.
     * <p>
     * If the {@code key} is named {@code uuid}, this attribute will be ignored.
     *
     * @param flowFile to update
     * @param key of attribute to add or modify
     * @param value of attribute to add or modify
     * @return the updated {@link FlowFile} with the attribute added or modified
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the {@code flowFile} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     */
    FlowFile putAttribute(FlowFile flowFile, String key, String value);

    /**
     * Updates the given {@link FlowFile}'s attributes with the given {@code key} / {@code value} pairs.
     * <p>
     * If the map contains a key named {@code uuid}, this attribute will be ignored.
     *
     * @param flowFile to update
     * @param attributes the attributes to add or modify
     * @return the updated {@link FlowFile} with the attributes added or modified
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the {@code flowFile} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     */
    FlowFile putAllAttributes(FlowFile flowFile, Map<String, String> attributes);

    /**
     * Removes the attribute with the given {@code key} from the given {@link FlowFile}.
     * <p>
     * If the {@code key} is named {@code uuid}, this method will return the same FlowFile without removing any attribute.
     *
     * @param flowFile to update
     * @param key of attribute to remove
     * @return the updated {@link FlowFile} with the matching attribute removed
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the {@code flowFile} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     */
    FlowFile removeAttribute(FlowFile flowFile, String key);

    /**
     * Removes the attributes with the given {@code keys} from the given {@link FlowFile}.
     * <p>
     * If the set of keys contains the value {@code uuid}, this key will be ignored.
     *
     * @param flowFile to update
     * @param keys of attributes to remove
     * @return the updated {@link FlowFile} with the matching attributes removed
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the {@code flowFile} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     */
    FlowFile removeAllAttributes(FlowFile flowFile, Set<String> keys);

    /**
     * Removes all attributes from the given {@link FlowFile} whose key matches the given pattern.
     * <p>
     * If the pattern matches the key {@code uuid}, this key will not be removed.
     *
     * @param flowFile to update
     * @param keyPattern pattern to match each {@link FlowFile} attribute against; may be null, in which case no attribute is removed
     * @return the updated {@link FlowFile} with the matching attributes removed
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the {@code flowFile} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     */
    FlowFile removeAllAttributes(FlowFile flowFile, Pattern keyPattern);

    /**
     * Transfers the given {@link FlowFile} back to the work queue from which it was pulled.
     * <p>
     * The processor will not be able to operate on the given FlowFile until this session is committed.
     * Any modifications that have been made to the FlowFile will be maintained.
     * FlowFiles that are created by the processor cannot be transferred back to themselves via this method.
     *
     * @param flowFile to transfer
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the {@code flowFile} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws IllegalArgumentException if the given {@link FlowFile} was created by this processor
     */
    void transfer(FlowFile flowFile);

    /**
     * Transfers the given {@link FlowFile}s back to the work queues from which the FlowFiles were pulled.
     * <p>
     * The processor will not be able to operate on the given FlowFiles until this session is committed.
     * Any modifications that have been made to the FlowFiles will be maintained.
     * FlowFiles that are created by the processor cannot be transferred back to themselves via this method.
     *
     * @param flowFiles to transfer
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the {@code flowFile} {@link FlowFile}
     * @throws FlowFileHandlingException if any of the given {@link FlowFile}s is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws IllegalArgumentException if any of the given {@link FlowFile}s was created by this processor
     */
    void transfer(Collection<FlowFile> flowFiles);

    /**
     * Transfers the given {@link FlowFile} to the appropriate destination processor work queue(s) based on the given {@code relationship}.
     * <p>
     * If the relationship leads to more than one destination the state of the FlowFile is replicated
     * such that each destination receives an exact copy of the FlowFile though each will have its own unique identity.
     * The destination processors will not be able to operate on the given FlowFile until this session is committed or
     * until the ownership of the session is migrated to another processor.
     * If ownership of the session is passed to a destination processor then that destination processor will have immediate visibility
     * of the transferred FlowFiles within the session.
     *
     * @param flowFile to transfer
     * @param relationship to transfer to
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the {@code flowFile} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws IllegalArgumentException if given relationship is not a known or registered relationship
     */
    void transfer(FlowFile flowFile, Relationship relationship);

    /**
     * Transfers the given {@link FlowFile}s to the appropriate destination processor work queue(s) based on the given {@code relationship}.
     * <p>
     * If the relationship leads to more than one destination the state of each FlowFile is replicated
     * such that each destination receives an exact copy of the FlowFile though each will have its own unique identity.
     * The destination processors will not be able to operate on the given FlowFiles until this session is committed or
     * until the ownership of the session is migrated to another processor.
     * If ownership of the session is passed to a destination processor then that destination processor will have immediate visibility
     * of the transferred FlowFiles within the session.
     *
     * @param flowFiles to transfer
     * @param relationship to transfer to
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for any of the {@code flowFiles} {@link FlowFile}s
     * @throws FlowFileHandlingException if any of the given {@link FlowFile}s is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws IllegalArgumentException if given relationship is not a known or registered relationship
     */
    void transfer(Collection<FlowFile> flowFiles, Relationship relationship);

    /**
     * Ends the managed persistence for the given {@link FlowFile}.
     * <p>
     * The persistent attributes for the FlowFile are deleted and so is the content assuming nothing else references it.
     * This FlowFile will no longer be available for further operation.
     *
     * @param flowFile to remove
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the given {@code flowFile} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     */
    void remove(FlowFile flowFile);

    /**
     * Ends the managed persistence for the given {@link FlowFile}s.
     * <p>
     * The persistent attributes for the FlowFiles are deleted and so is the content assuming nothing else references it.
     * The FlowFiles will no longer be available for further operation.
     *
     * @param flowFiles to remove
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for any of the given {@code flowFiles} {@link FlowFile}s
     * @throws FlowFileHandlingException if any of the given {@link FlowFile}s is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     */
    void remove(Collection<FlowFile> flowFiles);

    /**
     * Executes the given {code reader} {@link InputStreamCallback} against the content of the given {@link FlowFile}.
     *
     * @param source the {@link FlowFile} to retrieve the content from
     * @param reader {@link InputStreamCallback} that will be called to read the {@link FlowFile} content
     * @throws IllegalStateException if detected that this method is being called from within a write callback
     *              (see {@link #write(FlowFile, StreamCallback)}, {@link #write(FlowFile, OutputStreamCallback)})
     *              or while a write stream is open (see {@link #write(FlowFile)}) for the given {@code source} {@link FlowFile}.
     *              Said another way, it is not permissible to call this method while writing to the same FlowFile.
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if the given {@link FlowFile} content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content;
     *              if an attempt is made to access the {@link InputStream} provided to the given {@link InputStreamCallback}
     *              after this method completed its execution
     */
    void read(FlowFile source, InputStreamCallback reader) throws FlowFileAccessException;

    /**
     * Provides an {@link InputStream} that can be used to read the content of the given {@link FlowFile}.
     * <p>
     * This method differs from those that make use of callbacks in that this method returns an InputStream and expects the caller
     * to properly handle the lifecycle of the InputStream (i.e., the caller is responsible for ensuring that the InputStream is closed appropriately).
     * The session may or may not handle closing the stream when the session is commited or rolled back,
     * but the responsibility of doing so belongs to the caller.
     *
     * @param flowFile the {@link FlowFile} to retrieve the content from
     * @return an {@link InputStream} that can be used to read the content of the {@link FlowFile}
     * @throws IllegalStateException if detected that this method is being called from within a write callback
     *              (see {@link #write(FlowFile, StreamCallback)}, {@link #write(FlowFile, OutputStreamCallback)})
     *              or while a write stream is open (see {@link #write(FlowFile)}) for the given {@code flowFile} {@link FlowFile}.
     *              Said another way, it is not permissible to call this method while writing to the same FlowFile.
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if the given {@link FlowFile} content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content;
     *              if an attempt is made to read from the stream after the session is committed or rolled back.
     */
    InputStream read(FlowFile flowFile);

    /**
     * Combines the content of all given {@code sources} {@link FlowFile}s into a single given destination FlowFile.
     *
     * @param sources the {@link FlowFile}s whose content to merge
     * @param destination the {@link FlowFile} to use as the merged result
     * @return the updated {@code destination} destination {@link FlowFile} with changed content
     * @throws IllegalArgumentException if the given destination is contained within the sources
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for any of the given {@code sources} and {@code destination} {@link FlowFile}s
     * @throws FlowFileHandlingException if any of the given {@link FlowFile}s is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if any of the given {@link FlowFile}'s content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content;
     *              the state of the {@code destination} {@link FlowFile} will be as it was prior to this call.
     */
    FlowFile merge(Collection<FlowFile> sources, FlowFile destination);

    /**
     * Combines the content of all given {@code sources} {@link FlowFile}s into a single given destination FlowFile.
     *
     * @param sources the {@link FlowFile}s whose content to merge
     * @param destination the {@link FlowFile} to use as the merged result
     * @param header bytes that will be added to the beginning of the merged output; may be null or empty
     * @param footer bytes that will be added to the end of the merged output; may be null or empty
     * @param demarcator bytes that will be placed in between each object merged together; may be null or empty
     * @return the updated {@code destination} {@link FlowFile} with changed content
     * @throws IllegalArgumentException if the given destination is contained within the sources
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for any of the given {@code sources} and {@code destination} {@link FlowFile}s
     * @throws FlowFileHandlingException if any of the given {@link FlowFile}s is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if any of the given {@link FlowFile}'s content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content;
     *              the state of the {@code destination} {@link FlowFile} will be as it was prior to this call.
     */
    FlowFile merge(Collection<FlowFile> sources, FlowFile destination, byte[] header, byte[] footer, byte[] demarcator);

    /**
     * Executes the given {code writer} {@link OutputStreamCallback} against the content of the given {@link FlowFile}.
     *
     * @param source the {@link FlowFile} to write the content of
     * @param writer {@link InputStreamCallback} that will be called to write the {@link FlowFile} content
     * @return the updated {@code source} {@link FlowFile} with changed content
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the given {@code source} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if the given {@link FlowFile} content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content;
     *              if an attempt is made to access the {@link OutputStream} provided to the given {@link OutputStreamCallback}
     *              after this method completed its execution
     */
    FlowFile write(FlowFile source, OutputStreamCallback writer) throws FlowFileAccessException;

    /**
     * Provides an {@link OutputStream} that can be used to write the content of the given {@link FlowFile}.
     * <p>
     * This method differs from those that make use of callbacks in that this method returns an OutputStream and expects the caller
     * to properly handle the lifecycle of the OutputStream (i.e., the caller is responsible for ensuring that the OutputStream is closed appropriately).
     * The session may or may not handle closing the stream when the session is commited or rolled back,
     * but the responsibility of doing so belongs to the caller.
     *
     * @param source the {@link FlowFile} to write the content of
     * @return an {@link OutputStream} that can be used to write the content of the {@link FlowFile}
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the given {@code source} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if the given {@link FlowFile} content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content;
     *              if an attempt is made to write to the stream after the session is committed or rolled back.
     */
    OutputStream write(FlowFile source);

    /**
     * Executes the given {code writer} {@link StreamCallback} against the content of the given {@link FlowFile}.
     *
     * @param source the {@link FlowFile} to read and write the content of
     * @param writer {@link StreamCallback} that will be called to read and write the {@link FlowFile} content
     * @return the updated {@code source} {@link FlowFile} with changed content
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the given {@code source} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if the given {@link FlowFile} content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content;
     *              if an attempt is made to access the {@link InputStream} or {@link OutputStream}
     *              provided to the given {@link StreamCallback} after this method completed its execution
     */
    FlowFile write(FlowFile source, StreamCallback writer) throws FlowFileAccessException;

    /**
     * Executes the given {code writer} {@link OutputStreamCallback} against the content of the given {@link FlowFile},
     * such that any data written to the OutputStream will be appended to the end of FlowFile's content.
     *
     * @param source the {@link FlowFile} to extend the content of
     * @param writer {@link OutputStreamCallback} that will be called to append the {@link FlowFile}'s content
     * @return the updated {@code source} {@link FlowFile} with changed content
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the given {@code source} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if the given {@link FlowFile} content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content;
     *              if an attempt is made to access the {@link OutputStream} provided to the given {@link OutputStreamCallback}
     *              after this method completed its execution
     */
    FlowFile append(FlowFile source, OutputStreamCallback writer) throws FlowFileAccessException;

    /**
     * Writes to contents of the file a the {@code source} {@link Path} to the given {@link FlowFile}'s content.
     *
     * @param source the {@link Path} to the file from which content will be obtained
     * @param keepSourceFile if true the content is simply copied;
     *              if false the original content might be used in a destructive way for efficiency,
     *              such that the repository will have the data but the original data will be gone.
     *              If false the source object will be removed or gone once imported.
     *              It will not be restored if the session is rolled back so this must be used with caution.
     *              In some cases it can result in tremendous efficiency gains but is also dangerous.
     * @param destination the {@link FlowFile} whose content will be updated
     * @return the updated {@code destination} {@link FlowFile} with changed content
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the given {@code source} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if the given {@link FlowFile} content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content
     */
    FlowFile importFrom(Path source, boolean keepSourceFile, FlowFile destination);

    /**
     * Writes to contents of the {@code source} {@link InputStream} to the given {@link FlowFile}'s content.
     *
     * @param source the {@link InputStream} from which content will be obtained
     * @param destination the {@link FlowFile} whose content will be updated
     * @return the updated {@code destination} {@link FlowFile} with changed content
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the given {@code source} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if the given {@link FlowFile} content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content
     */
    FlowFile importFrom(InputStream source, FlowFile destination);

    /**
     * Writes the content of the given {@link FlowFile} to the file at the given {@code destination} {@link Path}.
     *
     * @param flowFile the {@link FlowFile} to export the content of
     * @param destination the {@link Path} to a file to export the {@link FlowFile}'s content to
     * @param append if true will append to the current content of the file at the given path;
     *              if false will replace any current content
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the given {@code flowFile} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if the given {@link FlowFile} content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content
     */
    void exportTo(FlowFile flowFile, Path destination, boolean append);

    /**
     * Writes the content of the given {@link FlowFile} to given {@code destination} {@link OutputStream}.
     *
     * @param flowFile the {@link FlowFile} to export the content of
     * @param destination the {@link OutputStream} to export the {@link FlowFile}'s content to
     * @throws IllegalStateException if detected that this method is being called from within a read or write callback
     *              (see {@link #read(FlowFile, InputStreamCallback)}, {@link #write(FlowFile, StreamCallback)},
     *              {@link #write(FlowFile, OutputStreamCallback)}) or while a read or write stream is open
     *              (see {@link #read(FlowFile)}, {@link #write(FlowFile)}) for the given {@code flowFile} {@link FlowFile}
     * @throws FlowFileHandlingException if the given {@link FlowFile} is already transferred or removed or doesn't belong to this session.
     *              Automatic rollback will occur.
     * @throws MissingFlowFileException if the given {@link FlowFile} content cannot be found.
     *              The FlowFile should no longer be referenced, will be internally destroyed. The session is automatically rolled back.
     * @throws FlowFileAccessException if some IO problem occurs accessing {@link FlowFile} content
     */
    void exportTo(FlowFile flowFile, OutputStream destination);

    /**
     * Returns the {@link ProvenanceReporter} that is tied to {@code this} {@link ProcessSession}.
     *
     * @return the {@link ProvenanceReporter} that is tied to {@code this} {@link ProcessSession}
     */
    ProvenanceReporter getProvenanceReporter();

    /**
     * Updates the value of the component's state, setting it to given value.
     * <p>
     * This method does update the remote State Provider immediately but rather caches the value until the session is committed.
     * At that point, it will publish the state to the remote State Provider, if the state is the latest according to the remote State Provider.
     *
     * @param state the value to change the state to
     * @param scope the {@link Scope} to use when storing the state
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    void setState(Map<String, String> state, Scope scope) throws IOException;

    /**
     * Returns the current state for the component.
     * <p>
     * This return value will never be {@code null}.
     * If the state has not yet been set, the StateMap's version will be -1, and the map of values will be empty.
     *
     * @param scope the {@link Scope} to use when fetching the state
     * @return the current state for the component
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    StateMap getState(Scope scope) throws IOException;

    /**
     * Updates the value of the component's state, setting it to given {@code newValue},
     * if and only if the current value is the same as the given {@code oldValue}.
     * <p>
     * The oldValue will be compared against the value of the state as it is known to {@code this} {@link ProcessSession}.
     * If the Process Session does not currently know the state, it will be fetched from the StateProvider.
     * <p>
     * This method does update the remote State Provider immediately but rather caches the value until the session is committed.
     * At that point, it will publish the state to the remote State Provider, if the state is the latest according to the remote State Provider.
     *
     * @param oldValue the value to compare the state's current value against
     * @param newValue the new value to use if and only if the state's current value is the same as the given {@code oldValue}
     * @param scope the {@link Scope} to use for fetching the current and storing the new state
     * @return {@code true} if the state was updated to the {@code newValue},
     *              {@code false} if the state's current value was not equal to {@code oldValue}
     * @throws IOException if unable to communicate with the underlying storage mechanism
     */
    boolean replaceState(StateMap oldValue, Map<String, String> newValue, Scope scope) throws IOException;

    /**
     * Clears all keys and values from the component's state.
     * <p>
     * This method does update the remote State Provider immediately but rather caches the value until the session is committed.
     * At that point, it will publish the state to the remote State Provider, if the state is the latest according to the remote State Provider.
     *
     * @param scope the {@link Scope} to use for clearing the state
     * @throws IOException if unable to communicate with the underlying storage mechanism.
     */
    void clearState(Scope scope) throws IOException;
}
