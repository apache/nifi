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
package org.apache.nifi.provenance;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;

import java.util.Collection;

/**
 * ProvenanceReporter generates and records Provenance-related events. A
 * ProvenanceReporter is always tied to a {@link ProcessSession}. Any events
 * that are generated are reported to Provenance only after the session has been
 * committed. If the session is rolled back, the events related to that session
 * are purged.
 */
public interface ProvenanceReporter {

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#RECEIVE RECEIVE} that indicates that the given
     * FlowFile was created from data received from an external source.
     *
     * @param flowFile the FlowFile that was received
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred. The intent of this
     * field is such that both the sender and the receiver can publish the
     * events to an external Enterprise-wide system that is then able to
     * correlate the SEND and RECEIVE events.
     */
    void receive(FlowFile flowFile, String transitUri);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#RECEIVE RECEIVE} that indicates that the given
     * FlowFile was created from data received from the specified URI and that
     * the source system used the specified identifier (a URI with namespace) to
     * refer to the data.
     *
     * @param flowFile the FlowFile that was received
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred. The intent of this
     * field is such that both the sender and the receiver can publish the
     * events to an external Enterprise-wide system that is then able to
     * correlate the SEND and RECEIVE events.
     * @param sourceSystemFlowFileIdentifier the URI/identifier that the source
     * system uses to refer to the data; if this value is non-null and is not a
     * URI, the prefix "urn:tdo:" will be used to form a URI.
     */
    void receive(FlowFile flowFile, String transitUri, String sourceSystemFlowFileIdentifier);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#RECEIVE RECEIVE} that indicates that the given
     * FlowFile was created from data received from an external source.
     *
     * @param flowFile the FlowFile that was received
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred. The intent of this
     * field is such that both the sender and the receiver can publish the
     * events to an external Enterprise-wide system that is then able to
     * correlate the SEND and RECEIVE events.
     * @param transmissionMillis the number of milliseconds taken to transfer
     * the data
     */
    void receive(FlowFile flowFile, String transitUri, long transmissionMillis);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#RECEIVE RECEIVE} that indicates that the given
     * FlowFile was created from data received from an external source and
     * provides additional details about the receipt of the FlowFile, such as a
     * remote system's Distinguished Name.
     *
     * @param flowFile the FlowFile that was received
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred. The intent of this
     * field is such that both the sender and the receiver can publish the
     * events to an external Enterprise-wide system that is then able to
     * correlate the SEND and RECEIVE events.
     * @param details details about the receive event; for example, it may be
     * relevant to include the DN of the sending system
     * @param transmissionMillis the number of milliseconds taken to transfer
     * the data
     */
    void receive(FlowFile flowFile, String transitUri, String details, long transmissionMillis);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#RECEIVE RECEIVE} that indicates that the given
     * FlowFile was created from data received from an external source and
     * provides additional details about the receipt of the FlowFile, such as a
     * remote system's Distinguished Name.
     *
     * @param flowFile the FlowFile that was received
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred. The intent of this
     * field is such that both the sender and the receiver can publish the
     * events to an external Enterprise-wide system that is then able to
     * correlate the SEND and RECEIVE events.
     * @param sourceSystemFlowFileIdentifier the URI/identifier that the source
     * system uses to refer to the data; if this value is non-null and is not a
     * URI, the prefix "urn:tdo:" will be used to form a URI.
     * @param details details about the receive event; for example, it may be
     * relevant to include the DN of the sending system
     * @param transmissionMillis the number of milliseconds taken to transfer
     * the data
     */
    void receive(FlowFile flowFile, String transitUri, String sourceSystemFlowFileIdentifier, String details, long transmissionMillis);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#FETCH FETCH} that indicates that the content of the given
     * FlowFile was overwritten with the data received from an external source.
     *
     * @param flowFile the FlowFile whose content was replaced
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred.
     */
    void fetch(FlowFile flowFile, String transitUri);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#FETCH FETCH} that indicates that the content of the given
     * FlowFile was overwritten with the data received from an external source.
     *
     * @param flowFile the FlowFile whose content was replaced
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred.
     * @param transmissionMillis the number of milliseconds taken to transfer the data
     */
    void fetch(FlowFile flowFile, String transitUri, long transmissionMillis);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#FETCH FETCH} that indicates that the content of the given
     * FlowFile was overwritten with the data received from an external source.
     *
     * @param flowFile the FlowFile whose content was replaced
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred.
     * @param details details about the event
     * @param transmissionMillis the number of milliseconds taken to transfer
     * the data
     */
    void fetch(FlowFile flowFile, String transitUri, String details, long transmissionMillis);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#SEND SEND}
     * that indicates that a copy of the given FlowFile was sent to an external
     * destination. The external destination may be a remote system or may be a
     * local destination, such as the local file system but is external to NiFi.
     *
     * @param flowFile the FlowFile that was sent
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred. The intent of this
     * field is such that both the sender and the receiver can publish the
     * events to an external Enterprise-wide system that is then able to
     * correlate the SEND and RECEIVE events.
     */
    void send(FlowFile flowFile, String transitUri);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#SEND SEND}
     * that indicates that a copy of the given FlowFile was sent to an external
     * destination. The external destination may be a remote system or may be a
     * local destination, such as the local file system but is external to NiFi.
     *
     * @param flowFile the FlowFile that was sent
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred. The intent of this
     * field is such that both the sender and the receiver can publish the
     * events to an external Enterprise-wide system that is then able to
     * correlate the SEND and RECEIVE events.
     * @param details additional details related to the SEND event, such as a
     * remote system's Distinguished Name
     */
    void send(FlowFile flowFile, String transitUri, String details);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#SEND SEND}
     * that indicates that a copy of the given FlowFile was sent to an external
     * destination. The external destination may be a remote system or may be a
     * local destination, such as the local file system but is external to NiFi.
     *
     * @param flowFile the FlowFile that was sent
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred. The intent of this
     * field is such that both the sender and the receiver can publish the
     * events to an external Enterprise-wide system that is then able to
     * correlate the SEND and RECEIVE events.
     * @param transmissionMillis the number of milliseconds spent sending the
     * data to the remote system
     */
    void send(FlowFile flowFile, String transitUri, long transmissionMillis);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#SEND SEND}
     * that indicates that a copy of the given FlowFile was sent to an external
     * destination. The external destination may be a remote system or may be a
     * local destination, such as the local file system but is external to NiFi.
     *
     * @param flowFile the FlowFile that was sent
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred. The intent of this
     * field is such that both the sender and the receiver can publish the
     * events to an external Enterprise-wide system that is then able to
     * correlate the SEND and RECEIVE events.
     * @param details additional details related to the SEND event, such as a
     * remote system's Distinguished Name
     * @param transmissionMillis the number of milliseconds spent sending the
     * data to the remote system
     */
    void send(FlowFile flowFile, String transitUri, String details, long transmissionMillis);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#SEND SEND}
     * that indicates that a copy of the given FlowFile was sent to an external
     * destination. The external destination may be a remote system or may be a
     * local destination, such as the local file system but is external to NiFi.
     *
     * @param flowFile the FlowFile that was sent
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred. The intent of this
     * field is such that both the sender and the receiver can publish the
     * events to an external Enterprise-wide system that is then able to
     * correlate the SEND and RECEIVE events.
     * @param force if <code>true</code>, this event will be added to the
     * Provenance Repository immediately and will still be persisted if the
     * {@link org.apache.nifi.processor.ProcessSession ProcessSession} to which this
     * ProvenanceReporter is associated is rolled back. Otherwise, the Event
     * will be recorded only on a successful session commit.
     */
    void send(FlowFile flowFile, String transitUri, boolean force);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#SEND SEND}
     * that indicates that a copy of the given FlowFile was sent to an external
     * destination. The external destination may be a remote system or may be a
     * local destination, such as the local file system but is external to NiFi.
     *
     * @param flowFile the FlowFile that was sent
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred. The intent of this
     * field is such that both the sender and the receiver can publish the
     * events to an external Enterprise-wide system that is then able to
     * correlate the SEND and RECEIVE events.
     * @param details additional details related to the SEND event, such as a
     * remote system's Distinguished Name
     * @param force if <code>true</code>, this event will be added to the
     * Provenance Repository immediately and will still be persisted if the
     * {@link org.apache.nifi.processor.ProcessSession ProcessSession} to which this
     * ProvenanceReporter is associated is rolled back. Otherwise, the Event
     * will be recorded only on a successful session commit.
     */
    void send(FlowFile flowFile, String transitUri, String details, boolean force);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#SEND SEND}
     * that indicates that a copy of the given FlowFile was sent to an external
     * destination. The external destination may be a remote system or may be a
     * local destination, such as the local file system but is external to NiFi.
     *
     * @param flowFile the FlowFile that was sent
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred. The intent of this
     * field is such that both the sender and the receiver can publish the
     * events to an external Enterprise-wide system that is then able to
     * correlate the SEND and RECEIVE events.
     * @param transmissionMillis the number of milliseconds spent sending the
     * data to the remote system
     * @param force if <code>true</code>, this event will be added to the
     * Provenance Repository immediately and will still be persisted if the
     * {@link org.apache.nifi.processor.ProcessSession ProcessSession} to which this
     * ProvenanceReporter is associated is rolled back. Otherwise, the Event
     * will be recorded only on a successful session commit.
     */
    void send(FlowFile flowFile, String transitUri, long transmissionMillis, boolean force);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#SEND SEND}
     * that indicates that a copy of the given FlowFile was sent to an external
     * destination. The external destination may be a remote system or may be a
     * local destination, such as the local file system but is external to NiFi.
     *
     * @param flowFile the FlowFile that was sent
     * @param transitUri A URI that provides information about the System and
     * Protocol information over which the transfer occurred. The intent of this
     * field is such that both the sender and the receiver can publish the
     * events to an external Enterprise-wide system that is then able to
     * correlate the SEND and RECEIVE events.
     * @param details additional details related to the SEND event, such as a
     * remote system's Distinguished Name
     * @param transmissionMillis the number of milliseconds spent sending the
     * data to the remote system
     * @param force if <code>true</code>, this event will be added to the
     * Provenance Repository immediately and will still be persisted if the
     * {@link org.apache.nifi.processor.ProcessSession ProcessSession} to which this
     * ProvenanceReporter is associated is rolled back. Otherwise, the Event
     * will be recorded only on a successful session commit.
     */
    void send(FlowFile flowFile, String transitUri, String details, long transmissionMillis, boolean force);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#ADDINFO ADDINFO} that provides a linkage
     * between the given FlowFile and alternate identifier. This information can
     * be useful if published to an external, enterprise-wide Provenance
     * tracking system that is able to associate the data between different
     * processes.
     *
     * @param flowFile the FlowFile for which the association should be made
     * @param alternateIdentifierNamespace the namespace of the alternate system
     * @param alternateIdentifier the identifier that the alternate system uses
     * when referring to the data that is encompassed by this FlowFile
     */
    void associate(FlowFile flowFile, String alternateIdentifierNamespace, String alternateIdentifier);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#FORK FORK}
     * that establishes that the given parent was split into multiple child
     * FlowFiles. In general, this method does not need to be called by
     * Processors, as the ProcessSession will handle this automatically for you
     * when calling {@link ProcessSession#create(FlowFile)}.
     *
     * @param parent the FlowFile from which the children are derived
     * @param children the FlowFiles that are derived from the parent.
     */
    void fork(FlowFile parent, Collection<FlowFile> children);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#FORK FORK}
     * that establishes that the given parent was split into multiple child
     * FlowFiles. In general, this method does not need to be called by
     * Processors, as the ProcessSession will handle this automatically for you
     * when calling {@link ProcessSession#create(FlowFile)}.
     *
     * @param parent the FlowFile from which the children are derived
     * @param children the FlowFiles that are derived from the parent.
     * @param details any details pertinent to the fork
     */
    void fork(FlowFile parent, Collection<FlowFile> children, String details);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#FORK FORK}
     * that establishes that the given parent was split into multiple child
     * FlowFiles. In general, this method does not need to be called by
     * Processors, as the ProcessSession will handle this automatically for you
     * when calling {@link ProcessSession#create(FlowFile)}.
     *
     * @param parent the FlowFile from which the children are derived
     * @param children the FlowFiles that are derived from the parent.
     * @param forkDuration the number of milliseconds that it took to perform
     * the task
     */
    void fork(FlowFile parent, Collection<FlowFile> children, long forkDuration);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#FORK FORK}
     * that establishes that the given parent was split into multiple child
     * FlowFiles. In general, this method does not need to be called by
     * Processors, as the ProcessSession will handle this automatically for you
     * when calling {@link ProcessSession#create(FlowFile)}.
     *
     * @param parent the FlowFile from which the children are derived
     * @param children the FlowFiles that are derived from the parent.
     * @param details any details pertinent to the fork
     * @param forkDuration the number of milliseconds that it took to perform
     * the task
     */
    void fork(FlowFile parent, Collection<FlowFile> children, String details, long forkDuration);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#JOIN JOIN}
     * that establishes that the given parents were joined together to create a
     * new child FlowFile. In general, this method does not need to be called by
     * Processors, as the ProcessSession will handle this automatically for you
     * when calling {@link ProcessSession#create(FlowFile)}.
     *
     * @param parents the FlowFiles that are being joined together to create the
     * child
     * @param child the FlowFile that is being created by joining the parents
     */
    void join(Collection<FlowFile> parents, FlowFile child);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#JOIN JOIN}
     * that establishes that the given parents were joined together to create a
     * new child FlowFile. In general, this method does not need to be called by
     * Processors, as the ProcessSession will handle this automatically for you
     * when calling {@link ProcessSession#create(FlowFile)}.
     *
     * @param parents the FlowFiles that are being joined together to create the
     * child
     * @param child the FlowFile that is being created by joining the parents
     * @param details any details pertinent to the event
     */
    void join(Collection<FlowFile> parents, FlowFile child, String details);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#JOIN JOIN}
     * that establishes that the given parents were joined together to create a
     * new child FlowFile. In general, this method does not need to be called by
     * Processors, as the ProcessSession will handle this automatically for you
     * when calling {@link ProcessSession#create(FlowFile)}.
     *
     * @param parents the FlowFiles that are being joined together to create the
     * child
     * @param child the FlowFile that is being created by joining the parents
     * @param joinDuration the number of milliseconds that it took to join the
     * FlowFiles
     */
    void join(Collection<FlowFile> parents, FlowFile child, long joinDuration);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#JOIN JOIN}
     * that establishes that the given parents were joined together to create a
     * new child FlowFile. In general, this method does not need to be called by
     * Processors, as the ProcessSession will handle this automatically for you
     * when calling {@link ProcessSession#create(FlowFile)}.
     *
     * @param parents the FlowFiles that are being joined together to create the
     * child
     * @param child the FlowFile that is being created by joining the parents
     * @param details any details pertinent to the event
     * @param joinDuration the number of milliseconds that it took to join the
     * FlowFiles
     */
    void join(Collection<FlowFile> parents, FlowFile child, String details, long joinDuration);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#CLONE CLONE}
     * that establishes that the given child is an exact replica of the parent.
     * In general, this method does not need to be called by Processors, as the
     * {@link ProcessSession} will handle this automatically for you when
     * calling {@link ProcessSession#clone(FlowFile)}
     *
     * @param parent the FlowFile that was cloned
     * @param child the clone
     */
    void clone(FlowFile parent, FlowFile child);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#CONTENT_MODIFIED CONTENT_MODIFIED} that
     * indicates that the content of the given FlowFile has been modified. One
     * of the <code>modifyContent</code> methods should be called any time that
     * the contents of a FlowFile are modified.
     *
     * @param flowFile the FlowFile whose content is being modified
     */
    void modifyContent(FlowFile flowFile);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#CONTENT_MODIFIED CONTENT_MODIFIED} that
     * indicates that the content of the given FlowFile has been modified. One
     * of the <code>modifyContent</code> methods should be called any time that
     * the contents of a FlowFile are modified.
     *
     * @param flowFile the FlowFile whose content is being modified
     * @param details Any details about how the content of the FlowFile has been
     * modified. Details should not be specified if they can be inferred by
     * other information in the event, such as the name of the Processor, as
     * specifying this information will add undue overhead
     */
    void modifyContent(FlowFile flowFile, String details);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#CONTENT_MODIFIED CONTENT_MODIFIED} that
     * indicates that the content of the given FlowFile has been modified. One
     * of the <code>modifyContent</code> methods should be called any time that
     * the contents of a FlowFile are modified.
     *
     * @param flowFile the FlowFile whose content is being modified
     * @param processingMillis the number of milliseconds spent processing the
     * FlowFile
     */
    void modifyContent(FlowFile flowFile, long processingMillis);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#CONTENT_MODIFIED CONTENT_MODIFIED} that
     * indicates that the content of the given FlowFile has been modified. One
     * of the <code>modifyContent</code> methods should be called any time that
     * the contents of a FlowFile are modified.
     *
     * @param flowFile the FlowFile whose content is being modified
     * @param details Any details about how the content of the FlowFile has been
     * modified. Details should not be specified if they can be inferred by
     * other information in the event, such as the name of the Processor, as
     * specifying this information will add undue overhead
     * @param processingMillis the number of milliseconds spent processing the
     * FlowFile
     */
    void modifyContent(FlowFile flowFile, String details, long processingMillis);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#ATTRIBUTES_MODIFIED ATTRIBUTES_MODIFIED} that
     * indicates that the Attributes of the given FlowFile were updated. It is
     * not necessary to emit such an event for a FlowFile if other Events are
     * already emitted by a Processor. For example, one should call both
     * {@link #modifyContent(FlowFile)} and {@link #modifyAttributes(FlowFile)}
     * for the same FlowFile in the same Processor. Rather, the Processor should
     * call just the {@link #modifyContent(FlowFile)}, as the call to
     * {@link #modifyContent(FlowFile)} will generate a Provenance Event that
     * already contains all FlowFile attributes. As such, emitting another event
     * that contains those attributes is unneeded and can result in a
     * significant amount of overhead for storage and processing.
     *
     * @param flowFile the FlowFile whose attributes were modified
     */
    void modifyAttributes(FlowFile flowFile);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#ATTRIBUTES_MODIFIED ATTRIBUTES_MODIFIED} that
     * indicates that the Attributes of the given FlowFile were updated. It is
     * not necessary to emit such an event for a FlowFile if other Events are
     * already emitted by a Processor. For example, one should call both
     * {@link #modifyContent(FlowFile)} and {@link #modifyAttributes(FlowFile)}
     * for the same FlowFile in the same Processor. Rather, the Processor should
     * call just the {@link #modifyContent(FlowFile)}, as the call to
     * {@link #modifyContent(FlowFile)} will generate a Provenance Event that
     * already contains all FlowFile attributes. As such, emitting another event
     * that contains those attributes is unneeded and can result in a
     * significant amount of overhead for storage and processing.
     *
     * @param flowFile the FlowFile whose attributes were modified
     * @param details any details should be provided about the attribute
     * modification
     */
    void modifyAttributes(FlowFile flowFile, String details);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#ROUTE ROUTE}
     * that indicates that the given FlowFile was routed to the given
     * {@link Relationship}. <b>Note: </b> this Event is intended for Processors
     * whose sole job it is to route FlowFiles and should NOT be used as a way
     * to indicate that the given FlowFile was routed to a standard 'success' or
     * 'failure' relationship. Doing so can be problematic, as DataFlow Managers
     * often will loop 'failure' relationships back to the same processor. As
     * such, emitting a Route event to indicate that a FlowFile was routed to
     * 'failure' can result in creating thousands of Provenance Events for a
     * given FlowFile, resulting in a very difficult-to- understand lineage.
     *
     * @param flowFile the FlowFile being routed
     * @param relationship the Relationship to which the FlowFile was routed
     */
    void route(FlowFile flowFile, Relationship relationship);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#ROUTE ROUTE}
     * that indicates that the given FlowFile was routed to the given
     * {@link Relationship}. <b>Note: </b> this Event is intended ONLY for
     * Processors whose sole job it is to route FlowFiles and should NOT be used
     * as a way to indicate that hte given FlowFile was routed to a standard
     * 'success' or 'failure' relationship. Doing so can be problematic, as
     * DataFlow Managers often will loop 'failure' relationships back to the
     * same processor. As such, emitting a Route event to indicate that a
     * FlowFile was routed to 'failure' can result in creating thousands of
     * Provenance Events for a given FlowFile, resulting in a very difficult-to-
     * understand lineage.
     *
     * @param flowFile the FlowFile being routed
     * @param relationship the Relationship to which the FlowFile was routed
     * @param details any details pertinent to the Route event, such as why the
     * FlowFile was routed to the specified Relationship
     */
    void route(FlowFile flowFile, Relationship relationship, String details);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#ROUTE ROUTE}
     * that indicates that the given FlowFile was routed to the given
     * {@link Relationship}. <b>Note: </b> this Event is intended ONLY for
     * Processors whose sole job it is to route FlowFiles and should NOT be used
     * as a way to indicate that hte given FlowFile was routed to a standard
     * 'success' or 'failure' relationship. Doing so can be problematic, as
     * DataFlow Managers often will loop 'failure' relationships back to the
     * same processor. As such, emitting a Route event to indicate that a
     * FlowFile was routed to 'failure' can result in creating thousands of
     * Provenance Events for a given FlowFile, resulting in a very difficult-to-
     * understand lineage.
     *
     * @param flowFile the FlowFile being routed
     * @param relationship the Relationship to which the FlowFile was routed
     * @param processingDuration the number of milliseconds that it took to
     * determine how to route the FlowFile
     */
    void route(FlowFile flowFile, Relationship relationship, long processingDuration);

    /**
     * Emits a Provenance Event of type {@link ProvenanceEventType#ROUTE ROUTE}
     * that indicates that the given FlowFile was routed to the given
     * {@link Relationship}. <b>Note: </b> this Event is intended ONLY for
     * Processors whose sole job it is to route FlowFiles and should NOT be used
     * as a way to indicate that hte given FlowFile was routed to a standard
     * 'success' or 'failure' relationship. Doing so can be problematic, as
     * DataFlow Managers often will loop 'failure' relationships back to the
     * same processor. As such, emitting a Route event to indicate that a
     * FlowFile was routed to 'failure' can result in creating thousands of
     * Provenance Events for a given FlowFile, resulting in a very difficult-to-
     * understand lineage.
     *
     * @param flowFile the FlowFile being routed
     * @param relationship the Relationship to which the FlowFile was routed
     * @param details any details pertinent to the Route event, such as why the
     * FlowFile was routed to the specified Relationship
     * @param processingDuration the number of milliseconds that it took to
     * determine how to route the FlowFile
     */
    void route(FlowFile flowFile, Relationship relationship, String details, long processingDuration);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#CREATE CREATE} that indicates that the given
     * FlowFile was created by NiFi from data that was not received from an
     * external entity. If the data was received from an external source, use
     * the {@link #receive(FlowFile, String)} event instead
     *
     * @param flowFile the FlowFile that was created
     */
    void create(FlowFile flowFile);

    /**
     * Emits a Provenance Event of type
     * {@link ProvenanceEventType#CREATE CREATE} that indicates that the given
     * FlowFile was created by NiFi from data that was not received from an
     * external entity. If the data was received from an external source, use
     * the {@link #receive(FlowFile, String, String, long)} event instead
     *
     * @param flowFile the FlowFile that was created
     * @param details any relevant details about the CREATE event
     */
    void create(FlowFile flowFile, String details);
}
