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

package org.apache.nifi.processors.standard;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.FileTransfer;
import org.apache.nifi.processors.standard.util.PermissionDeniedException;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.Tuple;

/**
 * A base class for FetchSFTP, FetchFTP processors.
 *
 * Note that implementations of this class should never use the @SupportsBatching annotation! Doing so
 * could result in data loss!
 */
public abstract class FetchFileTransfer extends AbstractProcessor {

    static final AllowableValue COMPLETION_NONE = new AllowableValue("None", "None", "Leave the file as-is");
    static final AllowableValue COMPLETION_MOVE = new AllowableValue("Move File", "Move File", "Move the file to the directory specified by the <Move Destination Directory> property");
    static final AllowableValue COMPLETION_DELETE = new AllowableValue("Delete File", "Delete File", "Deletes the original file from the remote system");


    static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
        .name("Hostname")
        .description("The fully-qualified hostname or IP address of the host to fetch the data from")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .required(true)
        .build();
    static final PropertyDescriptor UNDEFAULTED_PORT = new PropertyDescriptor.Builder()
        .name("Port")
        .description("The port to connect to on the remote host to fetch the data from")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .required(true)
        .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("Username")
        .description("Username")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .required(true)
        .build();
    public static final PropertyDescriptor REMOTE_FILENAME = new PropertyDescriptor.Builder()
        .name("Remote File")
        .description("The fully qualified filename on the remote system")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    static final PropertyDescriptor COMPLETION_STRATEGY = new PropertyDescriptor.Builder()
        .name("Completion Strategy")
        .description("Specifies what to do with the original file on the server once it has been pulled into NiFi. If the Completion Strategy fails, a warning will be "
            + "logged but the data will still be transferred.")
        .expressionLanguageSupported(false)
        .allowableValues(COMPLETION_NONE, COMPLETION_MOVE, COMPLETION_DELETE)
        .defaultValue(COMPLETION_NONE.getValue())
        .required(true)
        .build();
    static final PropertyDescriptor MOVE_DESTINATION_DIR = new PropertyDescriptor.Builder()
        .name("Move Destination Directory")
        .description("The directory on the remote server to the move the original file to once it has been ingested into NiFi. "
            + "This property is ignored unless the Completion Strategy is set to \"Move File\". The specified directory must already exist on"
            + "the remote system, or the rename will fail.")
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(false)
        .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("All FlowFiles that are received are routed to success")
        .build();
    static final Relationship REL_COMMS_FAILURE = new Relationship.Builder()
        .name("comms.failure")
        .description("Any FlowFile that could not be fetched from the remote server due to a communications failure will be transferred to this Relationship.")
        .build();
    static final Relationship REL_NOT_FOUND = new Relationship.Builder()
        .name("not.found")
        .description("Any FlowFile for which we receive a 'Not Found' message from the remote server will be transferred to this Relationship.")
        .build();
    static final Relationship REL_PERMISSION_DENIED = new Relationship.Builder()
        .name("permission.denied")
        .description("Any FlowFile that could not be fetched from the remote server due to insufficient permissions will be transferred to this Relationship.")
        .build();

    private final Map<Tuple<String, Integer>, BlockingQueue<FileTransferIdleWrapper>> fileTransferMap = new HashMap<>();
    private final long IDLE_CONNECTION_MILLIS = TimeUnit.SECONDS.toMillis(10L); // amount of time to wait before closing an idle connection
    private volatile long lastClearTime = System.currentTimeMillis();

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_NOT_FOUND);
        relationships.add(REL_PERMISSION_DENIED);
        relationships.add(REL_COMMS_FAILURE);
        return relationships;
    }

    /**
     * Close connections that are idle or optionally close all connections.
     * Connections are considered "idle" if they have not been used in 10 seconds.
     *
     * @param closeNonIdleConnections if <code>true</code> will close all connection; if <code>false</code> will close only idle connections
     */
    private void closeConnections(final boolean closeNonIdleConnections) {
        for (final Map.Entry<Tuple<String, Integer>, BlockingQueue<FileTransferIdleWrapper>> entry : fileTransferMap.entrySet()) {
            final BlockingQueue<FileTransferIdleWrapper> wrapperQueue = entry.getValue();

            final List<FileTransferIdleWrapper> putBack = new ArrayList<>();
            FileTransferIdleWrapper wrapper;
            while ((wrapper = wrapperQueue.poll()) != null) {
                final long lastUsed = wrapper.getLastUsed();
                final long nanosSinceLastUse = System.nanoTime() - lastUsed;
                if (!closeNonIdleConnections && TimeUnit.NANOSECONDS.toMillis(nanosSinceLastUse) < IDLE_CONNECTION_MILLIS) {
                    putBack.add(wrapper);
                } else {
                    try {
                        wrapper.getFileTransfer().close();
                    } catch (final IOException ioe) {
                        getLogger().warn("Failed to close Idle Connection due to {}", new Object[] {ioe}, ioe);
                    }
                }
            }

            for (final FileTransferIdleWrapper toPutBack : putBack) {
                wrapperQueue.offer(toPutBack);
            }
        }
    }

    @OnStopped
    public void cleanup() {
        // close all connections
        closeConnections(true);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HOSTNAME);
        properties.add(UNDEFAULTED_PORT);
        properties.add(REMOTE_FILENAME);
        properties.add(COMPLETION_STRATEGY);
        properties.add(MOVE_DESTINATION_DIR);
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        final String host = context.getProperty(HOSTNAME).evaluateAttributeExpressions(flowFile).getValue();
        final int port = context.getProperty(UNDEFAULTED_PORT).evaluateAttributeExpressions(flowFile).asInteger();
        final String filename = context.getProperty(REMOTE_FILENAME).evaluateAttributeExpressions(flowFile).getValue();

        // Try to get a FileTransfer object from our cache.
        BlockingQueue<FileTransferIdleWrapper> transferQueue;
        synchronized (fileTransferMap) {
            final Tuple<String, Integer> tuple = new Tuple<>(host, port);

            transferQueue = fileTransferMap.get(tuple);
            if (transferQueue == null) {
                transferQueue = new LinkedBlockingQueue<>();
                fileTransferMap.put(tuple, transferQueue);
            }

            // periodically close idle connections
            if (System.currentTimeMillis() - lastClearTime > IDLE_CONNECTION_MILLIS) {
                closeConnections(false);
                lastClearTime = System.currentTimeMillis();
            }
        }

        // we have a queue of FileTransfer Objects. Get one from the queue or create a new one.
        FileTransfer transfer;
        FileTransferIdleWrapper transferWrapper = transferQueue.poll();
        if (transferWrapper == null) {
            transfer = createFileTransfer(context);
        } else {
            transfer = transferWrapper.getFileTransfer();
        }

        // Pull data from remote system.
        final InputStream in;
        try {
            in = transfer.getInputStream(filename, flowFile);

            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    StreamUtils.copy(in, out);
                }
            });

            if(!transfer.flush(flowFile)) {
                throw new IOException("completePendingCommand returned false, file transfer failed");
            }

            transferQueue.offer(new FileTransferIdleWrapper(transfer, System.nanoTime()));
        } catch (final FileNotFoundException e) {
            getLogger().error("Failed to fetch content for {} from filename {} on remote host {} because the file could not be found on the remote system; routing to {}",
                new Object[] {flowFile, filename, host, REL_NOT_FOUND.getName()});
            session.transfer(session.penalize(flowFile), REL_NOT_FOUND);
            session.getProvenanceReporter().route(flowFile, REL_NOT_FOUND);
            return;
        } catch (final PermissionDeniedException e) {
            getLogger().error("Failed to fetch content for {} from filename {} on remote host {} due to insufficient permissions; routing to {}",
                new Object[] {flowFile, filename, host, REL_PERMISSION_DENIED.getName()});
            session.transfer(session.penalize(flowFile), REL_PERMISSION_DENIED);
            session.getProvenanceReporter().route(flowFile, REL_PERMISSION_DENIED);
            return;
        } catch (final ProcessException | IOException e) {
            try {
                transfer.close();
            } catch (final IOException e1) {
                getLogger().warn("Failed to close connection to {}:{} due to {}", new Object[] {host, port, e.toString()}, e);
            }

            getLogger().error("Failed to fetch content for {} from filename {} on remote host {}:{} due to {}; routing to comms.failure",
                new Object[] {flowFile, filename, host, port, e.toString()}, e);
            session.transfer(session.penalize(flowFile), REL_COMMS_FAILURE);
            return;
        }

        // Add FlowFile attributes
        final String protocolName = transfer.getProtocolName();
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(protocolName + ".remote.host", host);
        attributes.put(protocolName + ".remote.port", String.valueOf(port));
        attributes.put(protocolName + ".remote.filename", filename);

        if (filename.contains("/")) {
            final String path = StringUtils.substringBeforeLast(filename, "/");
            final String filenameOnly = StringUtils.substringAfterLast(filename, "/");
            attributes.put(CoreAttributes.PATH.key(), path);
            attributes.put(CoreAttributes.FILENAME.key(), filenameOnly);
        } else {
            attributes.put(CoreAttributes.FILENAME.key(), filename);
        }
        flowFile = session.putAllAttributes(flowFile, attributes);

        // emit provenance event and transfer FlowFile
        session.getProvenanceReporter().fetch(flowFile, protocolName + "://" + host + ":" + port + "/" + filename, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        session.transfer(flowFile, REL_SUCCESS);

        // it is critical that we commit the session before moving/deleting the remote file. Otherwise, we could have a situation where
        // we ingest the data, delete/move the remote file, and then NiFi dies/is shut down before the session is committed. This would
        // result in data loss! If we commit the session first, we are safe.
        session.commit();

        final String completionStrategy = context.getProperty(COMPLETION_STRATEGY).getValue();
        if (COMPLETION_DELETE.getValue().equalsIgnoreCase(completionStrategy)) {
            try {
                transfer.deleteFile(flowFile, null, filename);
            } catch (final FileNotFoundException e) {
                // file doesn't exist -- effectively the same as removing it. Move on.
            } catch (final IOException ioe) {
                getLogger().warn("Successfully fetched the content for {} from {}:{}{} but failed to remove the remote file due to {}",
                    new Object[] {flowFile, host, port, filename, ioe}, ioe);
            }
        } else if (COMPLETION_MOVE.getValue().equalsIgnoreCase(completionStrategy)) {
            String targetDir = context.getProperty(MOVE_DESTINATION_DIR).evaluateAttributeExpressions(flowFile).getValue();
            if (!targetDir.endsWith("/")) {
                targetDir = targetDir + "/";
            }
            final String simpleFilename = StringUtils.substringAfterLast(filename, "/");
            final String target = targetDir + simpleFilename;

            try {
                transfer.rename(flowFile, filename, target);
            } catch (final IOException ioe) {
                getLogger().warn("Successfully fetched the content for {} from {}:{}{} but failed to rename the remote file due to {}",
                    new Object[] {flowFile, host, port, filename, ioe}, ioe);
            }
        }
    }


    /**
     * Creates a new instance of a FileTransfer that can be used to pull files from a remote system.
     *
     * @param context the ProcessContext to use in order to obtain configured properties
     * @return a FileTransfer that can be used to pull files from a remote system
     */
    protected abstract FileTransfer createFileTransfer(ProcessContext context);

    /**
     * Wrapper around a FileTransfer object that is used to know when the FileTransfer was last used, so that
     * we have the ability to close connections that are "idle," or unused for some period of time.
     */
    private static class FileTransferIdleWrapper {
        private final FileTransfer fileTransfer;
        private final long lastUsed;

        public FileTransferIdleWrapper(final FileTransfer fileTransfer, final long lastUsed) {
            this.fileTransfer = fileTransfer;
            this.lastUsed = lastUsed;
        }

        public FileTransfer getFileTransfer() {
            return fileTransfer;
        }

        public long getLastUsed() {
            return this.lastUsed;
        }
    }
}
