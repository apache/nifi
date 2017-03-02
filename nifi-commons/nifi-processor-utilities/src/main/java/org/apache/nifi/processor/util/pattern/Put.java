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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Abstract Put pattern class with a generic onTrigger method structure, composed with various partial functions.
 * @param <FC> Class of context instance which is passed to each partial functions.
 *            Lifetime of an function context should be limited for a single onTrigger method.
 * @param <C> Class of connection to a data storage that this pattern puts data into.
 */
public class Put<FC, C extends AutoCloseable> {
    protected PartialFunctions.InitConnection<FC, C> initConnection;
    protected PartialFunctions.FetchFlowFiles<FC> fetchFlowFiles = PartialFunctions.fetchSingleFlowFile();
    protected PutFlowFile<FC, C> putFlowFile;
    protected PartialFunctions.TransferFlowFiles<FC> transferFlowFiles = PartialFunctions.transferRoutedFlowFiles();
    protected PartialFunctions.AdjustRoute<FC> adjustRoute;
    protected PartialFunctions.OnCompleted<FC, C> onCompleted;
    protected PartialFunctions.OnFailed<FC, C> onFailed;
    protected PartialFunctions.Cleanup<FC, C> cleanup;
    protected ComponentLog logger;

    /**
     * Put fetched FlowFiles to a data storage.
     * @param context process context passed from a Processor onTrigger.
     * @param session process session passed from a Processor onTrigger.
     * @param functionContext function context passed from a Processor onTrigger.
     * @param connection connection to data storage, established by {@link PartialFunctions.InitConnection}.
     * @param flowFiles FlowFiles fetched from {@link PartialFunctions.FetchFlowFiles}.
     * @param result Route incoming FlowFiles if necessary.
     */
    protected void putFlowFiles(ProcessContext context, ProcessSession session,
                                        FC functionContext, C connection, List<FlowFile> flowFiles, RoutingResult result) throws ProcessException {
        for (FlowFile flowFile : flowFiles) {
            putFlowFile.apply(context, session, functionContext, connection, flowFile, result);
        }
    }

    protected void validateCompositePattern() {
        Objects.requireNonNull(initConnection, "InitConnection function is required.");
        Objects.requireNonNull(putFlowFile, "PutFlowFile function is required.");
        Objects.requireNonNull(transferFlowFiles, "TransferFlowFiles function is required.");
    }

    /**
     * <p>Processor using this pattern is expected to call this method from its onTrigger.
     * <p>Typical usage would be constructing a process pattern instance at a processor method
     * which is annotated with {@link org.apache.nifi.annotation.lifecycle.OnScheduled},
     * and use pattern.onTrigger from processor.onTrigger.
     * <p>{@link PartialFunctions.InitConnection} is required at least. In addition to any functions required by an implementation class.
     * @param context process context passed from a Processor onTrigger.
     * @param session process session passed from a Processor onTrigger.
     * @param functionContext function context should be instantiated per onTrigger call.
     * @throws ProcessException Each partial function can throw ProcessException if onTrigger should stop immediately.
     */
    public void onTrigger(ProcessContext context, ProcessSession session, FC functionContext) throws ProcessException {

        validateCompositePattern();

        final RoutingResult result = new RoutingResult();
        final List<FlowFile> flowFiles = fetchFlowFiles.apply(context, session, functionContext, result);

        // Transfer FlowFiles if there is any.
        result.getRoutedFlowFiles().forEach(((relationship, routedFlowFiles) ->
                session.transfer(routedFlowFiles, relationship)));

        if (flowFiles == null || flowFiles.isEmpty()) {
            logger.debug("No incoming FlowFiles.");
            return;
        }

        try (C connection = initConnection.apply(context, session, functionContext)) {

            try {
                // Execute the core function.
                try {
                    putFlowFiles(context, session, functionContext, connection, flowFiles, result);
                } catch (DiscontinuedException e) {
                    // Whether it was an error or semi normal is depends on the implementation and reason why it wanted to discontinue.
                    // So, no logging is needed here.
                }

                // Extension point to alter routes.
                if (adjustRoute != null) {
                    adjustRoute.apply(context, session, functionContext, result);
                }

                // Put fetched, but unprocessed FlowFiles back to self.
                final List<FlowFile> transferredFlowFiles = result.getRoutedFlowFiles().values().stream()
                        .flatMap(List::stream).collect(Collectors.toList());
                final List<FlowFile> unprocessedFlowFiles = flowFiles.stream()
                        .filter(flowFile -> !transferredFlowFiles.contains(flowFile)).collect(Collectors.toList());
                result.routeTo(unprocessedFlowFiles, Relationship.SELF);

                // OnCompleted processing.
                if (onCompleted != null) {
                    onCompleted.apply(context, session, functionContext, connection);
                }

                // Transfer FlowFiles.
                transferFlowFiles.apply(context, session, functionContext, result);

            } catch (Exception e) {
                if (onFailed != null) {
                    onFailed.apply(context, session, functionContext, connection, e);
                }
                throw e;
            } finally {
                if (cleanup != null) {
                    cleanup.apply(context, session, functionContext, connection);
                }
            }

        } catch (ProcessException e) {
            throw e;
        } catch (Exception e) {
            // Throw uncaught exception as RuntimeException so that this processor will be yielded.
            final String msg = String.format("Failed to execute due to %s", e);
            logger.error(msg, e);
            throw new RuntimeException(msg, e);
        }

    }

    /**
     * Specify an optional function that fetches incoming FlowFIles.
     * If not specified, single FlowFile is fetched on each onTrigger.
     * @param f Function to fetch incoming FlowFiles.
     */
    public void fetchFlowFiles(PartialFunctions.FetchFlowFiles<FC> f) {
        fetchFlowFiles = f;
    }

    /**
     * Specify a function that establishes a connection to target data storage.
     * This function will be called when there is valid incoming FlowFiles.
     * The created connection instance is automatically closed when onTrigger is finished.
     * @param f Function to initiate a connection to a data storage.
     */
    public void initConnection(PartialFunctions.InitConnection<FC, C> f) {
        initConnection = f;
    }

    /**
     * Specify a function that puts an incoming FlowFile to target data storage.
     * @param f a function to put a FlowFile to target storage.
     */
    public void putFlowFile(PutFlowFile<FC, C> f) {
        this.putFlowFile = f;
    }

    /**
     * Specify an optional function that adjust routed FlowFiles before transfer it.
     * @param f a function to adjust route.
     */
    public void adjustRoute(PartialFunctions.AdjustRoute<FC> f) {
        this.adjustRoute = f;
    }

    /**
     * Specify an optional function responsible for transferring routed FlowFiles.
     * If not specified routed FlowFiles are simply transferred to its destination by default.
     * @param f a function to transfer routed FlowFiles.
     */
    public void transferFlowFiles(PartialFunctions.TransferFlowFiles<FC> f) {
        this.transferFlowFiles = f;
    }

    /**
     * Specify an optional function which will be called if input FlowFiles were successfully put to a target storage.
     * @param f Function to be called when a put operation finishes successfully.
     */
    public void onCompleted(PartialFunctions.OnCompleted<FC, C> f) {
        onCompleted = f;
    }

    /**
     * Specify an optional function which will be called if input FlowFiles failed being put to a target storage.
     * @param f Function to be called when a put operation failed.
     */
    public void onFailed(PartialFunctions.OnFailed<FC, C> f) {
        onFailed = f;
    }

    /**
     * Specify an optional function which will be called in a finally block.
     * Typically useful when a special cleanup operation is needed for the connection.
     * @param f Function to be called when a put operation finished regardless of whether it succeeded or not.
     */
    public void cleanup(PartialFunctions.Cleanup<FC, C> f) {
        cleanup = f;
    }

    public void setLogger(ComponentLog logger) {
        this.logger = logger;
    }

    @FunctionalInterface
    public interface PutFlowFile<FC, C> {
        void apply(ProcessContext context, ProcessSession session, FC functionContext, C connection,
                   FlowFile flowFile, RoutingResult result) throws ProcessException;
    }

}
