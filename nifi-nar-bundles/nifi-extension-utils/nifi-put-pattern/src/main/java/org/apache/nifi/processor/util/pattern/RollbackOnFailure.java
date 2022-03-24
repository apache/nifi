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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.pattern.PartialFunctions.AdjustRoute;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * <p>RollbackOnFailure can be used as a function context for process patterns such as {@link Put} to provide a configurable error handling.
 *
 * <p>
 *     RollbackOnFailure can add following characteristics to a processor:
 *     <li>When disabled, input FlowFiles caused an error will be routed to 'failure' or 'retry' relationship, based on the type of error.</li>
 *     <li>When enabled, input FlowFiles are kept in the input queue. A ProcessException is thrown to rollback the process session.</li>
 *     <li>It assumes anything happened during a processors onTrigger can rollback, if this is marked as transactional.</li>
 *     <li>If transactional and enabled, even if some FlowFiles are already processed, it rollbacks the session when error occurs.</li>
 *     <li>If not transactional and enabled, it only rollbacks the session when error occurs only if there was no progress.</li>
 * </p>
 *
 * <p>There are two approaches to apply RollbackOnFailure. One is using {@link ExceptionHandler#adjustError(BiFunction)},
 * and the other is implementing processor onTrigger using process patterns such as {@link Put#adjustRoute(AdjustRoute)}. </p>
 *
 * <p>It's also possible to use both approaches. ExceptionHandler can apply when an Exception is thrown immediately, while AdjustRoute respond later but requires less code.</p>
 */
public class RollbackOnFailure {

    private final boolean rollbackOnFailure;
    private final boolean transactional;
    private boolean discontinue;

    private int processedCount = 0;

    /**
     * Constructor.
     * @param rollbackOnFailure Should be set by user via processor configuration.
     * @param transactional Specify whether a processor is transactional.
     *                      If not, it is important to call {@link #proceed()} after successful execution of processors task,
     *                      that indicates processor made an operation that can not be undone.
     */
    public RollbackOnFailure(boolean rollbackOnFailure, boolean transactional) {
        this.rollbackOnFailure = rollbackOnFailure;
        this.transactional = transactional;
    }

    public static final PropertyDescriptor ROLLBACK_ON_FAILURE = createRollbackOnFailureProperty("");

    public static  PropertyDescriptor createRollbackOnFailureProperty(String additionalDescription) {
        return new PropertyDescriptor.Builder()
                .name("rollback-on-failure")
                .displayName("Rollback On Failure")
                .description("Specify how to handle error." +
                        " By default (false), if an error occurs while processing a FlowFile, the FlowFile will be routed to" +
                        " 'failure' or 'retry' relationship based on error type, and processor can continue with next FlowFile." +
                        " Instead, you may want to rollback currently processed FlowFiles and stop further processing immediately." +
                        " In that case, you can do so by enabling this 'Rollback On Failure' property. " +
                        " If enabled, failed FlowFiles will stay in the input relationship without penalizing it and being processed repeatedly" +
                        " until it gets processed successfully or removed by other means." +
                        " It is important to set adequate 'Yield Duration' to avoid retrying too frequently." + additionalDescription)
                .allowableValues("true", "false")
                .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                .defaultValue("false")
                .required(true)
                .build();
    }

    /**
     * Create a function to use with {@link ExceptionHandler} that adjust error type based on functional context.
     */
    public static <FCT extends RollbackOnFailure> BiFunction<FCT, ErrorTypes, ErrorTypes.Result> createAdjustError(final ComponentLog logger) {
        return (c, t) -> {

            ErrorTypes.Result adjusted = null;
            switch (t.destination()) {

                case ProcessException:
                    // If this process can rollback, then rollback it.
                    if (!c.canRollback()) {
                        // If an exception is thrown but the processor is not transactional and processed count > 0, adjust it to self,
                        // in order to stop any further processing until this input is processed successfully.
                        // If we throw an Exception in this state, the already succeeded FlowFiles will be rolled back, too.
                        // In case the progress was made by other preceding inputs,
                        // those successful inputs should be sent to 'success' and this input stays in incoming queue.
                        // In case this input made some progress to external system, the partial update will be replayed again,
                        // can cause duplicated data.
                        c.discontinue();
                        // We should not penalize a FlowFile, if we did, other FlowFiles can be fetched first.
                        // We need to block others to be processed until this one finishes.
                        adjusted = new ErrorTypes.Result(ErrorTypes.Destination.Self, ErrorTypes.Penalty.Yield);
                    }
                    break;

                case Failure:
                case Retry:
                    if (c.isRollbackOnFailure()) {
                        c.discontinue();
                        if (c.canRollback()) {
                            // If this process can rollback, then throw ProcessException instead, in order to rollback.
                            adjusted = new ErrorTypes.Result(ErrorTypes.Destination.ProcessException, ErrorTypes.Penalty.Yield);
                        } else {
                            // If not,
                            adjusted = new ErrorTypes.Result(ErrorTypes.Destination.Self, ErrorTypes.Penalty.Yield);
                        }
                    }
                    break;
            }

            if (adjusted != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Adjusted {} to {} based on context rollbackOnFailure={}, processedCount={}, transactional={}",
                            new Object[]{t, adjusted, c.isRollbackOnFailure(), c.getProcessedCount(), c.isTransactional()});
                }
                return adjusted;
            }

            return t.result();
        };
    }

    /**
     * Create an {@link AdjustRoute} function to use with process pattern such as {@link Put} that adjust routed FlowFiles based on context.
     * This function works as a safety net by covering cases that Processor implementation did not use ExceptionHandler and transfer FlowFiles
     * without considering RollbackOnFailure context.
     */
    public static <FCT extends RollbackOnFailure> AdjustRoute<FCT> createAdjustRoute(Relationship ... failureRelationships) {
        return (context, session, fc, result) -> {
            if (fc.isRollbackOnFailure()) {
                // Check if route contains failure relationship.
                for (Relationship failureRelationship : failureRelationships) {
                    if (!result.contains(failureRelationship)) {
                        continue;
                    }
                    if (fc.canRollback()) {
                        throw new ProcessException(String.format(
                                "A FlowFile is routed to %s. Rollback session based on context rollbackOnFailure=%s, processedCount=%d, transactional=%s",
                                failureRelationship.getName(), fc.isRollbackOnFailure(), fc.getProcessedCount(), fc.isTransactional()));
                    } else {
                        // Send failed FlowFiles to self.
                        final Map<Relationship, List<FlowFile>> routedFlowFiles = result.getRoutedFlowFiles();
                        final List<FlowFile> failedFlowFiles = routedFlowFiles.remove(failureRelationship);
                        result.routeTo(failedFlowFiles, Relationship.SELF);
                    }
                }
            }
        };
    }

    public static <FCT extends RollbackOnFailure, I> ExceptionHandler.OnError<FCT, I> createOnError(ExceptionHandler.OnError<FCT, I> onError) {
        return onError.andThen((context, input, result, e) -> {
            if (context.shouldDiscontinue()) {
                throw new DiscontinuedException("Discontinue processing due to " + e, e);
            }
        });
    }

    public static <FCT extends RollbackOnFailure> void onTrigger(
            ProcessContext context, ProcessSessionFactory sessionFactory, FCT functionContext, ComponentLog logger,
            PartialFunctions.OnTrigger onTrigger) throws ProcessException {

        PartialFunctions.onTrigger(context, sessionFactory, logger, onTrigger, (session, t) -> {
            // If RollbackOnFailure is enabled, do not penalize processing FlowFiles when rollback,
            // in order to keep those in the incoming relationship to be processed again.
            final boolean shouldPenalize = !functionContext.isRollbackOnFailure();
            session.rollback(shouldPenalize);

            // However, keeping failed FlowFile in the incoming relationship would retry it too often.
            // So, administratively yield the process.
            if (functionContext.isRollbackOnFailure()) {
                logger.warn("Administratively yielding {} after rolling back due to {}", new Object[]{context.getName(), t}, t);
                context.yield();
            }
        });
    }

    public int proceed() {
        return ++processedCount;
    }

    public int getProcessedCount() {
        return processedCount;
    }

    public boolean isRollbackOnFailure() {
        return rollbackOnFailure;
    }

    public boolean isTransactional() {
        return transactional;
    }

    public boolean canRollback() {
        return transactional || processedCount == 0;
    }

    public boolean shouldDiscontinue() {
        return discontinue;
    }

    public void discontinue() {
        this.discontinue = true;
    }
}
