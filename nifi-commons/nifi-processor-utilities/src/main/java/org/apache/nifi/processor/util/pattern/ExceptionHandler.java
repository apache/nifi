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
import org.apache.nifi.processor.util.pattern.ErrorTypes.Result;

import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * <p>ExceptionHandler provides a structured Exception handling logic composed by reusable partial functions.
 *
 * <p>
 *     Benefits of using ExceptionHandler:
 *     <li>Externalized error handling code which provides cleaner program only focusing on the expected path.</li>
 *     <li>Classify specific Exceptions into {@link ErrorTypes}, consolidated error handling based on error type.</li>
 *     <li>Context aware error handling, {@link RollbackOnFailure} for instance.</li>
 * </p>
 */
public class ExceptionHandler<C> {

    @FunctionalInterface
    public interface Procedure<I> {
        void apply(I input) throws Exception;
    }

    public interface OnError<C, I> {
        void apply(C context, I input, Result result, Exception e);

        default OnError<C, I> andThen(OnError<C, I> after) {
            return (c, i, r, e) -> {
                apply(c, i, r, e);
                after.apply(c, i, r, e);
            };
        }
    }

    /**
     * Simply categorise an Exception.
     */
    private Function<Exception, ErrorTypes> mapException;

    /**
     * Adjust error type based on the context.
     */
    private BiFunction<C, ErrorTypes, Result> adjustError;

    /**
     * Do some action to the input based on the final error type.
     */
    private OnError<C, ?> onError;

    /**
     * Specify a function that maps an Exception to certain ErrorType.
     */
    public void mapException(Function<Exception, ErrorTypes> mapException) {
        this.mapException = mapException;
    }

    /**
     * <p>Specify a function that adjust ErrorType based on a function context.
     * <p>For example, {@link RollbackOnFailure#createAdjustError(ComponentLog)} decides
     * whether a process session should rollback or transfer input to failure or retry.
     */
    public void adjustError(BiFunction<C, ErrorTypes, Result> adjustError) {
        this.adjustError = adjustError;
    }

    /**
     * <p>Specify a default OnError function that will be called if one is not explicitly specified when {@link #execute(Object, Object, Procedure)} is called.
     */
    public void onError(OnError<C, ?> onError) {
        this.onError = onError;
    }

    /**
     * <p>Executes specified procedure function with the input.
     * <p>Default OnError function will be called when an exception is thrown.
     * @param context function context
     * @param input input for procedure
     * @param procedure a function that does something with the input
     * @return True if the procedure finished without issue. False if procedure threw an Exception but it was handled by {@link OnError}.
     * @throws ProcessException Thrown if the exception was not handled by {@link OnError}
     * @throws DiscontinuedException Indicating the exception was handled by {@link OnError} but process should stop immediately
     * without processing any further input
     */
    @SuppressWarnings("unchecked")
    public <I> boolean execute(C context, I input, Procedure<I> procedure) throws ProcessException, DiscontinuedException {
        return execute(context, input, procedure, (OnError<C, I>) onError);
    }

    /**
     * <p>Executes specified procedure function with the input.
     * @param context function context
     * @param input input for procedure
     * @param procedure a function that does something with the input
     * @param onError specify {@link OnError} function for this execution
     * @return True if the procedure finished without issue. False if procedure threw an Exception but it was handled by {@link OnError}.
     * @throws ProcessException Thrown if the exception was not handled by {@link OnError}
     * @throws DiscontinuedException Indicating the exception was handled by {@link OnError} but process should stop immediately
     * without processing any further input
     */
    public <I> boolean execute(C context, I input, Procedure<I> procedure, OnError<C, I> onError) throws ProcessException, DiscontinuedException {
        try {
            procedure.apply(input);
            return true;
        } catch (Exception e) {

            if (mapException == null) {
                throw new ProcessException("An exception was thrown: " + e, e);
            }

            final ErrorTypes type = mapException.apply(e);

            final Result result;
            if (adjustError != null) {
                result = adjustError.apply(context, type);
            } else {
                result = new Result(type.destination(), type.penalty());
            }

            if (onError == null) {
                throw new IllegalStateException("OnError is not set.");
            }

            onError.apply(context, input, result, e);
        }
        return false;
    }

    private static FlowFile penalize(final ProcessContext context, final ProcessSession session,
                                     final FlowFile flowFile, final ErrorTypes.Penalty penalty) {
        switch (penalty) {
            case Penalize:
                return session.penalize(flowFile);
            case Yield:
                context.yield();
        }
        return flowFile;
    }

    /**
     * Create a {@link OnError} function instance that routes input based on {@link Result} destination and penalty.
     * @param context process context is used to yield a processor
     * @param session process session is used to penalize a FlowFile
     * @param routingResult input FlowFile will be routed to a destination relationship in this {@link RoutingResult}
     * @param relFailure specify failure relationship of a processor
     * @param relRetry specify retry relationship of a processor
     * @return composed function
     */
    public static <C> ExceptionHandler.OnError<C, FlowFile> createOnError(
            final ProcessContext context, final ProcessSession session, final RoutingResult routingResult,
            final Relationship relFailure, final Relationship relRetry) {

        return (fc, input, result, e) -> {
            final PartialFunctions.FlowFileGroup flowFileGroup = () -> Collections.singletonList(input);
            createOnGroupError(context, session, routingResult, relFailure, relRetry).apply(fc, flowFileGroup, result, e);
        };
    }

    /**
     * Same as {@link #createOnError(ProcessContext, ProcessSession, RoutingResult, Relationship, Relationship)} for FlowFileGroup.
     * @param context process context is used to yield a processor
     * @param session process session is used to penalize FlowFiles
     * @param routingResult input FlowFiles will be routed to a destination relationship in this {@link RoutingResult}
     * @param relFailure specify failure relationship of a processor
     * @param relRetry specify retry relationship of a processor
     * @return composed function
     */
    public static <C, I extends PartialFunctions.FlowFileGroup> ExceptionHandler.OnError<C, I> createOnGroupError(
            final ProcessContext context, final ProcessSession session, final RoutingResult routingResult,
            final Relationship relFailure, final Relationship relRetry) {
        return (c, g, r, e) -> {
            final Relationship routeTo;
            switch (r.destination()) {
                case Failure:
                    routeTo = relFailure;
                    break;
                case Retry:
                    routeTo = relRetry;
                    break;
                case Self:
                    routeTo = Relationship.SELF;
                    break;
                default:
                    if (e instanceof ProcessException) {
                        throw (ProcessException)e;
                    } else {
                        Object inputs = null;
                        if (g != null) {
                            final List<FlowFile> flowFiles = g.getFlowFiles();
                            switch (flowFiles.size()) {
                                case 0:
                                    inputs = "[]";
                                    break;
                                case 1:
                                    inputs = flowFiles.get(0);
                                    break;
                                default:
                                    inputs = String.format("%d FlowFiles including %s", flowFiles.size(), flowFiles.get(0));
                                    break;
                            }
                        }
                        throw new ProcessException(String.format("Failed to process %s due to %s", inputs, e), e);
                    }
            }
            for (FlowFile f : g.getFlowFiles()) {
                final FlowFile maybePenalized = penalize(context, session, f, r.penalty());
                routingResult.routeTo(maybePenalized, routeTo);
            }
        };
    }
}
