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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"test", "debug", "processor", "utility", "flow", "FlowFile"})
@CapabilityDescription("The DebugFlow processor aids testing and debugging the FlowFile framework by allowing various "
        + "responses to be explicitly triggered in response to the receipt of a FlowFile or a timer event without a "
        + "FlowFile if using timer or cron based scheduling.  It can force responses needed to exercise or test "
        + "various failure modes that can occur when a processor runs.")
public class DebugFlow extends AbstractProcessor {

    private final AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles processed successfully.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to process.")
            .build();

    private final AtomicReference<List<PropertyDescriptor>> properties = new AtomicReference<>();

    static final PropertyDescriptor FF_SUCCESS_ITERATIONS = new PropertyDescriptor.Builder()
            .name("FlowFile Success Iterations")
            .description("Number of FlowFiles to forward to success relationship.")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor FF_FAILURE_ITERATIONS = new PropertyDescriptor.Builder()
            .name("FlowFile Failure Iterations")
            .description("Number of FlowFiles to forward to failure relationship.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor FF_ROLLBACK_ITERATIONS = new PropertyDescriptor.Builder()
            .name("FlowFile Rollback Iterations")
            .description("Number of FlowFiles to roll back (without penalty).")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor FF_ROLLBACK_YIELD_ITERATIONS = new PropertyDescriptor.Builder()
            .name("FlowFile Rollback Yield Iterations")
            .description("Number of FlowFiles to roll back and yield.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor FF_ROLLBACK_PENALTY_ITERATIONS = new PropertyDescriptor.Builder()
            .name("FlowFile Rollback Penalty Iterations")
            .description("Number of FlowFiles to roll back with penalty.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor FF_EXCEPTION_ITERATIONS = new PropertyDescriptor.Builder()
            .name("FlowFile Exception Iterations")
            .description("Number of FlowFiles to throw exception.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor FF_EXCEPTION_CLASS = new PropertyDescriptor.Builder()
            .name("FlowFile Exception Class")
            .description("Exception class to be thrown (must extend java.lang.RuntimeException).")
            .required(true)
            .defaultValue("java.lang.RuntimeException")
        .addValidator(new RuntimeExceptionValidator())
            .build();

    static final PropertyDescriptor NO_FF_SKIP_ITERATIONS = new PropertyDescriptor.Builder()
            .name("No FlowFile Skip Iterations")
            .description("Number of times to skip onTrigger if no FlowFile.")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor NO_FF_EXCEPTION_ITERATIONS = new PropertyDescriptor.Builder()
            .name("No FlowFile Exception Iterations")
            .description("Number of times to throw NPE exception if no FlowFile.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor NO_FF_YIELD_ITERATIONS = new PropertyDescriptor.Builder()
            .name("No FlowFile Yield Iterations")
            .description("Number of times to yield if no FlowFile.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor NO_FF_EXCEPTION_CLASS = new PropertyDescriptor.Builder()
            .name("No FlowFile Exception Class")
            .description("Exception class to be thrown if no FlowFile (must extend java.lang.RuntimeException).")
            .required(true)
            .defaultValue("java.lang.RuntimeException")
            .addValidator(new RuntimeExceptionValidator())
            .build();
    static final PropertyDescriptor WRITE_ITERATIONS = new PropertyDescriptor.Builder()
        .name("Write Iterations")
        .description("Number of times to write to the FlowFile")
        .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
        .required(true)
        .defaultValue("0")
        .build();
    static final PropertyDescriptor CONTENT_SIZE = new PropertyDescriptor.Builder()
        .name("Content Size")
        .description("The number of bytes to write each time that the FlowFile is written to")
        .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
        .required(true)
        .defaultValue("1 KB")
        .build();

    static final PropertyDescriptor ON_SCHEDULED_SLEEP_TIME = new PropertyDescriptor.Builder()
        .name("@OnScheduled Pause Time")
        .description("Specifies how long the processor should sleep in the @OnScheduled method, so that the processor can be forced to take a long time to start up")
        .required(true)
        .defaultValue("0 sec")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .build();
    static final PropertyDescriptor ON_SCHEDULED_FAIL = new PropertyDescriptor.Builder()
        .name("Fail When @OnScheduled called")
        .description("Specifies whether or not the Processor should throw an Exception when the methods annotated with @OnScheduled are called")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();
    static final PropertyDescriptor ON_UNSCHEDULED_SLEEP_TIME = new PropertyDescriptor.Builder()
        .name("@OnUnscheduled Pause Time")
        .description("Specifies how long the processor should sleep in the @OnUnscheduled method, so that the processor can be forced to take a long time to respond when user clicks stop")
        .required(true)
        .defaultValue("0 sec")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .build();
    static final PropertyDescriptor ON_UNSCHEDULED_FAIL = new PropertyDescriptor.Builder()
        .name("Fail When @OnUnscheduled called")
        .description("Specifies whether or not the Processor should throw an Exception when the methods annotated with @OnUnscheduled are called")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();
    static final PropertyDescriptor ON_STOPPED_SLEEP_TIME = new PropertyDescriptor.Builder()
        .name("@OnStopped Pause Time")
        .description("Specifies how long the processor should sleep in the @OnStopped method, so that the processor can be forced to take a long time to shutdown")
        .required(true)
        .defaultValue("0 sec")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .build();
    static final PropertyDescriptor ON_STOPPED_FAIL = new PropertyDescriptor.Builder()
        .name("Fail When @OnStopped called")
        .description("Specifies whether or not the Processor should throw an Exception when the methods annotated with @OnStopped are called")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("false")
        .build();
    static final PropertyDescriptor ON_TRIGGER_SLEEP_TIME = new PropertyDescriptor.Builder()
        .name("OnTrigger Pause Time")
        .description("Specifies how long the processor should sleep in the onTrigger() method, so that the processor can be forced to take a long time to perform its task")
        .required(true)
        .defaultValue("0 sec")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .build();
    static final PropertyDescriptor CUSTOM_VALIDATE_SLEEP_TIME = new PropertyDescriptor.Builder()
        .name("CustomValidate Pause Time")
        .description("Specifies how long the processor should sleep in the customValidate() method")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .defaultValue("0 sec")
        .required(true)
        .build();
    static final PropertyDescriptor IGNORE_INTERRUPTS = new PropertyDescriptor.Builder()
        .name("Ignore Interrupts When Paused")
        .description("If the Processor's thread(s) are sleeping (due to one of the \"Pause Time\" properties above), and the thread is interrupted, "
            + "this indicates whether the Processor should ignore the interrupt and continue sleeping or if it should allow itself to be interrupted.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();

    private volatile Integer flowFileMaxSuccess = 0;
    private volatile Integer flowFileMaxFailure = 0;
    private volatile Integer flowFileMaxRollback = 0;
    private volatile Integer flowFileMaxYield = 0;
    private volatile Integer flowFileMaxPenalty = 0;
    private volatile Integer flowFileMaxException = 0;

    private volatile Integer noFlowFileMaxSkip = 0;
    private volatile Integer noFlowFileMaxException = 0;
    private volatile Integer noFlowFileMaxYield = 0;

    private volatile Integer flowFileCurrSuccess = 0;
    private volatile Integer flowFileCurrFailure = 0;
    private volatile Integer flowFileCurrRollback = 0;
    private volatile Integer flowFileCurrYield = 0;
    private volatile Integer flowFileCurrPenalty = 0;
    private volatile Integer flowFileCurrException = 0;

    private volatile Integer noFlowFileCurrSkip = 0;
    private volatile Integer noFlowFileCurrException = 0;
    private volatile Integer noFlowFileCurrYield = 0;

    private volatile Class<? extends RuntimeException> flowFileExceptionClass = null;
    private volatile Class<? extends RuntimeException> noFlowFileExceptionClass = null;

    private final FlowFileResponse curr_ff_resp = new FlowFileResponse();
    private final NoFlowFileResponse curr_noff_resp = new NoFlowFileResponse();

    @Override
    public Set<Relationship> getRelationships() {
        synchronized (relationships) {
            if (relationships.get() == null) {
                relationships.compareAndSet(null, Set.of(REL_SUCCESS, REL_FAILURE));
            }
            return relationships.get();
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        synchronized (properties) {
            if (properties.get() == null) {
                List<PropertyDescriptor> properties = List.of(
                        FF_SUCCESS_ITERATIONS,
                        FF_FAILURE_ITERATIONS,
                        FF_ROLLBACK_ITERATIONS,
                        FF_ROLLBACK_YIELD_ITERATIONS,
                        FF_ROLLBACK_PENALTY_ITERATIONS,
                        FF_EXCEPTION_ITERATIONS,
                        FF_EXCEPTION_CLASS,
                        NO_FF_SKIP_ITERATIONS,
                        NO_FF_EXCEPTION_ITERATIONS,
                        NO_FF_YIELD_ITERATIONS,
                        NO_FF_EXCEPTION_CLASS,
                        WRITE_ITERATIONS,
                        CONTENT_SIZE,
                        ON_SCHEDULED_SLEEP_TIME,
                        ON_SCHEDULED_FAIL,
                        ON_UNSCHEDULED_SLEEP_TIME,
                        ON_UNSCHEDULED_FAIL,
                        ON_STOPPED_SLEEP_TIME,
                        ON_STOPPED_FAIL,
                        ON_TRIGGER_SLEEP_TIME,
                        CUSTOM_VALIDATE_SLEEP_TIME,
                        IGNORE_INTERRUPTS
                );

                this.properties.compareAndSet(null, properties);
            }
            return properties.get();
        }
    }

    @OnScheduled
    @SuppressWarnings("unchecked")
    public void onScheduled(ProcessContext context) throws ClassNotFoundException, InterruptedException {
        flowFileMaxSuccess = context.getProperty(FF_SUCCESS_ITERATIONS).asInteger();
        flowFileMaxFailure = context.getProperty(FF_FAILURE_ITERATIONS).asInteger();
        flowFileMaxYield = context.getProperty(FF_ROLLBACK_YIELD_ITERATIONS).asInteger();
        flowFileMaxRollback = context.getProperty(FF_ROLLBACK_ITERATIONS).asInteger();
        flowFileMaxPenalty = context.getProperty(FF_ROLLBACK_PENALTY_ITERATIONS).asInteger();
        flowFileMaxException = context.getProperty(FF_EXCEPTION_ITERATIONS).asInteger();
        noFlowFileMaxException = context.getProperty(NO_FF_EXCEPTION_ITERATIONS).asInteger();
        noFlowFileMaxYield = context.getProperty(NO_FF_YIELD_ITERATIONS).asInteger();
        noFlowFileMaxSkip = context.getProperty(NO_FF_SKIP_ITERATIONS).asInteger();
        curr_ff_resp.reset();
        curr_noff_resp.reset();
        flowFileExceptionClass = (Class<? extends RuntimeException>) Class.forName(context.getProperty(FF_EXCEPTION_CLASS).toString());
        noFlowFileExceptionClass = (Class<? extends RuntimeException>) Class.forName(context.getProperty(NO_FF_EXCEPTION_CLASS).toString());

        sleep(context.getProperty(ON_SCHEDULED_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS),
            context.getProperty(IGNORE_INTERRUPTS).asBoolean());
        fail(context.getProperty(ON_SCHEDULED_FAIL).asBoolean(), OnScheduled.class);
    }

    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) throws InterruptedException {
        sleep(context.getProperty(ON_UNSCHEDULED_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS),
            context.getProperty(IGNORE_INTERRUPTS).asBoolean());
        fail(context.getProperty(ON_UNSCHEDULED_FAIL).asBoolean(), OnUnscheduled.class);
    }

    @OnStopped
    public void onStopped(final ProcessContext context) throws InterruptedException {
        sleep(context.getProperty(ON_STOPPED_SLEEP_TIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS),
            context.getProperty(IGNORE_INTERRUPTS).asBoolean());

        fail(context.getProperty(ON_STOPPED_FAIL).asBoolean(), OnStopped.class);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        try {
            sleep(validationContext.getProperty(CUSTOM_VALIDATE_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS),
                validationContext.getProperty(IGNORE_INTERRUPTS).asBoolean());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            return Set.of(new ValidationResult.Builder()
                .valid(false)
                .subject("Validation")
                .explanation("Processor Interrupted while performing validation").build());
        }

        return super.customValidate(validationContext);
    }

    private void sleep(final long millis, final boolean ignoreInterrupts) throws InterruptedException {
        if (millis > 0L) {
            final long endSleep = System.currentTimeMillis() + millis;
            while (System.currentTimeMillis() < endSleep) {
                // Use Math.max(1, <sleep duration>) here in case
                // System.currentTimeMillis() has changed since the check above
                // and subtracting it from endSleep would now result in a value <= 0.
                try {
                    Thread.sleep(Math.max(1L, endSleep - System.currentTimeMillis()));
                } catch (final InterruptedException ie) {
                    if (!ignoreInterrupts) {
                        Thread.currentThread().interrupt();
                        throw ie;
                    }
                }
            }
        }
    }

    private void fail(final boolean isAppropriate, final Class<?> annotationClass) {
        if (isAppropriate) {
            throw new RuntimeException("Failure configured for " + annotationClass.getSimpleName() + " methods");
        }
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        FlowFile ff = session.get();

        try {
            // Make up to 2 passes to allow rollover from last cycle to first.
            // (This could be "while(true)" since responses should break out  if selected, but this
            //  prevents endless loops in the event of unexpected errors or future changes.)
            for (int pass = 2; pass > 0; pass--) {
                if (ff == null) {
                    if (curr_noff_resp.state() == NoFlowFileResponseState.NO_FF_SKIP_RESPONSE) {
                        if (noFlowFileCurrSkip < noFlowFileMaxSkip) {
                            noFlowFileCurrSkip += 1;
                            logger.info("DebugFlow skipping with no flow file");
                            return;
                        } else {
                            noFlowFileCurrSkip = 0;
                            curr_noff_resp.getNextCycle();
                        }
                    }

                    if (curr_noff_resp.state() == NoFlowFileResponseState.NO_FF_EXCEPTION_RESPONSE) {
                        if (noFlowFileCurrException < noFlowFileMaxException) {
                            noFlowFileCurrException += 1;
                            logger.info("DebugFlow throwing NPE with no flow file");
                            String message = "forced by " + this.getClass().getName();
                            RuntimeException rte;
                            try {
                                rte = noFlowFileExceptionClass.getConstructor(String.class).newInstance(message);
                                throw rte;
                            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("{} unexpected exception throwing DebugFlow exception: {}",
                                            this, e);
                                }
                            }
                        } else {
                            noFlowFileCurrException = 0;
                            curr_noff_resp.getNextCycle();
                        }
                    }

                    if (curr_noff_resp.state() == NoFlowFileResponseState.NO_FF_YIELD_RESPONSE) {
                        if (noFlowFileCurrYield < noFlowFileMaxYield) {
                            noFlowFileCurrYield += 1;
                            logger.info("DebugFlow yielding with no flow file");
                            context.yield();
                            break;
                        } else {
                            noFlowFileCurrYield = 0;
                            curr_noff_resp.getNextCycle();
                        }
                    }

                    return;
                } else {
                    final int writeIterations = context.getProperty(WRITE_ITERATIONS).asInteger();
                    if (writeIterations > 0 && pass == 1) {
                        final Random random = new Random();

                        for (int i = 0; i < writeIterations; i++) {
                            final byte[] data = new byte[context.getProperty(CONTENT_SIZE).asDataSize(DataUnit.B).intValue()];
                            random.nextBytes(data);

                            ff = session.write(ff, out -> out.write(data));
                        }
                    }

                    if (curr_ff_resp.state() == FlowFileResponseState.FF_SUCCESS_RESPONSE) {
                        if (flowFileCurrSuccess < flowFileMaxSuccess) {
                            flowFileCurrSuccess += 1;
                            logger.info("DebugFlow transferring to success file={} UUID={}",
                                    ff.getAttribute(CoreAttributes.FILENAME.key()),
                                    ff.getAttribute(CoreAttributes.UUID.key()));
                            session.transfer(ff, REL_SUCCESS);
                            break;
                        } else {
                            flowFileCurrSuccess = 0;
                            curr_ff_resp.getNextCycle();
                        }
                    }

                    if (curr_ff_resp.state() == FlowFileResponseState.FF_FAILURE_RESPONSE) {
                        if (flowFileCurrFailure < flowFileMaxFailure) {
                            flowFileCurrFailure += 1;
                            logger.info("DebugFlow transferring to failure file={} UUID={}",
                                    ff.getAttribute(CoreAttributes.FILENAME.key()),
                                    ff.getAttribute(CoreAttributes.UUID.key()));
                            session.transfer(ff, REL_FAILURE);
                            break;
                        } else {
                            flowFileCurrFailure = 0;
                            curr_ff_resp.getNextCycle();
                        }
                    }

                    if (curr_ff_resp.state() == FlowFileResponseState.FF_ROLLBACK_RESPONSE) {
                        if (flowFileCurrRollback < flowFileMaxRollback) {
                            flowFileCurrRollback += 1;
                            logger.info("DebugFlow rolling back (no penalty) file={} UUID={}",
                                    ff.getAttribute(CoreAttributes.FILENAME.key()),
                                    ff.getAttribute(CoreAttributes.UUID.key()));
                            session.rollback();
                            break;
                        } else {
                            flowFileCurrRollback = 0;
                            curr_ff_resp.getNextCycle();
                        }
                    }

                    if (curr_ff_resp.state() == FlowFileResponseState.FF_YIELD_RESPONSE) {
                        if (flowFileCurrYield < flowFileMaxYield) {
                            flowFileCurrYield += 1;
                            logger.info("DebugFlow yielding file={} UUID={}",
                                    ff.getAttribute(CoreAttributes.FILENAME.key()),
                                    ff.getAttribute(CoreAttributes.UUID.key()));
                            session.rollback();
                            context.yield();
                            return;
                        } else {
                            flowFileCurrYield = 0;
                            curr_ff_resp.getNextCycle();
                        }
                    }

                    if (curr_ff_resp.state() == FlowFileResponseState.FF_PENALTY_RESPONSE) {
                        if (flowFileCurrPenalty < flowFileMaxPenalty) {
                            flowFileCurrPenalty += 1;
                            logger.info("DebugFlow rolling back (with penalty) file={} UUID={}",
                                    ff.getAttribute(CoreAttributes.FILENAME.key()),
                                    ff.getAttribute(CoreAttributes.UUID.key()));
                            session.rollback(true);
                            break;
                        } else {
                            flowFileCurrPenalty = 0;
                            curr_ff_resp.getNextCycle();
                        }
                    }

                    if (curr_ff_resp.state() == FlowFileResponseState.FF_EXCEPTION_RESPONSE) {
                        if (flowFileCurrException < flowFileMaxException) {
                            flowFileCurrException += 1;
                            String message = "forced by " + this.getClass().getName();
                            logger.info("DebugFlow throwing NPE file={} UUID={}",
                                    ff.getAttribute(CoreAttributes.FILENAME.key()),
                                    ff.getAttribute(CoreAttributes.UUID.key()));
                            RuntimeException rte;
                            try {
                                rte = flowFileExceptionClass.getConstructor(String.class).newInstance(message);
                                throw rte;
                            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                                if (logger.isErrorEnabled()) {
                                    logger.error("{} unexpected exception throwing DebugFlow exception: {}",
                                            this, e);
                                }
                            }
                        } else {
                            flowFileCurrException = 0;
                            curr_ff_resp.getNextCycle();
                        }
                    }
                }
            }
        } finally {
            final long sleepMillis = context.getProperty(ON_TRIGGER_SLEEP_TIME).asTimePeriod(TimeUnit.MILLISECONDS);

            try {
                if (sleepMillis > 0) {
                    sleep(sleepMillis, context.getProperty(IGNORE_INTERRUPTS).asBoolean());
                    getLogger().info("DebugFlow finished sleeping at completion of its onTrigger() method");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class RuntimeExceptionValidator implements Validator {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final ValidationResult.Builder resultBuilder = new ValidationResult.Builder()
                .subject(subject)
                .input(input);

            try {
                final Class<?> exceptionClass = Class.forName(input);
                if (RuntimeException.class.isAssignableFrom(exceptionClass)) {
                    resultBuilder.valid(true);
                } else {
                    resultBuilder.valid(false).explanation("Class " + input + " is a Checked Exception, not a RuntimeException");
                }
            } catch (ClassNotFoundException e) {
                resultBuilder.valid(false).explanation("Class " + input + " cannot be found");
            }

            return resultBuilder.build();
        }
    }

    private enum FlowFileResponseState {
        FF_SUCCESS_RESPONSE,
        FF_FAILURE_RESPONSE,
        FF_ROLLBACK_RESPONSE,
        FF_YIELD_RESPONSE,
        FF_PENALTY_RESPONSE,
        FF_EXCEPTION_RESPONSE;

        private FlowFileResponseState nextState;
        static {
            FF_SUCCESS_RESPONSE.nextState = FF_FAILURE_RESPONSE;
            FF_FAILURE_RESPONSE.nextState = FF_ROLLBACK_RESPONSE;
            FF_ROLLBACK_RESPONSE.nextState = FF_YIELD_RESPONSE;
            FF_YIELD_RESPONSE.nextState = FF_PENALTY_RESPONSE;
            FF_PENALTY_RESPONSE.nextState = FF_EXCEPTION_RESPONSE;
            FF_EXCEPTION_RESPONSE.nextState = FF_SUCCESS_RESPONSE;
        }
        FlowFileResponseState next() {
            return nextState;
        }
    }

    private class FlowFileResponse {
        private final AtomicReference<FlowFileResponseState> current = new AtomicReference<>();
        FlowFileResponse() {
            current.set(FlowFileResponseState.FF_SUCCESS_RESPONSE);
        }
        synchronized FlowFileResponseState state() {
            return current.get();
        }
        synchronized void getNextCycle() {
            current.set(current.get().next());
        }
        synchronized void reset() {
            current.set(FlowFileResponseState.FF_SUCCESS_RESPONSE);
        }
    }

    private enum NoFlowFileResponseState {
        NO_FF_SKIP_RESPONSE,
        NO_FF_EXCEPTION_RESPONSE,
        NO_FF_YIELD_RESPONSE;

        private NoFlowFileResponseState nextState;
        static {
            NO_FF_SKIP_RESPONSE.nextState = NO_FF_EXCEPTION_RESPONSE;
            NO_FF_EXCEPTION_RESPONSE.nextState = NO_FF_YIELD_RESPONSE;
            NO_FF_YIELD_RESPONSE.nextState = NO_FF_SKIP_RESPONSE;
        }
        NoFlowFileResponseState next() {
            return nextState;
        }
    }

    private class NoFlowFileResponse {
        private final AtomicReference<NoFlowFileResponseState> current = new AtomicReference<>();
        NoFlowFileResponse() {
            current.set(NoFlowFileResponseState.NO_FF_SKIP_RESPONSE);
        }
        synchronized NoFlowFileResponseState state() {
            return current.get();
        }
        synchronized void getNextCycle() {
            current.set(current.get().next());
        }
        synchronized void reset() {
            current.set(NoFlowFileResponseState.NO_FF_SKIP_RESPONSE);
        }
    }
}
