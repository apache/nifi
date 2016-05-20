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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@EventDriven()
@Tags({"test", "debug", "processor", "utility", "flow", "flowfile"})
@CapabilityDescription("This processor aids in the testing and debugging of the flowfile framework by allowing "
        + "a developer or flow manager to force various responses to a flowfile.  In response to a receiving a flowfile "
        + "it can route to the success or failure relationships, rollback, rollback and yield, rollback with penalty, "
        + "or throw an exception.  In addition, if using a timer based scheduling strategy, upon being triggered without "
        + "a flowfile, it can be configured to throw an exception,  when triggered without a flowfile.\n"
        + "\n"
        + "The 'iterations' properties, such as \"Success iterations\", configure how many times each response should occur "
        + "in succession before moving on to the next response within the group of flowfile responses or no flowfile"
        + "responses.\n"
        + "\n"
        + "The order of responses when a flow file is received are:"
        + "  1. transfer flowfile to success relationship.\n"
        + "  2. transfer flowfile to failure relationship.\n"
        + "  3. rollback the flowfile without penalty.\n"
        + "  4. rollback the flowfile and yield the context.\n"
        + "  5. rollback the flowfile with penalty.\n"
        + "  6. throw an exception.\n"
        + "\n"
        + "The order of responses when no flow file is received are:"
        + "  1. yield the context.\n"
        + "  2. throw an exception.\n"
        + "  3. do nothing and return.\n"
        + "\n"
        + "By default, the processor is configured to perform each response one time.  After processing the list of "
        + "responses it will resume from the top of the list.\n"
        + "\n"
        + "To suppress any response, it's value can be set to zero (0) and no responses of that type will occur during "
        + "processing.")
public class DebugFlow extends AbstractProcessor {

    private Set<Relationship> relationships = null;

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Flowfiles processed successfully.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Flowfiles that failed to process.")
            .build();

    private List<PropertyDescriptor> propertyDescriptors = null;

    static final PropertyDescriptor FF_SUCCESS_ITERATIONS = new PropertyDescriptor.Builder()
            .name("Success Iterations")
            .description("Number of flowfiles to forward to success relationship.")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor FF_FAILURE_ITERATIONS = new PropertyDescriptor.Builder()
            .name("Failure Iterations")
            .description("Number of flowfiles to forward to failure relationship.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor FF_ROLLBACK_ITERATIONS = new PropertyDescriptor.Builder()
            .name("Rollback Iterations")
            .description("Number of flowfiles to roll back (without penalty).")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor FF_ROLLBACK_YIELD_ITERATIONS = new PropertyDescriptor.Builder()
            .name("Rollback Yield Iterations")
            .description("Number of flowfiles to roll back and yield.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor FF_ROLLBACK_PENALTY_ITERATIONS = new PropertyDescriptor.Builder()
            .name("Rollback Penalty Iterations")
            .description("Number of flowfiles to roll back with penalty.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor FF_EXCEPTION_ITERATIONS = new PropertyDescriptor.Builder()
            .name("Exception Iterations")
            .description("Number of flowfiles to throw NPE exception.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor NO_FF_EXCEPTION_ITERATIONS = new PropertyDescriptor.Builder()
            .name("No Flowfile Exception Iterations")
            .description("Number of times to throw NPE exception if no flowfile.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor NO_FF_YIELD_ITERATIONS = new PropertyDescriptor.Builder()
            .name("No Flowfile Yield Iterations")
            .description("Number of times to yield if no flowfile.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor NO_FF_SKIP_ITERATIONS = new PropertyDescriptor.Builder()
            .name("No Flowfile Skip Iterations")
            .description("Number of times to skip onTrigger if no flowfile.")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    Integer FF_SUCCESS_MAX = 0;
    private Integer FF_FAILURE_MAX = 0;
    private Integer FF_ROLLBACK_MAX = 0;
    private Integer FF_YIELD_MAX = 0;
    private Integer FF_PENALTY_MAX = 0;
    private Integer FF_EXCEPTION_MAX = 0;

    private Integer NO_FF_EXCEPTION_MAX = 0;
    private Integer NO_FF_YIELD_MAX = 0;
    private Integer NO_FF_SKIP_MAX = 0;

    private Integer FF_SUCCESS_CURR = 0;
    private Integer FF_FAILURE_CURR = 0;
    private Integer FF_ROLLBACK_CURR = 0;
    private Integer FF_YIELD_CURR = 0;
    private Integer FF_PENALTY_CURR = 0;
    private Integer FF_EXCEPTION_CURR = 0;

    private Integer NO_FF_EXCEPTION_CURR = 0;
    private Integer NO_FF_YIELD_CURR = 0;
    private Integer NO_FF_SKIP_CURR = 0;

    private FlowfileResponse curr_ff_resp;
    private NoFlowfileResponse curr_noff_resp;

    private enum FlowfileResponse {
        FF_SUCCESS_RESPONSE(0, 1),
        FF_FAILURE_RESPONSE(1, 2),
        FF_ROLLBACK_RESPONSE(2, 3),
        FF_YIELD_RESPONSE(3, 4),
        FF_PENALTY_RESPONSE(4, 5),
        FF_EXCEPTION_RESPONSE(5, 0);

        private Integer id;
        private Integer nextId;
        private FlowfileResponse next;

        private static final Map<Integer, FlowfileResponse> byId = new HashMap<>();
        static {
            for (FlowfileResponse rc : FlowfileResponse.values()) {
                if (byId.put(rc.id, rc) != null) {
                    throw new IllegalArgumentException("duplicate id: " + rc.id);
                }
            }
            for (FlowfileResponse rc : FlowfileResponse.values()) {
                rc.next = byId.get(rc.nextId);
            }
        }
        FlowfileResponse(Integer pId, Integer pNext) {
            id = pId;
            nextId = pNext;
        }
        FlowfileResponse getNextCycle() {
            return next;
        }
    }

    private enum NoFlowfileResponse {
        NO_FF_EXCEPTION_RESPONSE(0, 1),
        NO_FF_YIELD_RESPONSE(1, 2),
        NO_FF_SKIP_RESPONSE(2, 0);

        private Integer id;
        private Integer nextId;
        private NoFlowfileResponse next;

        private static final Map<Integer, NoFlowfileResponse> byId = new HashMap<>();
        static {
            for (NoFlowfileResponse rc : NoFlowfileResponse.values()) {
                if (byId.put(rc.id, rc) != null) {
                    throw new IllegalArgumentException("duplicate id: " + rc.id);
                }
            }
            for (NoFlowfileResponse rc : NoFlowfileResponse.values()) {
                rc.next = byId.get(rc.nextId);
            }
        }
        NoFlowfileResponse(Integer pId, Integer pNext) {
            id = pId;
            nextId = pNext;
        }
        NoFlowfileResponse getNextCycle() {
            return next;
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        if (relationships == null) {
            HashSet<Relationship> relSet = new HashSet<>();
            relSet.add(REL_SUCCESS);
            relSet.add(REL_FAILURE);
            relationships = Collections.unmodifiableSet(relSet);
        }
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        if (propertyDescriptors == null) {
            ArrayList<PropertyDescriptor> propList = new ArrayList<>();
            propList.add(FF_SUCCESS_ITERATIONS);
            propList.add(FF_FAILURE_ITERATIONS);
            propList.add(FF_ROLLBACK_ITERATIONS);
            propList.add(FF_ROLLBACK_YIELD_ITERATIONS);
            propList.add(FF_ROLLBACK_PENALTY_ITERATIONS);
            propList.add(FF_EXCEPTION_ITERATIONS);
            propList.add(NO_FF_EXCEPTION_ITERATIONS);
            propList.add(NO_FF_YIELD_ITERATIONS);
            propList.add(NO_FF_SKIP_ITERATIONS);
            propertyDescriptors = Collections.unmodifiableList(propList);
        }
        return propertyDescriptors;
    }

    @SuppressWarnings("unused")
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        FF_SUCCESS_MAX = context.getProperty(FF_SUCCESS_ITERATIONS).asInteger();
        FF_FAILURE_MAX = context.getProperty(FF_FAILURE_ITERATIONS).asInteger();
        FF_YIELD_MAX = context.getProperty(FF_ROLLBACK_YIELD_ITERATIONS).asInteger();
        FF_ROLLBACK_MAX = context.getProperty(FF_ROLLBACK_ITERATIONS).asInteger();
        FF_PENALTY_MAX = context.getProperty(FF_ROLLBACK_PENALTY_ITERATIONS).asInteger();
        FF_EXCEPTION_MAX = context.getProperty(FF_EXCEPTION_ITERATIONS).asInteger();
        NO_FF_EXCEPTION_MAX = context.getProperty(NO_FF_EXCEPTION_ITERATIONS).asInteger();
        NO_FF_YIELD_MAX = context.getProperty(NO_FF_YIELD_ITERATIONS).asInteger();
        NO_FF_SKIP_MAX = context.getProperty(NO_FF_SKIP_ITERATIONS).asInteger();
        curr_ff_resp = FlowfileResponse.FF_SUCCESS_RESPONSE;
        curr_noff_resp = NoFlowfileResponse.NO_FF_EXCEPTION_RESPONSE;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ProcessorLog logger = getLogger();

        FlowFile ff = session.get();

        // Make up to 2 passes to allow rollover from last cycle to first.
        // (This could be "while(true)" since responses should break out  if selected, but this
        //  prevents endless loops in the event of unexpected errors or future changes.)
        int pass = 2;
        while (pass > 0) {
            pass -= 1;
            if (ff == null) {
                if (curr_noff_resp == NoFlowfileResponse.NO_FF_EXCEPTION_RESPONSE) {
                    if (NO_FF_EXCEPTION_CURR < NO_FF_EXCEPTION_MAX) {
                        NO_FF_EXCEPTION_CURR += 1;
                        logger.info("DebugFlow throwing NPE with no flow file");
                        throw new NullPointerException("forced by " + this.getClass().getName());
                    } else {
                        NO_FF_EXCEPTION_CURR = 0;
                        curr_noff_resp = curr_noff_resp.getNextCycle();
                    }
                }
                if (curr_noff_resp == NoFlowfileResponse.NO_FF_YIELD_RESPONSE) {
                    if (NO_FF_YIELD_CURR < NO_FF_YIELD_MAX) {
                        NO_FF_YIELD_CURR += 1;
                        logger.info("DebugFlow yielding with no flow file");
                        context.yield();
                        break;
                    } else {
                        NO_FF_YIELD_CURR = 0;
                        curr_noff_resp = curr_noff_resp.getNextCycle();
                    }
                }
                if (curr_noff_resp == NoFlowfileResponse.NO_FF_SKIP_RESPONSE) {
                    if (NO_FF_SKIP_CURR < NO_FF_SKIP_MAX) {
                        NO_FF_SKIP_CURR += 1;
                        logger.info("DebugFlow skipping with no flow file");
                        return;
                    } else {
                        NO_FF_SKIP_CURR = 0;
                        curr_noff_resp = curr_noff_resp.getNextCycle();
                    }
                }
                return;
            } else {
                if (curr_ff_resp == FlowfileResponse.FF_SUCCESS_RESPONSE) {
                    if (FF_SUCCESS_CURR < FF_SUCCESS_MAX) {
                        FF_SUCCESS_CURR += 1;
                        logger.info("DebugFlow transferring to success file={} UUID={}",
                                new Object[]{ff.getAttribute(CoreAttributes.FILENAME.key()),
                                        ff.getAttribute(CoreAttributes.UUID.key())});
                        session.transfer(ff, REL_SUCCESS);
                        session.commit();
                        break;
                    } else {
                        FF_SUCCESS_CURR = 0;
                        curr_ff_resp = curr_ff_resp.getNextCycle();
                    }
                }
                if (curr_ff_resp == FlowfileResponse.FF_FAILURE_RESPONSE) {
                    if (FF_FAILURE_CURR < FF_FAILURE_MAX) {
                        FF_FAILURE_CURR += 1;
                        logger.info("DebugFlow transferring to failure file={} UUID={}",
                                new Object[]{ff.getAttribute(CoreAttributes.FILENAME.key()),
                                        ff.getAttribute(CoreAttributes.UUID.key())});
                        session.transfer(ff, REL_FAILURE);
                        session.commit();
                        break;
                    } else {
                        FF_FAILURE_CURR = 0;
                        curr_ff_resp = curr_ff_resp.getNextCycle();
                    }
                }
                if (curr_ff_resp == FlowfileResponse.FF_ROLLBACK_RESPONSE) {
                    if (FF_ROLLBACK_CURR < FF_ROLLBACK_MAX) {
                        FF_ROLLBACK_CURR += 1;
                        logger.info("DebugFlow rolling back (no penalty) file={} UUID={}",
                                new Object[]{ff.getAttribute(CoreAttributes.FILENAME.key()),
                                        ff.getAttribute(CoreAttributes.UUID.key())});
                        session.rollback();
                        session.commit();
                        break;
                    } else {
                        FF_ROLLBACK_CURR = 0;
                        curr_ff_resp = curr_ff_resp.getNextCycle();
                    }
                }
                if (curr_ff_resp == FlowfileResponse.FF_YIELD_RESPONSE) {
                    if (FF_YIELD_CURR < FF_YIELD_MAX) {
                        FF_YIELD_CURR += 1;
                        logger.info("DebugFlow yielding file={} UUID={}",
                                new Object[]{ff.getAttribute(CoreAttributes.FILENAME.key()),
                                        ff.getAttribute(CoreAttributes.UUID.key())});
                        session.rollback();
                        context.yield();
                        return;
                    } else {
                        FF_YIELD_CURR = 0;
                        curr_ff_resp = curr_ff_resp.getNextCycle();
                    }
                }
                if (curr_ff_resp == FlowfileResponse.FF_PENALTY_RESPONSE) {
                    if (FF_PENALTY_CURR < FF_PENALTY_MAX) {
                        FF_PENALTY_CURR += 1;
                        logger.info("DebugFlow rolling back (with penalty) file={} UUID={}",
                                new Object[]{ff.getAttribute(CoreAttributes.FILENAME.key()),
                                        ff.getAttribute(CoreAttributes.UUID.key())});
                        session.rollback(true);
                        session.commit();
                        break;
                    } else {
                        FF_PENALTY_CURR = 0;
                        curr_ff_resp = curr_ff_resp.getNextCycle();
                    }
                }
                if (curr_ff_resp == FlowfileResponse.FF_EXCEPTION_RESPONSE) {
                    if (FF_EXCEPTION_CURR < FF_EXCEPTION_MAX) {
                        FF_EXCEPTION_CURR += 1;
                        logger.info("DebugFlow throwing NPE file={} UUID={}",
                                new Object[]{ff.getAttribute(CoreAttributes.FILENAME.key()),
                                        ff.getAttribute(CoreAttributes.UUID.key())});
                        throw new NullPointerException("forced by " + this.getClass().getName());
                    } else {
                        FF_EXCEPTION_CURR = 0;
                        curr_ff_resp = curr_ff_resp.getNextCycle();
                    }
                }
            }
        }
    }
}
