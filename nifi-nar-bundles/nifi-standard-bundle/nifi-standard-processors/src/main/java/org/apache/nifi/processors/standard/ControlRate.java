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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.timebuffer.LongEntityAccess;
import org.apache.nifi.util.timebuffer.TimedBuffer;
import org.apache.nifi.util.timebuffer.TimestampedLong;

@SideEffectFree
@TriggerSerially
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"rate control", "throttle", "rate", "throughput"})
@CapabilityDescription("Controls the rate at which data is transferred to follow-on processors."
        + " If you configure a very small Time Duration, then the accuracy of the throttle gets worse."
        + " You can improve this accuracy by decreasing the Yield Duration, at the expense of more Tasks given to the processor.")
public class ControlRate extends AbstractProcessor {

    public static final String DATA_RATE = "data rate";
    public static final String FLOWFILE_RATE = "flowfile count";
    public static final String ATTRIBUTE_RATE = "attribute value";
    public static final AllowableValue DATA_RATE_VALUE = new AllowableValue(DATA_RATE, DATA_RATE,
            "Rate is controlled by counting bytes transferred per time duration.");
    public static final AllowableValue FLOWFILE_RATE_VALUE = new AllowableValue(FLOWFILE_RATE, FLOWFILE_RATE,
            "Rate is controlled by counting flowfiles transferred per time duration");
    public static final AllowableValue ATTRIBUTE_RATE_VALUE = new AllowableValue(ATTRIBUTE_RATE, ATTRIBUTE_RATE,
            "Rate is controlled by accumulating the value of a specified attribute that is transferred per time duration");

    // based on testing to balance commits and 10,000 FF swap limit
    public static final int MAX_FLOW_FILES_PER_BATCH = 1000;

    public static final PropertyDescriptor RATE_CONTROL_CRITERIA = new PropertyDescriptor.Builder()
            .name("Rate Control Criteria")
            .description("Indicates the criteria that is used to control the throughput rate. Changing this value resets the rate counters.")
            .required(true)
            .allowableValues(DATA_RATE_VALUE, FLOWFILE_RATE_VALUE, ATTRIBUTE_RATE_VALUE)
            .defaultValue(DATA_RATE)
            .build();
    public static final PropertyDescriptor MAX_RATE = new PropertyDescriptor.Builder()
            .name("Maximum Rate")
            .description("The maximum rate at which data should pass through this processor. The format of this property is expected to be a "
                    + "positive integer, or a Data Size (such as '1 MB') if Rate Control Criteria is set to 'data rate'.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR) // validated in customValidate b/c dependent on Rate Control Criteria
            .build();
    public static final PropertyDescriptor RATE_CONTROL_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Rate Controlled Attribute")
            .description("The name of an attribute whose values build toward the rate limit if Rate Control Criteria is set to 'attribute value'. "
                    + "The value of the attribute referenced by this property must be a positive long, or the FlowFile will be routed to failure. "
                    + "This value is ignored if Rate Control Criteria is not set to 'attribute value'. Changing this value resets the rate counters.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();
    public static final PropertyDescriptor TIME_PERIOD = new PropertyDescriptor.Builder()
            .name("Time Duration")
            .description("The amount of time to which the Maximum Rate pertains. Changing this value resets the rate counters.")
            .required(true)
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
            .defaultValue("1 min")
            .build();
    public static final PropertyDescriptor GROUPING_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Grouping Attribute")
            .description("By default, a single \"throttle\" is used for all FlowFiles. If this value is specified, a separate throttle is used for "
                    + "each value specified by the attribute with this name. Changing this value resets the rate counters.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are transferred to this relationship under normal conditions")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles will be routed to this relationship if they are missing a necessary Rate Controlled Attribute or the attribute is not in the expected format")
            .build();

    private static final Pattern POSITIVE_LONG_PATTERN = Pattern.compile("0*[1-9][0-9]*");
    private static final String DEFAULT_GROUP_ATTRIBUTE = ControlRate.class.getName() + "###____DEFAULT_GROUP_ATTRIBUTE___###";

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    private final ConcurrentMap<String, Throttle> throttleMap = new ConcurrentHashMap<>();
    private final AtomicLong lastThrottleClearTime = new AtomicLong(System.currentTimeMillis());
    private volatile String rateControlCriteria = null;
    private volatile String rateControlAttribute = null;
    private volatile String maximumRateStr = null;
    private volatile String groupingAttributeName = null;
    private volatile int timePeriodSeconds = 1;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RATE_CONTROL_CRITERIA);
        properties.add(MAX_RATE);
        properties.add(RATE_CONTROL_ATTRIBUTE_NAME);
        properties.add(TIME_PERIOD);
        properties.add(GROUPING_ATTRIBUTE_NAME);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));

        final Validator rateValidator;
        switch (context.getProperty(RATE_CONTROL_CRITERIA).getValue().toLowerCase()) {
            case DATA_RATE:
                rateValidator = StandardValidators.DATA_SIZE_VALIDATOR;
                break;
            case ATTRIBUTE_RATE:
                rateValidator = StandardValidators.POSITIVE_LONG_VALIDATOR;
                final String rateAttr = context.getProperty(RATE_CONTROL_ATTRIBUTE_NAME).getValue();
                if (rateAttr == null) {
                    validationResults.add(new ValidationResult.Builder()
                            .subject(RATE_CONTROL_ATTRIBUTE_NAME.getName())
                            .explanation("<Rate Controlled Attribute> property must be set if using <Rate Control Criteria> of 'attribute value'")
                            .build());
                }
                break;
            case FLOWFILE_RATE:
            default:
                rateValidator = StandardValidators.POSITIVE_LONG_VALIDATOR;
                break;
        }

        final ValidationResult rateResult = rateValidator.validate("Maximum Rate", context.getProperty(MAX_RATE).getValue(), context);
        if (!rateResult.isValid()) {
            validationResults.add(rateResult);
        }

        return validationResults;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);

        if (descriptor.equals(RATE_CONTROL_CRITERIA)
                || descriptor.equals(RATE_CONTROL_ATTRIBUTE_NAME)
                || descriptor.equals(GROUPING_ATTRIBUTE_NAME)
                || descriptor.equals(TIME_PERIOD)) {
            // if the criteria that is being used to determine limits/throttles is changed, we must clear our throttle map.
            throttleMap.clear();
        } else if (descriptor.equals(MAX_RATE)) {
            final long newRate;
            if (DataUnit.DATA_SIZE_PATTERN.matcher(newValue.toUpperCase()).matches()) {
                newRate = DataUnit.parseDataSize(newValue, DataUnit.B).longValue();
            } else {
                newRate = Long.parseLong(newValue);
            }

            for (final Throttle throttle : throttleMap.values()) {
                throttle.setMaxRate(newRate);
            }
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        rateControlCriteria = context.getProperty(RATE_CONTROL_CRITERIA).getValue().toLowerCase();
        rateControlAttribute = context.getProperty(RATE_CONTROL_ATTRIBUTE_NAME).getValue();
        maximumRateStr = context.getProperty(MAX_RATE).getValue().toUpperCase();
        groupingAttributeName = context.getProperty(GROUPING_ATTRIBUTE_NAME).getValue();
        timePeriodSeconds = context.getProperty(TIME_PERIOD).asTimePeriod(TimeUnit.SECONDS).intValue();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        List<FlowFile> flowFiles = session.get(new ThrottleFilter(MAX_FLOW_FILES_PER_BATCH));
        if (flowFiles.isEmpty()) {
            context.yield();
            return;
        }

        // Periodically clear any Throttle that has not been used in more than 2 throttling periods
        final long lastClearTime = lastThrottleClearTime.get();
        final long throttleExpirationMillis = System.currentTimeMillis() - 2 * context.getProperty(TIME_PERIOD).asTimePeriod(TimeUnit.MILLISECONDS);
        if (lastClearTime < throttleExpirationMillis) {
            if (lastThrottleClearTime.compareAndSet(lastClearTime, System.currentTimeMillis())) {
                final Iterator<Map.Entry<String, Throttle>> itr = throttleMap.entrySet().iterator();
                while (itr.hasNext()) {
                    final Map.Entry<String, Throttle> entry = itr.next();
                    final Throttle throttle = entry.getValue();
                    if (throttle.tryLock()) {
                        try {
                            if (throttle.lastUpdateTime() < lastClearTime) {
                                itr.remove();
                            }
                        } finally {
                            throttle.unlock();
                        }
                    }
                }
            }
        }

        final ComponentLog logger = getLogger();
        for (FlowFile flowFile : flowFiles) {
            // call this to capture potential error
            final long accrualAmount = getFlowFileAccrual(flowFile);
            if (accrualAmount < 0) {
                logger.error("Routing {} to 'failure' due to missing or invalid attribute", new Object[]{flowFile});
                session.transfer(flowFile, REL_FAILURE);
            } else {
                logger.info("transferring {} to 'success'", new Object[]{flowFile});
                session.transfer(flowFile, REL_SUCCESS);
            }
        }
    }

    /*
     * Determine the amount this FlowFile will incur against the maximum allowed rate.
     * If the value returned is negative then the flowfile given is missing the required attribute
     * or the attribute has an invalid value for accrual.
     */
    private long getFlowFileAccrual(FlowFile flowFile) {
        long rateValue;
        switch (rateControlCriteria) {
            case DATA_RATE:
                rateValue = flowFile.getSize();
                break;
            case FLOWFILE_RATE:
                rateValue = 1;
                break;
            case ATTRIBUTE_RATE:
                final String attributeValue = flowFile.getAttribute(rateControlAttribute);
                if (attributeValue == null) {
                    return -1L;
                }

                if (!POSITIVE_LONG_PATTERN.matcher(attributeValue).matches()) {
                    return -1L;
                }
                rateValue = Long.parseLong(attributeValue);
                break;
            default:
                throw new AssertionError("<Rate Control Criteria> property set to illegal value of " + rateControlCriteria);
        }
        return rateValue;
    }

    private static class Throttle extends ReentrantLock {

        private final AtomicLong maxRate = new AtomicLong(1L);
        private final long timePeriodMillis;
        private final TimedBuffer<TimestampedLong> timedBuffer;
        private final ComponentLog logger;

        private volatile long penalizationPeriod = 0;
        private volatile long penalizationExpired = 0;
        private volatile long lastUpdateTime;

        public Throttle(final int timePeriod, final TimeUnit unit, final ComponentLog logger) {
            this.timePeriodMillis = TimeUnit.MILLISECONDS.convert(timePeriod, unit);
            this.timedBuffer = new TimedBuffer<>(unit, timePeriod, new LongEntityAccess());
            this.logger = logger;
        }

        public void setMaxRate(final long maxRate) {
            this.maxRate.set(maxRate);
        }

        public long lastUpdateTime() {
            return lastUpdateTime;
        }

        public boolean tryAdd(final long value) {
            final long now = System.currentTimeMillis();
            if (penalizationExpired > now) {
                return false;
            }

            final long maxRateValue = maxRate.get();

            final TimestampedLong sum = timedBuffer.getAggregateValue(timePeriodMillis);
            if (sum != null && sum.getValue() >= maxRateValue) {
                if (logger.isDebugEnabled()) {
                    logger.debug("current sum for throttle is {} at time {}, so not allowing rate of {} through", new Object[]{sum.getValue(), sum.getTimestamp(), value});
                }
                return false;
            }

            // Implement the Throttle penalization based on how much extra 'amountOver' was allowed through
            if (penalizationPeriod > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Starting Throttle penalization, expiring {} milliseconds from now", new Object[]{penalizationPeriod});
                }
                penalizationExpired = now + penalizationPeriod;
                penalizationPeriod = 0;
                return false;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("current sum for throttle is {} at time {}, so allowing rate of {} through",
                        new Object[]{sum == null ? 0 : sum.getValue(), sum == null ? 0 : sum.getTimestamp(), value});
            }

            final long transferred = timedBuffer.add(new TimestampedLong(value)).getValue();
            if (transferred > maxRateValue) {
                final long amountOver = transferred - maxRateValue;
                // determine how long it should take to transfer 'amountOver' and 'penalize' the Throttle for that long
                final double pct = (double) amountOver / (double) maxRateValue;
                this.penalizationPeriod = (long) (timePeriodMillis * pct);

                if (logger.isDebugEnabled()) {
                    logger.debug("allowing rate of {} through but penalizing Throttle for {} milliseconds", new Object[]{value, penalizationPeriod});
                }
            }

            lastUpdateTime = now;
            return true;
        }
    }

    private class ThrottleFilter implements FlowFileFilter {

        private final int flowFilesPerBatch;
        private int flowFilesInBatch = 0;

        ThrottleFilter(final int maxFFPerBatch) {
            flowFilesPerBatch = maxFFPerBatch;
        }

        @Override
        public FlowFileFilterResult filter(FlowFile flowFile) {
            long accrual = getFlowFileAccrual(flowFile);
            if (accrual < 0) {
                // this FlowFile is invalid for this configuration so let the processor deal with it
                return FlowFileFilterResult.ACCEPT_AND_TERMINATE;
            }

            String groupName = (groupingAttributeName == null) ? DEFAULT_GROUP_ATTRIBUTE : flowFile.getAttribute(groupingAttributeName);

            // the flow file may not have the required attribute: in this case it is considered part
            // of the DEFAULT_GROUP_ATTRIBUTE
            if (groupName == null) {
                groupName = DEFAULT_GROUP_ATTRIBUTE;
            }

            Throttle throttle = throttleMap.get(groupName);
            if (throttle == null) {
                throttle = new Throttle(timePeriodSeconds, TimeUnit.SECONDS, getLogger());

                final long newRate;
                if (DataUnit.DATA_SIZE_PATTERN.matcher(maximumRateStr).matches()) {
                    newRate = DataUnit.parseDataSize(maximumRateStr, DataUnit.B).longValue();
                } else {
                    newRate = Long.parseLong(maximumRateStr);
                }
                throttle.setMaxRate(newRate);

                throttleMap.put(groupName, throttle);
            }

            throttle.lock();
            try {
                if (throttle.tryAdd(accrual)) {
                    flowFilesInBatch += 1;
                    if (flowFilesInBatch>= flowFilesPerBatch) {
                        flowFilesInBatch = 0;
                        return FlowFileFilterResult.ACCEPT_AND_TERMINATE;
                    } else {
                        return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
                    }
                }
            } finally {
                throttle.unlock();
            }

            // If we are not using a grouping attribute, then no FlowFile will be able to continue on. So we can
            // just TERMINATE the iteration over FlowFiles.
            // However, if we are using a grouping attribute, then another FlowFile in the queue may be able to proceed,
            // so we want to continue our iteration.
            if (groupingAttributeName == null) {
                return FlowFileFilterResult.REJECT_AND_TERMINATE;
            }

            return FlowFileFilterResult.REJECT_AND_CONTINUE;
        }
    }
}
