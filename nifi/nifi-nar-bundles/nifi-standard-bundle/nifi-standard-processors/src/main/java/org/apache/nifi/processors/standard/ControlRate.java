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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.timebuffer.EntityAccess;
import org.apache.nifi.util.timebuffer.TimedBuffer;

@SideEffectFree
@TriggerSerially
@Tags({"rate control", "throttle", "rate", "throughput"})
@CapabilityDescription("Controls the rate at which data is transferred to follow-on processors.")
public class ControlRate extends AbstractProcessor {

    public static final String DATA_RATE = "data rate";
    public static final String FLOWFILE_RATE = "flowfile count";
    public static final String ATTRIBUTE_RATE = "attribute value";

    public static final PropertyDescriptor RATE_CONTROL_CRITERIA = new PropertyDescriptor.Builder()
            .name("Rate Control Criteria")
            .description("Indicates the criteria that is used to control the throughput rate. Changing this value resets the rate counters.")
            .required(true)
            .allowableValues(DATA_RATE, FLOWFILE_RATE, ATTRIBUTE_RATE)
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
                    + "The value of the attribute referenced by this property must be a positive integer, or the FlowFile will be routed to failure. "
                    + "This value is ignored if Rate Control Criteria is not set to 'attribute value'. Changing this value resets the rate counters.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(false)
            .build();
    public static final PropertyDescriptor TIME_PERIOD = new PropertyDescriptor.Builder()
            .name("Time Duration")
            .description("The amount of time to which the Maximum Data Size and Maximum Number of Files pertains. Changing this value resets the rate counters.")
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
            .expressionLanguageSupported(false)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles are transferred to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles will be routed to this relationship if they are missing a necessary attribute or the attribute is not in the expected format")
            .build();

    private static final Pattern POSITIVE_LONG_PATTERN = Pattern.compile("0*[1-9][0-9]*");
    private static final String DEFAULT_GROUP_ATTRIBUTE = ControlRate.class.getName() + "###____DEFAULT_GROUP_ATTRIBUTE___###";

    private final ConcurrentMap<String, Throttle> throttleMap = new ConcurrentHashMap<>();
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private final AtomicLong lastThrottleClearTime = new AtomicLong(System.currentTimeMillis());

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
            if (DataUnit.DATA_SIZE_PATTERN.matcher(newValue).matches()) {
                newRate = DataUnit.parseDataSize(newValue, DataUnit.B).longValue();
            } else {
                newRate = Long.parseLong(newValue);
            }

            for (final Throttle throttle : throttleMap.values()) {
                throttle.setMaxRate(newRate);
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
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

        // TODO: Should periodically clear any Throttle that has not been used in more than 2 throttling periods
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ProcessorLog logger = getLogger();
        final long seconds = context.getProperty(TIME_PERIOD).asTimePeriod(TimeUnit.SECONDS);
        final String rateControlAttributeName = context.getProperty(RATE_CONTROL_ATTRIBUTE_NAME).getValue();
        long rateValue;
        switch (context.getProperty(RATE_CONTROL_CRITERIA).getValue().toLowerCase()) {
            case DATA_RATE:
                rateValue = flowFile.getSize();
                break;
            case FLOWFILE_RATE:
                rateValue = 1;
                break;
            case ATTRIBUTE_RATE:
                final String attributeValue = flowFile.getAttribute(rateControlAttributeName);
                if (attributeValue == null) {
                    logger.error("routing {} to 'failure' because FlowFile is missing required attribute {}", new Object[]{flowFile, rateControlAttributeName});
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }

                if (!POSITIVE_LONG_PATTERN.matcher(attributeValue).matches()) {
                    logger.error("routing {} to 'failure' because FlowFile attribute {} has a value of {}, which is not a positive integer",
                            new Object[]{flowFile, rateControlAttributeName, attributeValue});
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
                rateValue = Long.parseLong(attributeValue);
                break;
            default:
                throw new AssertionError("<Rate Control Criteria> property set to illegal value of " + context.getProperty(RATE_CONTROL_CRITERIA).getValue());
        }

        final String groupingAttributeName = context.getProperty(GROUPING_ATTRIBUTE_NAME).getValue();
        final String groupName = (groupingAttributeName == null) ? DEFAULT_GROUP_ATTRIBUTE : flowFile.getAttribute(groupingAttributeName);
        Throttle throttle = throttleMap.get(groupName);
        if (throttle == null) {
            throttle = new Throttle((int) seconds, TimeUnit.SECONDS, logger);

            final String maxRateValue = context.getProperty(MAX_RATE).getValue();
            final long newRate;
            if (DataUnit.DATA_SIZE_PATTERN.matcher(maxRateValue).matches()) {
                newRate = DataUnit.parseDataSize(maxRateValue, DataUnit.B).longValue();
            } else {
                newRate = Long.parseLong(maxRateValue);
            }
            throttle.setMaxRate(newRate);

            throttleMap.put(groupName, throttle);
        }

        throttle.lock();
        try {
            if (throttle.tryAdd(rateValue)) {
                logger.info("transferring {} to 'success'", new Object[]{flowFile});
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile);
            }
        } finally {
            throttle.unlock();
        }
    }

    private static class TimestampedLong {

        private final Long value;
        private final long timestamp = System.currentTimeMillis();

        public TimestampedLong(final Long value) {
            this.value = value;
        }

        public Long getValue() {
            return value;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    private static class RateEntityAccess implements EntityAccess<TimestampedLong> {

        @Override
        public TimestampedLong aggregate(TimestampedLong oldValue, TimestampedLong toAdd) {
            if (oldValue == null && toAdd == null) {
                return new TimestampedLong(0L);
            } else if (oldValue == null) {
                return toAdd;
            } else if (toAdd == null) {
                return oldValue;
            }

            return new TimestampedLong(oldValue.getValue() + toAdd.getValue());
        }

        @Override
        public TimestampedLong createNew() {
            return new TimestampedLong(0L);
        }

        @Override
        public long getTimestamp(TimestampedLong entity) {
            return entity == null ? 0L : entity.getTimestamp();
        }
    }

    private static class Throttle extends ReentrantLock {

        private final AtomicLong maxRate = new AtomicLong(1L);
        private final long timePeriodValue;
        private final TimeUnit timePeriodUnit;
        private final TimedBuffer<TimestampedLong> timedBuffer;
        private final ProcessorLog logger;

        private volatile long penalizationExpired;
        private volatile long lastUpdateTime;

        public Throttle(final int timePeriod, final TimeUnit unit, final ProcessorLog logger) {
            this.timePeriodUnit = unit;
            this.timePeriodValue = timePeriod;
            this.timedBuffer = new TimedBuffer<>(unit, timePeriod, new RateEntityAccess());
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

            final TimestampedLong sum = timedBuffer.getAggregateValue(TimeUnit.MILLISECONDS.convert(timePeriodValue, timePeriodUnit));
            if (sum != null && sum.getValue() >= maxRateValue) {
                logger.debug("current sum for throttle is {}, so not allowing rate of {} through", new Object[]{sum.getValue(), value});
                return false;
            }

            logger.debug("current sum for throttle is {}, so allowing rate of {} through",
                    new Object[]{sum == null ? 0 : sum.getValue(), value});

            final long transferred = timedBuffer.add(new TimestampedLong(value)).getValue();
            if (transferred > maxRateValue) {
                final long amountOver = transferred - maxRateValue;
                // determine how long it should take to transfer 'amountOver' and 'penalize' the Throttle for that long
                final long milliDuration = TimeUnit.MILLISECONDS.convert(timePeriodValue, timePeriodUnit);
                final double pct = (double) amountOver / (double) maxRateValue;
                final long penalizationPeriod = (long) (milliDuration * pct);
                this.penalizationExpired = now + penalizationPeriod;
                logger.debug("allowing rate of {} through but penalizing Throttle for {} milliseconds", new Object[]{value, penalizationPeriod});
            }

            lastUpdateTime = now;
            return true;
        }
    }
}
