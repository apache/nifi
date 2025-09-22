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
package org.apache.nifi.controller;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.util.FormatUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Reporting task used to monitor usage of memory after Garbage Collection has
 * been performed.
 *
 * Each time this ReportingTask runs, it checks the amount of memory that was
 * used in the configured Memory Pool after the last garbage collection took
 * place. If the amount of memory used exceeds a configurable threshold, a
 * System-Level Bulletin is generated and a message is logged. If the memory
 * usage did not exceed the configured threshold but the previous iteration did,
 * an INFO level message is logged and an associated bulletin published.
 *
 * The following properties are supported:
 *
 * <ul>
 * <li><b>Memory Pool</b> - The name of the JVM Memory Pool. The allowed values
 * depend on the JVM and operating system.
 *
 * <p>
 * <br />The following values are typically supported:
 * <ul>
 * <li>PS Old Gen</li>
 * <li>PS Survivor Space</li>
 * <li>PS Eden Space</li>
 * <li>PS Perm Gen</li>
 * </ul>
 * </li>
 * <li>
 * <b>Usage Threshold</b> - The threshold for memory usage that will cause a
 * notification to occur. The format can either be a percentage (e.g., 80%) or a
 * Data Size (e.g., 1 GB)
 * </li>
 * <li>
 * <b>Reporting Interval</b> - How often a notification should occur in the
 * event that the memory usage exceeds the threshold. This differs from the
 * scheduling period such that having a short value for the scheduling period
 * and a long value for the reportingInterval property will result in checking
 * the memory usage often so that notifications happen quickly but prevents
 * notifications from continually being generated. The format of this property
 * is The Period format (e.g., 5 mins).
 * </li>
 * </ul>
 */
@Tags({"monitor", "memory", "heap", "jvm", "gc", "garbage collection", "warning"})
@CapabilityDescription("Checks the amount of Java Heap available in the JVM for a particular JVM Memory Pool. If the"
        + " amount of space used exceeds some configurable threshold, will warn (via a log message and System-Level Bulletin)"
        + " that the memory pool is exceeding this threshold.")
public class MonitorMemory extends AbstractReportingTask {

    private static final List<String> GC_OLD_GEN_POOLS = Collections.unmodifiableList(Arrays.asList("Tenured Gen", "PS Old Gen", "G1 Old Gen", "CMS Old Gen", "ZHeap"));
    private static final AllowableValue[] memPoolAllowableValues;
    private static String defaultMemoryPool;

    static {
        // Only allow memory pool beans that support usage thresholds, otherwise we wouldn't report anything anyway
        memPoolAllowableValues = ManagementFactory.getMemoryPoolMXBeans()
                .stream()
                .filter(MemoryPoolMXBean::isCollectionUsageThresholdSupported)
                .map(MemoryPoolMXBean::getName)
                .map(AllowableValue::new)
                .toArray(AllowableValue[]::new);
        defaultMemoryPool = Arrays.stream(memPoolAllowableValues)
                .map(AllowableValue::getValue)
                .filter(GC_OLD_GEN_POOLS::contains)
                .findFirst()
                .orElse(null);
    }

    public static final PropertyDescriptor MEMORY_POOL_PROPERTY = new PropertyDescriptor.Builder()
            .name("Memory Pool")
            .description("The name of the JVM Memory Pool to monitor. The allowed values for Memory Pools are platform and JVM"
                    + " dependent and may vary for different versions of Java and from published documentation. This reporting"
                    + " task will become invalidated if configured to use a Memory Pool that is not available on the currently"
                    + " running host platform and JVM")
            .required(true)
            .allowableValues(memPoolAllowableValues)
            .defaultValue(defaultMemoryPool)
            .build();
    public static final PropertyDescriptor THRESHOLD_PROPERTY = new PropertyDescriptor.Builder()
            .name("Usage Threshold")
            .description("Indicates the threshold at which warnings should be generated. This can be a percentage or a Data Size")
            .required(true)
            .addValidator(new ThresholdValidator())
            .defaultValue("65%")
            .build();
    public static final PropertyDescriptor REPORTING_INTERVAL = new PropertyDescriptor.Builder()
            .name("Reporting Interval")
            .description("Indicates how often this reporting task should report bulletins while the memory utilization exceeds the configured threshold")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final Pattern PERCENTAGE_PATTERN = Pattern.compile("\\d{1,2}%");
    public static final Pattern DATA_SIZE_PATTERN = DataUnit.DATA_SIZE_PATTERN;
    public static final Pattern TIME_PERIOD_PATTERN = FormatUtils.TIME_DURATION_PATTERN;

    private volatile MemoryPoolMXBean monitoredBean;
    private volatile String threshold = "65%";
    private volatile long calculatedThreshold;
    private volatile long lastReportTime;
    private volatile long reportingIntervalMillis;
    private volatile boolean lastValueWasExceeded;

    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(MEMORY_POOL_PROPERTY);
        _propertyDescriptors.add(THRESHOLD_PROPERTY);
        _propertyDescriptors.add(REPORTING_INTERVAL);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public void onConfigured(final ConfigurationContext config) throws InitializationException {
        final String desiredMemoryPoolName = config.getProperty(MEMORY_POOL_PROPERTY).getValue();
        final String thresholdValue = config.getProperty(THRESHOLD_PROPERTY).getValue().trim();
        threshold = thresholdValue;

        final Long reportingIntervalValue = config.getProperty(REPORTING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
        if (reportingIntervalValue == null) {
            reportingIntervalMillis = config.getSchedulingPeriod(TimeUnit.MILLISECONDS);
        } else {
            reportingIntervalMillis = reportingIntervalValue;
        }

        final List<MemoryPoolMXBean> memoryPoolBeans = ManagementFactory.getMemoryPoolMXBeans();
        for (int i = 0; i < memoryPoolBeans.size() && monitoredBean == null; i++) {
            MemoryPoolMXBean memoryPoolBean = memoryPoolBeans.get(i);
            String memoryPoolName = memoryPoolBean.getName();
            if (desiredMemoryPoolName.equals(memoryPoolName)) {
                monitoredBean = memoryPoolBean;
                if (memoryPoolBean.isCollectionUsageThresholdSupported()) {
                    if (DATA_SIZE_PATTERN.matcher(thresholdValue).matches()) {
                        calculatedThreshold = DataUnit.parseDataSize(thresholdValue, DataUnit.B).longValue();
                    } else {
                        final String percentage = thresholdValue.substring(0, thresholdValue.length() - 1);
                        final double pct = Double.parseDouble(percentage) / 100D;
                        calculatedThreshold = (long) (monitoredBean.getCollectionUsage().getMax() * pct);
                    }

                    if (monitoredBean.isCollectionUsageThresholdSupported()) {
                        monitoredBean.setCollectionUsageThreshold(calculatedThreshold);
                    }
                }
            }
        }

        if (monitoredBean == null) {
            throw new InitializationException("Found no JVM Memory Pool with name " + desiredMemoryPoolName + "; will not monitor Memory Pool");
        }
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final MemoryPoolMXBean bean = monitoredBean;
        if (bean == null) {
            return;
        }

        final MemoryUsage usage = bean.getCollectionUsage();
        if (usage == null) {
            getLogger().warn("{} could not determine memory usage for pool with name {}", this, context.getProperty(MEMORY_POOL_PROPERTY));
            return;
        }

        final double percentageUsed = (double) usage.getUsed() / (double) usage.getMax() * 100D;
        // In certain scenarios in the monitored memory bean the gcSensor can get stuck in 'on' state before the usage would reach the threshold
        // and this will cause false exceeded state until the next garbage collection. To eliminate this we are adding a condition with the calculated usage threshold.
        if (bean.isCollectionUsageThresholdSupported() && bean.isCollectionUsageThresholdExceeded() && usage.getUsed() > calculatedThreshold) {
            if (System.currentTimeMillis() < reportingIntervalMillis + lastReportTime && lastReportTime > 0L) {
                return;
            }

            lastReportTime = System.currentTimeMillis();
            lastValueWasExceeded = true;
            final String message = String.format("Memory Pool '%1$s' has exceeded the configured Threshold of %2$s, having used %3$s / %4$s (%5$.2f%%)",
                    bean.getName(), threshold, FormatUtils.formatDataSize(usage.getUsed()),
                    FormatUtils.formatDataSize(usage.getMax()), percentageUsed);

            getLogger().warn("{}", message);
        } else if (lastValueWasExceeded) {
            lastValueWasExceeded = false;
            lastReportTime = System.currentTimeMillis();
            final String message = String.format("Memory Pool '%1$s' is no longer exceeding the configured Threshold of %2$s; currently using %3$s / %4$s (%5$.2f%%)",
                    bean.getName(), threshold, FormatUtils.formatDataSize(usage.getUsed()),
                    FormatUtils.formatDataSize(usage.getMax()), percentageUsed);

            getLogger().info("{}", message);
        }
    }

    @OnStopped
    public void onStopped() {
        monitoredBean = null;
    }

    private static class ThresholdValidator implements Validator {

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {

            if (!PERCENTAGE_PATTERN.matcher(input).matches() && !DATA_SIZE_PATTERN.matcher(input).matches()) {
                return new ValidationResult.Builder().input(input).subject(subject).valid(false)
                        .explanation("Valid value is a number in the range of 0-99 followed by a percent sign (e.g. 65%) or a Data Size (e.g. 100 MB)").build();
            }

            return new ValidationResult.Builder().input(input).subject(subject).valid(true).build();
        }
    }
}
