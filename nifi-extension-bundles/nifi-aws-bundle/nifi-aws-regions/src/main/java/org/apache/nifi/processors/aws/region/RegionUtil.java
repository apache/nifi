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
package org.apache.nifi.processors.aws.region;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Utility class for AWS region methods.
 *
 */
public final class RegionUtil {

    private RegionUtil() { }

    public static final AllowableValue USE_CUSTOM_REGION = new AllowableValue("use-custom-region",
            "Use Custom Region",
            "User defined Region that can be a non-AWS region and/or come from Expression Language");

    public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
            .name("Region")
            .description("AWS Region in which the service is located")
            .required(true)
            .allowableValues(getRegionAllowableValues())
            .defaultValue(createAwsRegionAllowableValue(Region.US_WEST_2).getValue())
            .build();

    public static final PropertyDescriptor CUSTOM_REGION = new PropertyDescriptor.Builder()
            .name("Custom Region")
            .description("Custom region, e.g. a region of an AWS compatible service provider")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dependsOn(REGION, USE_CUSTOM_REGION)
            .build();

    public static final PropertyDescriptor CUSTOM_REGION_WITH_FF_EL = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(CUSTOM_REGION)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static Region getRegion(final PropertyContext context) {
        return getRegion(context, Map.of());
    }

    public static Region getRegion(final PropertyContext context, final Map<String, String> attributes) {
        final Region region;

        final String regionValue = context.getProperty(REGION).getValue();
        if (regionValue != null) {
            if (USE_CUSTOM_REGION.getValue().equals(regionValue)) {
                final String customRegionValue = context.getProperty(CUSTOM_REGION).evaluateAttributeExpressions(attributes).getValue();
                region = Region.of(customRegionValue);
            } else {
                region = Region.of(regionValue);
            }
        } else {
            region = null;
        }

        return region;
    }

    public static boolean isDynamicRegion(final PropertyContext context) {
        String regionValue = context.getProperty(REGION).getValue();
        return USE_CUSTOM_REGION.getValue().equals(regionValue)
                && context.getProperty(CUSTOM_REGION).isExpressionLanguagePresent();
    }

    private static AllowableValue[] getRegionAllowableValues() {
        final List<AllowableValue> values = getAwsRegionAllowableValues();
        values.add(USE_CUSTOM_REGION);
        return values.toArray(new AllowableValue[0]);
    }

    /**
     *
     * @return All available AWS regions as AllowableValues.
     */
    private static List<AllowableValue> getAwsRegionAllowableValues() {
        final List<AllowableValue> values = new ArrayList<>();
        for (final Region region : Region.regions()) {
            values.add(createAwsRegionAllowableValue(region));
        }
        values.sort(Comparator.comparing(AllowableValue::getDisplayName));
        return values;
    }

    /**
     * Creates an AllowableValue from a Region.
     * @param region An AWS region
     * @return An AllowableValue for the region
     */
    private static AllowableValue createAwsRegionAllowableValue(final Region region) {
        final String description = region.metadata() != null ? region.metadata().description() : region.id();
        return new AllowableValue(region.id(), description, "AWS Region Code : " + region.id());
    }

}
