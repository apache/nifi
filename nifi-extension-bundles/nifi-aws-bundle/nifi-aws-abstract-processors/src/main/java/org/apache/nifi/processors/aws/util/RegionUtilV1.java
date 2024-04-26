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
package org.apache.nifi.processors.aws.util;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Arrays;
import java.util.Map;

/**
 * Utility class for AWS region methods. This class uses AWS SDK v1.
 *
 */
public final class RegionUtilV1 {

    private RegionUtilV1() {
    }

    public static final String S3_REGION_ATTRIBUTE = "s3.region" ;
    public static final AllowableValue ATTRIBUTE_DEFINED_REGION = new AllowableValue("attribute-defined-region",
            "Use '" + S3_REGION_ATTRIBUTE + "' Attribute",
            "Uses '" + S3_REGION_ATTRIBUTE + "' FlowFile attribute as region.");

    public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
            .name("Region")
            .description("The AWS Region to connect to.")
            .required(true)
            .allowableValues(getAvailableRegions())
            .defaultValue(createAllowableValue(Regions.DEFAULT_REGION).getValue())
            .build();

    public static final PropertyDescriptor S3_REGION = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(REGION)
            .allowableValues(getAvailableS3Regions())
            .build();

    public static Region resolveS3Region(final PropertyContext context, final Map<String, String> attributes) {
        String regionValue = context.getProperty(S3_REGION).getValue();

        if (ATTRIBUTE_DEFINED_REGION.getValue().equals(regionValue)) {
            regionValue = attributes.get(S3_REGION_ATTRIBUTE);
        }

        return parseS3RegionValue(regionValue);
    }

    public static AllowableValue[] getAvailableS3Regions() {
        final AllowableValue[] availableRegions = getAvailableRegions();
        return ArrayUtils.add(availableRegions, ATTRIBUTE_DEFINED_REGION);
    }

    public static AllowableValue createAllowableValue(final Regions region) {
        return new AllowableValue(region.getName(), region.getDescription(), "AWS Region Code : " + region.getName());
    }

    public static AllowableValue[] getAvailableRegions() {
        return Arrays.stream(Regions.values())
                .map(RegionUtilV1::createAllowableValue)
                .toArray(AllowableValue[]::new);
    }

    private static Region parseS3RegionValue(String regionValue) {
        if (regionValue == null) {
            throw new ProcessException(String.format("[%s] was selected as region source but [%s] attribute does not exist", ATTRIBUTE_DEFINED_REGION, S3_REGION_ATTRIBUTE));
        }

        try {
            return Region.getRegion(Regions.fromName(regionValue));
        } catch (Exception e) {
            throw new ProcessException(String.format("The [%s] attribute contains an invalid region value [%s]", S3_REGION_ATTRIBUTE, regionValue), e);
        }
    }

}
