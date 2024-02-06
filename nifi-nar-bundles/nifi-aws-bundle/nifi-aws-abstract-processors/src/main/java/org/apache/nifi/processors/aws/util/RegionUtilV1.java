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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Map;

import static org.apache.nifi.processors.aws.s3.AbstractS3Processor.S3_REGION;

/**
 * Utility class for AWS region methods. This class uses AWS SDK v1.
 *
 */
public abstract class RegionUtilV1 {

    public static final String S3_REGION_ATTRIBUTE = "s3.region" ;
    static final AllowableValue ATTRIBUTE_DEFINED_REGION = new AllowableValue("attribute-defined-region",
            "Use '" + S3_REGION_ATTRIBUTE + "' Attribute",
            "Uses '" + S3_REGION_ATTRIBUTE + "' FlowFile attribute as region.");

    public static Region parseRegionValue(String regionValue) {
        if (regionValue == null) {
            throw new ProcessException(String.format("[%s] was selected as region source but [%s] attribute does not exist", ATTRIBUTE_DEFINED_REGION, S3_REGION_ATTRIBUTE));
        }

        try {
            return Region.getRegion(Regions.fromName(regionValue));
        } catch (Exception e) {
            throw new ProcessException(String.format("The [%s] attribute contains an invalid region value [%s]", S3_REGION_ATTRIBUTE, regionValue), e);
        }
    }

    public static Region resolveRegion(final PropertyContext context, final Map<String, String> attributes) {
        String regionValue = context.getProperty(S3_REGION).getValue();

        if (ATTRIBUTE_DEFINED_REGION.getValue().equals(regionValue)) {
            regionValue = attributes.get(S3_REGION_ATTRIBUTE);
        }

        return parseRegionValue(regionValue);
    }
}
