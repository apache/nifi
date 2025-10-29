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

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.util.MockPropertyContext;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import java.util.Map;

import static org.apache.nifi.processors.aws.region.RegionUtil.CUSTOM_REGION;
import static org.apache.nifi.processors.aws.region.RegionUtil.REGION;
import static org.apache.nifi.processors.aws.region.RegionUtil.USE_CUSTOM_REGION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RegionUtilTest {

    @Test
    void testGetRegionWithAwsRegion() {
        String region = Region.US_WEST_2.id();

        PropertyContext propertyContext = new MockPropertyContext(Map.of(
                REGION, region
        ));

        assertEquals(region, RegionUtil.getRegion(propertyContext).id());
    }

    @Test
    void testGetRegionWithStaticCustomRegion() {
        String region = "non-aws-region";

        PropertyContext propertyContext = new MockPropertyContext(Map.of(
                REGION, USE_CUSTOM_REGION.getValue(),
                CUSTOM_REGION, region
        ));

        assertEquals(region, RegionUtil.getRegion(propertyContext).id());
    }

    @Test
    void testGetRegionWithDynamicCustomRegion() {
        String region = Region.US_WEST_2.id();
        String flowFileAttributeName = "s3.region";

        PropertyContext propertyContext = new MockPropertyContext(Map.of(
                REGION, USE_CUSTOM_REGION.getValue(),
                CUSTOM_REGION, String.format("${%s}", flowFileAttributeName)
        ));

        Map<String, String> flowFileAttributes = Map.of(
                flowFileAttributeName, region
        );

        assertEquals(region, RegionUtil.getRegion(propertyContext, flowFileAttributes).id());
    }


    @Test
    void testIsDynamicRegionWithAwsRegion() {
        String region = Region.US_WEST_2.id();

        PropertyContext propertyContext = new MockPropertyContext(Map.of(
                REGION, region
        ));

        assertFalse(RegionUtil.isDynamicRegion(propertyContext));
    }

    @Test
    void testIsDynamicRegionWithStaticCustomRegion() {
        String region = "non-aws-region";

        PropertyContext propertyContext = new MockPropertyContext(Map.of(
                REGION, USE_CUSTOM_REGION.getValue(),
                CUSTOM_REGION, region
        ));

        assertFalse(RegionUtil.isDynamicRegion(propertyContext));
    }

    @Test
    void testIsDynamicRegionWithDynamicCustomRegion() {
        String flowFileAttributeName = "s3.region";

        PropertyContext propertyContext = new MockPropertyContext(Map.of(
                REGION, USE_CUSTOM_REGION.getValue(),
                CUSTOM_REGION, String.format("${%s}", flowFileAttributeName)
        ));

        assertTrue(RegionUtil.isDynamicRegion(propertyContext));
    }
}
