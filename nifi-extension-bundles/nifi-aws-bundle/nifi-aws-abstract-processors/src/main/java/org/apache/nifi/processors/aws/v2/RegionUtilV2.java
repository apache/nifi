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
package org.apache.nifi.processors.aws.v2;

import org.apache.nifi.components.AllowableValue;
import software.amazon.awssdk.regions.Region;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Utility class for AWS region methods. This class uses AWS SDK v2.
 *
 */
public final class RegionUtilV2 {

    private RegionUtilV2() {
    }

    /**
     * Creates an AllowableValue from a Region.
     * @param region An AWS region
     * @return An AllowableValue for the region
     */
    public static AllowableValue createAllowableValue(final Region region) {
        final String description = region.metadata() != null ? region.metadata().description() : region.id();
        return new AllowableValue(region.id(), description, "AWS Region Code : " + region.id());
    }

    /**
     *
     * @return All available regions as AllowableValues.
     */
    public static AllowableValue[] getAvailableRegions() {
        final List<AllowableValue> values = new ArrayList<>();
        for (final Region region : Region.regions()) {
            values.add(createAllowableValue(region));
        }
        Collections.sort(values, Comparator.comparing(AllowableValue::getDisplayName));
        return values.toArray(new AllowableValue[0]);
    }

}