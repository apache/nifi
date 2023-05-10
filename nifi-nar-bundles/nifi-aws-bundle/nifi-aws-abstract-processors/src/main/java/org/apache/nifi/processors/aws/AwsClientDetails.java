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
package org.apache.nifi.processors.aws;

import com.amazonaws.regions.Region;
import java.util.Objects;

/**
 * This class contains the AWS client details used to distinguish between the various AWS clients stored in the cache.
 * The class acts as a cache key for @link AwsClientCache.
 * AwsClientDetails contains the region only, since actually the region value may come from the FlowFile attributes.
 */
public class AwsClientDetails {
    private Region region;

    public AwsClientDetails(Region region) {
       this.region =  region;
    }

    public Region getRegion() {
        return region;
    }

    public void setRegion(final Region region) {
        this.region = region;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AwsClientDetails that = (AwsClientDetails) o;
        return Objects.equals(region, that.region);
    }

    @Override
    public int hashCode() {
        return Objects.hash(region);
    }
}
