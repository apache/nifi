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
package org.apache.nifi.processors.aws.signer;

import org.apache.nifi.processors.aws.AwsServiceType;

public class AwsCustomSignerContext {

    private final AwsServiceType serviceType;
    private final String regionName;
    private final String endpointUrl;

    private AwsCustomSignerContext(final AwsServiceType serviceType, final String regionName, final String endpointUrl) {
        this.serviceType = serviceType;
        this.regionName = regionName;
        this.endpointUrl = endpointUrl;
    }

    public AwsServiceType getServiceType() {
        return serviceType;
    }

    public String getRegionName() {
        return regionName;
    }

    public String getEndpointUrl() {
        return endpointUrl;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private AwsServiceType serviceType;
        private String regionName;
        private String endpointUrl;

        public Builder setServiceType(AwsServiceType serviceType) {
            this.serviceType = serviceType;
            return this;
        }

        public Builder setRegionName(String regionName) {
            this.regionName = regionName;
            return this;
        }

        public Builder setEndpointUrl(String endpointUrl) {
            this.endpointUrl = endpointUrl;
            return this;
        }

        public AwsCustomSignerContext build() {
            return new AwsCustomSignerContext(serviceType, regionName, endpointUrl);
        }
    }
}
