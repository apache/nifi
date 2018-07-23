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
package org.apache.nifi.processors.aws.regions;

public enum AWSRegions {

    GovCloud("us-gov-west-1", "AWS GovCloud (US)"),
    US_EAST_1("us-east-1", "US East (N. Virginia)"),
    US_EAST_2("us-east-2", "US East (Ohio)"),
    US_WEST_1("us-west-1", "US West (N. California)"),
    US_WEST_2("us-west-2", "US West (Oregon)"),
    EU_WEST_1("eu-west-1", "EU (Ireland)"),
    EU_WEST_2("eu-west-2", "EU (London)"),
    EU_WEST_3("eu-west-3", "EU (Paris)"),
    EU_CENTRAL_1("eu-central-1", "EU (Frankfurt)"),
    AP_SOUTH_1("ap-south-1", "Asia Pacific (Mumbai)"),
    AP_SOUTHEAST_1("ap-southeast-1", "Asia Pacific (Singapore)"),
    AP_SOUTHEAST_2("ap-southeast-2", "Asia Pacific (Sydney)"),
    AP_NORTHEAST_1("ap-northeast-1", "Asia Pacific (Tokyo)"),
    AP_NORTHEAST_2("ap-northeast-2", "Asia Pacific (Seoul)"),
    SA_EAST_1("sa-east-1", "South America (Sao Paulo)"),
    CN_NORTH_1("cn-north-1", "China (Beijing)"),
    CN_NORTHWEST_1("cn-northwest-1", "China (Ningxia)"),
    CA_CENTRAL_1("ca-central-1", "Canada (Central)");

    private final String regionCode;
    private final String regionDisplayName;

    AWSRegions(String regionCode, String regionDisplayName) {
        this.regionCode = regionCode;
        this.regionDisplayName = regionDisplayName;
    }

    public static String getRegionDisplayName(String regionCode) {
        for (AWSRegions regions : AWSRegions.values()) {
            if (regions.regionCode.equalsIgnoreCase(regionCode)) {
                return regions.regionDisplayName;
            }
        }

        return regionCode;
    }

}
