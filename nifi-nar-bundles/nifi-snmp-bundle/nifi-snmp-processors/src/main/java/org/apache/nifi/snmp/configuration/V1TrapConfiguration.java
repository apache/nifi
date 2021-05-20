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
package org.apache.nifi.snmp.configuration;

import org.apache.nifi.util.StringUtils;

public class V1TrapConfiguration {

    private final String enterpriseOid;
    private final String agentAddress;
    private final String genericTrapType;
    private final String specificTrapType;

    private V1TrapConfiguration(final V1TrapConfiguration.Builder builder) {
        this.enterpriseOid = builder.enterpriseOid;
        this.agentAddress = builder.agentAddress;
        this.genericTrapType = builder.genericTrapType;
        this.specificTrapType = builder.specificTrapType;
    }

    public String getEnterpriseOid() {
        return enterpriseOid;
    }

    public String getAgentAddress() {
        return agentAddress;
    }

    public int getGenericTrapType() {
        return Integer.parseInt(genericTrapType);
    }

    public Integer getSpecificTrapType() {
        if (StringUtils.isNotEmpty(specificTrapType)) {
            return Integer.parseInt(specificTrapType);
        }
        return null;
    }

    public static V1TrapConfiguration.Builder builder() {
        return new V1TrapConfiguration.Builder();
    }

    public static final class Builder {
        String enterpriseOid;
        String agentAddress;
        String genericTrapType;
        String specificTrapType;

        public Builder enterpriseOid(String enterpriseOid) {
            this.enterpriseOid = enterpriseOid;
            return this;
        }

        public Builder agentAddress(String agentAddress) {
            this.agentAddress = agentAddress;
            return this;
        }

        public Builder genericTrapType(String genericTrapType) {
            this.genericTrapType = genericTrapType;
            return this;
        }

        public Builder specificTrapType(String specificTrapType) {
            this.specificTrapType = specificTrapType;
            return this;
        }

        public V1TrapConfiguration build() {
            if (StringUtils.isEmpty(enterpriseOid)) {
                throw new IllegalArgumentException("Enterprise OID must be specified.");
            }
            if (StringUtils.isEmpty(agentAddress)) {
                throw new IllegalArgumentException("Agent address must be specified.");
            }

            final int parsedGenericTrapType;
            try {
                parsedGenericTrapType = Integer.parseInt(genericTrapType);
                if (parsedGenericTrapType < 0 || parsedGenericTrapType > 6) {
                    throw new IllegalArgumentException("Generic Trap Type must be between 0 and 6.");
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Generic Trap Type is not a number.");
            }

            if (parsedGenericTrapType == 6) {
                try {
                    final int parsedSpecificTrapType = Integer.parseInt(specificTrapType);
                    if (parsedSpecificTrapType < 0) {
                        throw new IllegalArgumentException("Specific Trap Type must be between 0 and 2147483647.");
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Generic Trap Type is [6 - Enterprise Specific] but Specific Trap Type is not provided or not a number.");
                }
            } else if (StringUtils.isNotEmpty(specificTrapType)) {
                throw new IllegalArgumentException("Invalid argument: Generic Trap Type is not [6 - Enterprise Specific] but Specific Trap Type is provided.");
            }
            return new V1TrapConfiguration(this);
        }
    }
}
