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
package org.apache.nifi.snmp.factory;

import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.mp.SnmpConstants;

public class V1SNMPFactory extends AbstractSNMPFactory implements SNMPFactory {

    @Override
    public boolean supports(final int version) {
        return SnmpConstants.version1 == version;
    }

    @Override
    public Snmp createSnmpManagerInstance(final SNMPConfiguration configuration) {
        return createSnmpClient();
    }

    @Override
    public Target createTargetInstance(final SNMPConfiguration configuration) {
        return createCommunityTarget(configuration);
    }
}
