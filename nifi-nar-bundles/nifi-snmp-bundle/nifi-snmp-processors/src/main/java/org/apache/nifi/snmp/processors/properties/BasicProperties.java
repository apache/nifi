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
package org.apache.nifi.snmp.processors.properties;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public class BasicProperties {

    private BasicProperties() {
        // Utility class, not needed to instantiate.
    }

    public static final AllowableValue SNMP_V1 = new AllowableValue("SNMPv1", "v1", "SNMP version 1");
    public static final AllowableValue SNMP_V2C = new AllowableValue("SNMPv2c", "v2c", "SNMP version 2c");
    public static final AllowableValue SNMP_V3 = new AllowableValue("SNMPv3", "v3", "SNMP version 3 with improved security");


    public static final PropertyDescriptor SNMP_VERSION = new PropertyDescriptor.Builder()
            .name("snmp-version")
            .displayName("SNMP Version")
            .description("Three significant versions of SNMP have been developed and deployed. " +
                    "SNMPv1 is the original version of the protocol. More recent versions, " +
                    "SNMPv2c and SNMPv3, feature improvements in performance, flexibility and security.")
            .required(true)
            .allowableValues(SNMP_V1, SNMP_V2C, SNMP_V3)
            .defaultValue(SNMP_V1.getValue())
            .build();

    public static final PropertyDescriptor SNMP_COMMUNITY = new PropertyDescriptor.Builder()
            .name("snmp-community")
            .displayName("SNMP Community")
            .description("SNMPv1 and SNMPv2 use communities to establish trust between managers and agents." +
                    " Most agents support three community names, one each for read-only, read-write and trap." +
                    " These three community strings control different types of activities. The read-only community" +
                    " applies to get requests. The read-write community string applies to set requests. The trap" +
                    " community string applies to receipt of traps.")
            .required(true)
            .sensitive(true)
            .defaultValue("public")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(SNMP_VERSION, SNMP_V1, SNMP_V2C)
            .build();

    public static final PropertyDescriptor SNMP_RETRIES = new PropertyDescriptor.Builder()
            .name("snmp-retries")
            .displayName("Number of Retries")
            .description("Set the number of retries when requesting the SNMP Agent.")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor SNMP_TIMEOUT = new PropertyDescriptor.Builder()
            .name("snmp-timeout")
            .displayName("Timeout (ms)")
            .description("Set the timeout in ms when requesting the SNMP Agent.")
            .required(false)
            .defaultValue("5000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
}
