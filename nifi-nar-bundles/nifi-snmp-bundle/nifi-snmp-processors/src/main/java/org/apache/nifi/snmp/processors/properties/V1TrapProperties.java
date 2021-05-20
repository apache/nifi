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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import static org.apache.nifi.snmp.processors.properties.BasicProperties.SNMP_V1;
import static org.apache.nifi.snmp.processors.properties.BasicProperties.SNMP_VERSION;


public class V1TrapProperties {

    private V1TrapProperties() {
        // Utility class, not needed to instantiate.
    }

    private static final AllowableValue GENERIC_TRAP_TYPE_ENTERPRISE_SPECIFIC = new AllowableValue("6", "Enterprise Specific",
            "An enterpriseSpecific trap signifies that a particular enterprise-specific trap has occurred which " +
                    "can be defined in the Specific Trap Type field.");

    public static final PropertyDescriptor ENTERPRISE_OID = new PropertyDescriptor.Builder()
            .name("snmp-trap-enterprise-oid")
            .displayName("Enterprise OID")
            .description("Enterprise is the vendor identification (OID) for the network management sub-system that generated the trap.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(SNMP_VERSION, SNMP_V1)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor AGENT_ADDRESS = new PropertyDescriptor.Builder()
            .name("snmp-trap-agent-address")
            .displayName("SNMP Trap Agent Address")
            .description("The address where the SNMP Manager sends the trap.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(SNMP_VERSION, SNMP_V1)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor GENERIC_TRAP_TYPE = new PropertyDescriptor.Builder()
            .name("snmp-trap-generic-type")
            .displayName("Generic Trap Type")
            .description("Generic trap type is an integer in the range of 0 to 6. See processor usage for details.")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(0, 6, true))
            .dependsOn(SNMP_VERSION, SNMP_V1)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SPECIFIC_TRAP_TYPE = new PropertyDescriptor.Builder()
            .name("snmp-trap-specific-type")
            .displayName("Specific Trap Type")
            .description("Specific trap type is a number that further specifies the nature of the event that generated " +
                    "the trap in the case of traps of generic type 6 (enterpriseSpecific). The interpretation of this " +
                    "code is vendor-specific.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .dependsOn(SNMP_VERSION, SNMP_V1)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
}
