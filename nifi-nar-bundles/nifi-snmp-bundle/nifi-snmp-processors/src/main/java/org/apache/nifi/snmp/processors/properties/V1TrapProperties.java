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

    static final AllowableValue COLD_START = new AllowableValue("0", "Cold Start",
            "A coldStart trap signifies that the sending protocol entity is reinitializing itself such that the agent's configuration or the protocol" +
                    " entity implementation may be altered");

    static final AllowableValue WARM_START = new AllowableValue("1", "Warm Start",
            "A warmStart trap signifies that the sending protocol entity is reinitializing itself such that neither the agent configuration nor the" +
                    " protocol entity implementation is altered.");

    static final AllowableValue LINK_DOWN = new AllowableValue("2", "Link Down",
            "A linkDown trap signifies that the sending protocol entity recognizes a failure in one of the communication links represented in the agent's" +
                    " configuration.");

    static final AllowableValue LINK_UP = new AllowableValue("3", "Link Up",
            "A linkUp trap signifies that the sending protocol entity recognizes that one of the communication links represented in the agent's configuration" +
                    " has come up.");

    static final AllowableValue AUTHENTICATION_FAILURE = new AllowableValue("4", "Authentication Failure",
            "An authenticationFailure trap signifies that the sending protocol entity is the addressee of a protocol message that is not properly authenticated." +
                    " While implementations of the SNMP must be capable of generating this trap, they must also be capable of suppressing the emission of such traps via" +
                    " an implementation- specific mechanism.");

    static final AllowableValue EGP_NEIGHBORLOSS = new AllowableValue("5", "EGP Neighborloss",
            "An egpNeighborLoss trap signifies that an EGP neighbor for whom the sending protocol entity was an EGP peer has been marked down and the peer relationship" +
                    " no longer obtains");

    static final AllowableValue ENTERPRISE_SPECIFIC = new AllowableValue("6", "Enterprise Specific",
            "An enterpriseSpecific trap signifies that a particular enterprise-specific trap has occurred which " +
                    "can be defined in the Specific Trap Type field.");

    public static final String GENERIC_TRAP_TYPE_FF_ATTRIBUTE = "generic-trap-type";

    public static final AllowableValue WITH_FLOW_FILE_ATTRIBUTE = new AllowableValue(GENERIC_TRAP_TYPE_FF_ATTRIBUTE, "With \"generic-trap-type\" FlowFile Attribute",
            "Provide the Generic Trap Type with the \"generic-trap-type\" flowfile attribute.");

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
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(SNMP_VERSION, SNMP_V1)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor GENERIC_TRAP_TYPE = new PropertyDescriptor.Builder()
            .name("snmp-trap-generic-type")
            .displayName("Generic Trap Type")
            .description("Generic trap type is an integer in the range of 0 to 6. See processor usage for details.")
            .required(true)
            .dependsOn(SNMP_VERSION, SNMP_V1)
            .allowableValues(COLD_START, WARM_START, LINK_DOWN, LINK_UP, AUTHENTICATION_FAILURE, EGP_NEIGHBORLOSS,
                    ENTERPRISE_SPECIFIC, WITH_FLOW_FILE_ATTRIBUTE)
            .build();

    public static final PropertyDescriptor SPECIFIC_TRAP_TYPE = new PropertyDescriptor.Builder()
            .name("snmp-trap-specific-type")
            .displayName("Specific Trap Type")
            .description("Specific trap type is a number that further specifies the nature of the event that generated " +
                    "the trap in the case of traps of generic type 6 (enterpriseSpecific). The interpretation of this " +
                    "code is vendor-specific.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .dependsOn(SNMP_VERSION, SNMP_V1)
            .dependsOn(GENERIC_TRAP_TYPE, ENTERPRISE_SPECIFIC)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
}
