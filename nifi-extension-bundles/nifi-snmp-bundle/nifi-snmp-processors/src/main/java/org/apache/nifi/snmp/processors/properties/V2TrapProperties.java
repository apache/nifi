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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import static org.apache.nifi.snmp.processors.properties.BasicProperties.SNMP_V2C;
import static org.apache.nifi.snmp.processors.properties.BasicProperties.SNMP_V3;
import static org.apache.nifi.snmp.processors.properties.BasicProperties.SNMP_VERSION;

public class V2TrapProperties {

    private V2TrapProperties() {
        // Utility class, not needed to instantiate.
    }

    public static final PropertyDescriptor TRAP_OID_VALUE = new PropertyDescriptor.Builder()
            .name("snmp-trap-oid-value")
            .displayName("Trap OID Value")
            .description("The value of the trap OID.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(SNMP_VERSION, SNMP_V2C, SNMP_V3)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
}
