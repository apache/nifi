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
package org.apache.nifi.processors.standard;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * Base class for Syslog processors.
 */
public abstract class AbstractSyslogProcessor extends AbstractProcessor {

    public static final AllowableValue TCP_VALUE = new AllowableValue("TCP", "TCP");
    public static final AllowableValue UDP_VALUE = new AllowableValue("UDP", "UDP");

    public static final PropertyDescriptor PROTOCOL = new PropertyDescriptor
            .Builder().name("Protocol")
            .description("The protocol for Syslog communication.")
            .required(true)
            .allowableValues(TCP_VALUE, UDP_VALUE)
            .defaultValue(UDP_VALUE.getValue())
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("Port")
            .description("The port for Syslog communication. Note that Expression language is not evaluated per FlowFile.")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set of the Syslog messages. Note that Expression language is not evaluated per FlowFile.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Timeout")
            .description("The timeout for connecting to and communicating with the syslog server. Does not apply to UDP. Note that Expression language is not evaluated per FlowFile.")
            .required(false)
            .defaultValue("10 seconds")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();



}
