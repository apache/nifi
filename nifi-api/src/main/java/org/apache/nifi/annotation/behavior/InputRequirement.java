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
package org.apache.nifi.annotation.behavior;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>
 * Marker interface that a Processor can use to indicate whether it accepts, requires, or forbids
 * input from other Processors. This information is used by the framework in order to ensure that
 * a Processor is marked as invalid if it is missing necessary input or has input that will be ignored.
 * This information also is used by the NiFi UI in order to prevent users from making connections
 * to Processors that don't make sense.
 * </p>
 */
@Documented
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface InputRequirement {
    Requirement value();

    enum Requirement {
        /**
         * This value is used to indicate that the Processor requires input from other Processors
         * in order to run. As a result, the Processor will not be valid if it does not have any
         * incoming connections.
         */
        INPUT_REQUIRED,

        /**
         * This value is used to indicate that the Processor will consume data from an incoming
         * connection but does not require an incoming connection in order to perform its task.
         * If the {@link InputRequirement} annotation is not present, this is the default value
         * that is used.
         */
        INPUT_ALLOWED,

        /**
         * This value is used to indicate that the Processor is a "Source Processor" and does
         * not accept incoming connections. Because the Processor does not pull FlowFiles from
         * an incoming connection, it can be very confusing for users who create incoming connections
         * to the Processor. As a result, this value can be used in order to clarify that incoming
         * connections will not be used. This prevents the user from even creating such a connection.
         */
        INPUT_FORBIDDEN;
    }
}
