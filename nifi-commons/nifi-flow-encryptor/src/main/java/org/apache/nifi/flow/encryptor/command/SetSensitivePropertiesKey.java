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
package org.apache.nifi.flow.encryptor.command;

/**
 * Set Sensitive Properties Key for NiFi Properties and update encrypted Flow Configuration
 */
public class SetSensitivePropertiesKey {
    private static final int MINIMUM_REQUIRED_LENGTH = 12;

    public static void main(final String[] arguments) {
        if (arguments.length == 1) {
            final String outputPropertiesKey = arguments[0];
            if (outputPropertiesKey.length() < MINIMUM_REQUIRED_LENGTH) {
                System.err.printf("Sensitive Properties Key length less than required [%d]%n", MINIMUM_REQUIRED_LENGTH);
            } else {
                final FlowEncryptorCommand command = new FlowEncryptorCommand();
                command.setRequestedPropertiesKey(outputPropertiesKey);
                command.run();
            }
        } else {
            System.err.printf("Unexpected number of arguments [%d]%n", arguments.length);
            System.err.printf("Usage: %s <sensitivePropertiesKey>%n", SetSensitivePropertiesKey.class.getSimpleName());
        }
    }
}
