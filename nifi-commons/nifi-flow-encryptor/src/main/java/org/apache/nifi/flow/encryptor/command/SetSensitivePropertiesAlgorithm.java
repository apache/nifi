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
 * Set Sensitive Properties Algorithm for NiFi Properties and update encrypted Flow Configuration
 */
public class SetSensitivePropertiesAlgorithm {

    public static void main(final String[] arguments) {
        if (arguments.length == 1) {
            final String algorithm = arguments[0];
            final FlowEncryptorCommand command = new FlowEncryptorCommand();
            command.setRequestedPropertiesAlgorithm(algorithm);
            command.run();
        } else {
            System.err.printf("Unexpected number of arguments [%d]%n", arguments.length);
            System.err.printf("Usage: %s <algorithm>%n", SetSensitivePropertiesAlgorithm.class.getSimpleName());
        }
    }
}
