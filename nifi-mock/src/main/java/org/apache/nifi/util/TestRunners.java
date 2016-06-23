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
package org.apache.nifi.util;

import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.VariableRegistryUtils;

public class TestRunners {

    public static TestRunner newTestRunner(final Processor processor) {
        return newTestRunner(processor,VariableRegistryUtils.createSystemVariableRegistry());
    }

    public static TestRunner newTestRunner(final Processor processor, VariableRegistry variableRegistry){
        return new StandardProcessorTestRunner(processor, variableRegistry);
    }

    public static TestRunner newTestRunner(final Class<? extends Processor> processorClass) {
        try {
            return newTestRunner(processorClass.newInstance());

        } catch (final Exception e) {
            System.err.println("Could not instantiate instance of class " + processorClass.getName() + " due to: " + e);
            throw new RuntimeException(e);
        }
    }

}
