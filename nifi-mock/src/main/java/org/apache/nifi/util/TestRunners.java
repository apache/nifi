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

public class TestRunners {

    /**
     * Returns a {@code TestRunner} for the given {@code Processor}.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will have the default name of {@code processor.getClass().getName()}
     * @param processor the {@code Processor} under test
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Processor processor) {
        return newTestRunner(processor,processor.getClass().getName());
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor}.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will be the passed name.
     * @param processor the {@code Processor} under test
     * @param name the name to give the {@code Processor}
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Processor processor, String name) {
        return new StandardProcessorTestRunner(processor, name);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor} class.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will have the default name of {@code processor.getClass().getName()}
     * @param processorClass the {@code Processor} class
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Class<? extends Processor> processorClass) {
        return newTestRunner(processorClass, processorClass.getName());
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor} class.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will have the default name of {@code processor.getClass().getName()}
     * @param processorClass the {@code Processor} class
     * @param name the name to give the {@code Processor}
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Class<? extends Processor> processorClass, String name) {
        try {
            return newTestRunner(processorClass.newInstance(), name);
        } catch (final Exception e) {
            System.err.println("Could not instantiate instance of class " + processorClass.getName() + " due to: " + e);
            throw new RuntimeException(e);
        }
    }

}
