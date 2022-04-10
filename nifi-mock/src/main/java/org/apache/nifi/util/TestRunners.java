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

import org.apache.nifi.kerberos.KerberosContext;
import org.apache.nifi.processor.Processor;

public class TestRunners {

    /**
     * Returns a {@code TestRunner} for the given {@code Processor}.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will have the default name of {@code processor.getClass().getName()}
     * @param processor the {@code Processor} under test
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Processor processor) {
        return newTestRunner(processor,processor.getClass().getName(), false);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor}.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will have the default name of {@code processor.getClass().getName()}
     * @param processor the {@code Processor} under test
     * @param requireBestPractices enable best practices enforcement in the test suite
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Processor processor, boolean requireBestPractices) {
        return newTestRunner(processor,processor.getClass().getName(), requireBestPractices);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor} which uses the given {@code KerberosContext}.
     * @param processor the {@code Processor} under test
     * @param kerberosContext the {@code KerberosContext} used during the test
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Processor processor, KerberosContext kerberosContext) {
        return newTestRunner(processor,processor.getClass().getName(), kerberosContext);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor} which uses the given {@code KerberosContext}.
     * @param processor the {@code Processor} under test
     * @param kerberosContext the {@code KerberosContext} used during the test
     * @param requireBestPractices enable best practices enforcement during construction of the test runner
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Processor processor, KerberosContext kerberosContext, boolean requireBestPractices) {
        return newTestRunner(processor,processor.getClass().getName(), kerberosContext, requireBestPractices);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor}.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will have the default name of {@code processor.getClass().getName()}
     * @param processor the {@code Processor} under test
     * @param logger the {@code ComponentLog} used for logging
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Processor processor, MockComponentLog logger) {
        return newTestRunner(processor,processor.getClass().getName(), logger);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor}.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will be the passed name.
     * @param processor the {@code Processor} under test
     * @param name the name to give the {@code Processor}
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Processor processor, String name) {
        return newTestRunner(processor, name, false);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor}.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will be the passed name.
     * @param processor the {@code Processor} under test
     * @param name the name to give the {@code Processor}
     * @param requireBestPractices enable best practices enforcement during test runner construction
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Processor processor, String name, boolean requireBestPractices) {
        return new StandardProcessorTestRunner(processor, name, null, null, requireBestPractices);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor} and {@code KerberosContext}.
     * @param processor the {@code Processor} under test
     * @param name the name to give the {@code Processor}
     * @param kerberosContext the {@code KerberosContext} used during the test
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Processor processor, String name, KerberosContext kerberosContext) {
        return newTestRunner(processor, name, kerberosContext, false);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor} and {@code KerberosContext}.
     * @param processor the {@code Processor} under test
     * @param name the name to give the {@code Processor}
     * @param kerberosContext the {@code KerberosContext} used during the test
     * @param requireBestPractices enable best practices enforcement during the test runner construction
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Processor processor, String name, KerberosContext kerberosContext, boolean requireBestPractices) {
        return new StandardProcessorTestRunner(processor, name, kerberosContext);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor}.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will be the passed name.
     * @param processor the {@code Processor} under test
     * @param name the name to give the {@code Processor}
     * @param logger the {@code ComponentLog} used for logging
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Processor processor, String name, MockComponentLog logger) {
        return new StandardProcessorTestRunner(processor, name, logger);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor} class.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will have the default name of {@code processor.getClass().getName()}
     * @param processorClass the {@code Processor} class
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Class<? extends Processor> processorClass) {
        return newTestRunner(processorClass, processorClass.getName(), false);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor} class.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will have the default name of {@code processor.getClass().getName()}
     * @param processorClass the {@code Processor} class
     * @param requireBestPractices enable best practices enforcement during test runner construction
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Class<? extends Processor> processorClass, boolean requireBestPractices) {
        return newTestRunner(processorClass, processorClass.getName(), requireBestPractices);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor} class.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will have the default name of {@code processor.getClass().getName()}
     * @param processorClass the {@code Processor} class
     * @param logger the {@code ComponentLog} used for logging
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Class<? extends Processor> processorClass, MockComponentLog logger) {
        return newTestRunner(processorClass, processorClass.getName(), logger);
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor} class.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will have the default name of {@code processor.getClass().getName()}
     * @param processorClass the {@code Processor} class
     * @param name the name to give the {@code Processor}
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Class<? extends Processor> processorClass, String name, boolean requireBestPractices) {
        try {
            return newTestRunner(processorClass.newInstance(), name, requireBestPractices);
        } catch (final Exception e) {
            System.err.println("Could not instantiate instance of class " + processorClass.getName() + " due to: " + e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns a {@code TestRunner} for the given {@code Processor} class.
     * The processor name available from {@code TestRunner.getProcessContext().getName()} will have the default name of {@code processor.getClass().getName()}
     * @param processorClass the {@code Processor} class
     * @param name the name to give the {@code Processor}
     * @param logger the {@code ComponentLog} used for logging
     * @return a {@code TestRunner}
     */
    public static TestRunner newTestRunner(final Class<? extends Processor> processorClass, String name, MockComponentLog logger) {
        try {
            return newTestRunner(processorClass.newInstance(), name, logger);
        } catch (final Exception e) {
            System.err.println("Could not instantiate instance of class " + processorClass.getName() + " due to: " + e);
            throw new RuntimeException(e);
        }
    }
}
