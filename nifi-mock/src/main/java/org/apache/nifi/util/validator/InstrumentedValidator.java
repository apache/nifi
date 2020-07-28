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
package org.apache.nifi.util.validator;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

/**
 * InstrumentedValidator wraps a {@class Validator} and provides statistics on it's interactions.
 * Because many of the {@class Validator} instances returned from {@class StandardValidator }
 * are not mockable with with mockito, this is required to know, when running a test, if a
 * {@class Validator} was in fact called, for example.
 */
public class InstrumentedValidator implements Validator {

    /**
     * Flag to reset a count after retrieving it.
     * Thus not having to explicitly call reset() for simple cases.
     */
    boolean doReset = false;

    /**
     * Count the number of calls to validate()
     */
    private int validateCallCount;
    private Validator mockedValidator;

    /**
     * Constructs a new {@class InstrumentedValidator}.
     *
     * @param mockedValidator the {@class Validator} to wrap.
     */
    public InstrumentedValidator(Validator mockedValidator) {
        this(mockedValidator,false);
    }

    /**
     * Constructs a new {@class InstrumentedValidator}.
     *
     * @param mockedValidator the {@class Validator} to wrap.
     */
    public InstrumentedValidator(Validator mockedValidator, boolean resetOnGet) {
        this.mockedValidator = mockedValidator;
        this.doReset = resetOnGet;
    }

    /**
     * Default constructor without wrapping not supported.
     *
     */
    private InstrumentedValidator(){}

    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
        validateCallCount++;
        return mockedValidator.validate(subject, input, context);
    }

    /**
     * Returns the count of times validate was called
     * @return count of validate() calls
     */
    public int getValidateCallCount() {
        int count = validateCallCount;
        if (doReset) {
            validateCallCount = 0;
        }
        return count;
    }

    /**
     * Resets the count of all calls to 0.
     */
    public void resetAll() {
        validateCallCount = 0;
    }
}
