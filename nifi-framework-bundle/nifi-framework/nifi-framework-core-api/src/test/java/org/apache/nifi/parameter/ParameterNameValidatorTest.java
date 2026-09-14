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
package org.apache.nifi.parameter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParameterNameValidatorTest {
    @Test
    void testValidParameterName() {
        assertTrue(ParameterNameValidator.isValid("Parameter Name-1.0"));
        assertDoesNotThrow(() -> ParameterNameValidator.validate("Parameter Name-1.0"));
    }

    @Test
    void testInvalidParameterName() {
        final String parameterName = "PARAMETER_{{ ENVIRONMENT }}";

        assertFalse(ParameterNameValidator.isValid(parameterName));
        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ParameterNameValidator.validate(parameterName));

        assertTrue(exception.getMessage().contains(parameterName));
    }
}
