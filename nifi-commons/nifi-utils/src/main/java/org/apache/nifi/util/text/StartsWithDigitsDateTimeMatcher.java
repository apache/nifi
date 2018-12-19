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
package org.apache.nifi.util.text;

/**
 * A DateTimeMatcher that bases its decision purely on whether or not the first X number of characters in the input text are digits.
 */
public class StartsWithDigitsDateTimeMatcher implements DateTimeMatcher {
    private final int expectedNumberOfDigits;

    public StartsWithDigitsDateTimeMatcher(final int expectedNumberOfDigits) {
        this.expectedNumberOfDigits = expectedNumberOfDigits;
    }

    @Override
    public boolean matches(final String text) {
        if (text.length() < expectedNumberOfDigits) {
            return false;
        }

        for (int i=0; i < expectedNumberOfDigits; i++) {
            if (!Character.isDigit(text.charAt(i))) {
                return false;
            }
        }

        return true;
    }
}
