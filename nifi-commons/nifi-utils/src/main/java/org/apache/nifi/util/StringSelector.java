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

/**
 * Fluent api for checking one or more strings and selecting the first non-empty one.
 * <br/><br/>
 * {@link #toString()} returns the first encountered non-empty string or "".
 * <p>
 * Optimized so that no intermediary objects are created, only one, once the first non-empty string is found.
 */
public interface StringSelector {
    /**
     * Starts the fluent expression by checking the first string(s).
     *
     * @param strings The first string(s) to check if empty.
     * @return a {@link StringSelector} that checked the first string.
     */
    static StringSelector of(String... strings) {
        return EMPTY_STRING_SELECTOR.or(strings);
    }

    /**
     * Check the next string(s).
     *
     * @param strings The next string(s) to check if empty.
     * @return a {@link StringSelector} that checked all strings so far.
     */
    StringSelector or(String... strings);

    /**
     * May be used to stop processing subsequent inputs when a result is already available.
     *
     * @return true if a non-empty string has been found, false otherwise.
     */
    boolean found();

    StringSelector EMPTY_STRING_SELECTOR = new StringSelector() {
        @Override
        public String toString() {
            return "";
        }

        @Override
        public StringSelector or(String... strings) {
            for (String string : strings) {
                if (string != null && string.length() > 0) {
                    return new StringSelector() {
                        @Override
                        public StringSelector or(String... string) {
                            return this;
                        }

                        @Override
                        public String toString() {
                            return string;
                        }

                        @Override
                        public boolean found() {
                            return true;
                        }
                    };
                }
            }

            return EMPTY_STRING_SELECTOR;
        }

        @Override
        public boolean found() {
            return false;
        }
    };
}
