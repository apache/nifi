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

package org.apache.nifi.minifi.commons.schema.common;

import java.util.function.Consumer;

public class StringUtil {
    /**
     * Returns true if the string is null or empty
     *
     * @param string the string
     * @return true if the string is null or empty
     */
    public static boolean isNullOrEmpty(String string) {
        return string == null || string.isEmpty();
    }

    /**
     * Passes the string to the consumer if it is neither null nor empty
     *
     * @param string   the input
     * @param consumer the action to perform
     */
    public static void doIfNotNullOrEmpty(String string, Consumer<String> consumer) {
        if (!isNullOrEmpty(string)) {
            consumer.accept(string);
        }
    }

    /**
     * Passes the string to the consumer if it is either null nor empty
     *
     * @param string   the input
     * @param consumer the action to perform
     */
    public static void doIfNullOrEmpty(String string, Consumer<String> consumer) {
        if (isNullOrEmpty(string)) {
            consumer.accept(string);
        }
    }
}
