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
package org.apache.nifi.deprecation.log;

/**
 * Deprecation Exception provides stack traces referencing deprecated features or capabilities
 */
class DeprecationException extends RuntimeException {
    /**
     * Deprecation Exception package-private constructor for internal usage within Standard Deprecation Logger
     *
     * @param referenceClass Reference Class
     */
    DeprecationException(final Class<?> referenceClass) {
        super(getMessage(referenceClass));
    }

    private static String getMessage(final Class<?> referenceClass) {
        final ClassLoader classLoader = referenceClass.getClassLoader();
        return String.format("Reference Class [%s] ClassLoader [%s]", referenceClass.getName(), classLoader);
    }
}
