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

package org.apache.nifi.cluster.integration;

import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class ClusterUtils {

    public static void waitUntilConditionMet(final long time, final TimeUnit timeUnit, final BooleanSupplier test) {
        waitUntilConditionMet(time, timeUnit, test, null);
    }

    public static void waitUntilConditionMet(final long time, final TimeUnit timeUnit, final BooleanSupplier test, final Supplier<String> errorMessageSupplier) {
        final long nanosToWait = timeUnit.toNanos(time);
        final long start = System.nanoTime();
        final long maxTime = start + nanosToWait;

        while (!test.getAsBoolean()) {
            if (System.nanoTime() > maxTime) {
                if (errorMessageSupplier == null) {
                    throw new AssertionError("Condition never occurred after waiting " + time + " " + timeUnit);
                } else {
                    throw new AssertionError("Condition never occurred after waiting " + time + " " + timeUnit + " : " + errorMessageSupplier.get());
                }
            }
        }
    }

    public static <T> T waitUntilNonNull(final long time, final TimeUnit timeUnit, final Supplier<T> test) {
        final long nanosToWait = timeUnit.toNanos(time);
        final long start = System.nanoTime();
        final long maxTime = start + nanosToWait;

        T returnVal;
        while ((returnVal = test.get()) == null) {
            if (System.nanoTime() > maxTime) {
                throw new AssertionError("Condition never occurred after waiting " + time + " " + timeUnit);
            }
        }

        return returnVal;
    }
}
