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
package org.apache.nifi.web.api.dto.util;

/**
 * Utility class for numbers.
 */
public class NumberUtil {

    /**
     * Calculate sum of Integers those can be null.
     * This method can be used to avoid getting NullPointerException when a null Integer being auto-boxed into an int.
     * @param values Integers to add
     * @return the sum of given values or null if all values are null
     */
    public static Integer sumNullableIntegers(Integer ... values) {
        int sum = 0;
        int count = 0;
        for (Integer value : values) {
            if (value == null) {
                continue;
            }
            sum += value;
            count++;
        }
        return count == 0 ? null : sum;
    }

}
