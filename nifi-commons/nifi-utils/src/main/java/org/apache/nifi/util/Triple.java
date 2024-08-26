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

public record Triple<A, B, C>(A first, B second, C third) {

    @Override
    public boolean equals(final Object other) {
        if (other == null) {
            return false;
        }
        if (other == this) {
            return true;
        }
        if (!(other instanceof Triple<?, ?, ?> triple)) {
            return false;
        }

        if (first == null) {
            if (triple.first != null) {
                return false;
            }
        } else {
            if (!first.equals(triple.first)) {
                return false;
            }
        }

        if (second == null) {
            if (triple.second != null) {
                return false;
            }
        } else {
            if (!second.equals(triple.second)) {
                return false;
            }
        }

        if (third == null) {
            return triple.third == null;
        } else {
            return third.equals(triple.third);
        }
    }

}
