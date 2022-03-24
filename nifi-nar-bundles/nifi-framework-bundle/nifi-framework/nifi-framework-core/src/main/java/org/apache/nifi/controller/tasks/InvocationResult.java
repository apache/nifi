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

package org.apache.nifi.controller.tasks;

public interface InvocationResult {
    boolean isYield();

    String getYieldExplanation();


    public static InvocationResult DO_NOT_YIELD = new InvocationResult() {
        @Override
        public boolean isYield() {
            return false;
        }

        @Override
        public String getYieldExplanation() {
            return null;
        }
    };

    public static InvocationResult yield(final String explanation) {
        return new InvocationResult() {
            @Override
            public boolean isYield() {
                return true;
            }

            @Override
            public String getYieldExplanation() {
                return explanation;
            }
        };
    }
}
