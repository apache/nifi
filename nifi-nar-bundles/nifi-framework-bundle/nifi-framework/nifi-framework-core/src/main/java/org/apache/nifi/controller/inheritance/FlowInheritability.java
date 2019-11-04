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
package org.apache.nifi.controller.inheritance;

public interface FlowInheritability {
    /**
     * @return whether or not the flow can be inherited
     */
    boolean isInheritable();

    /**
     * If the flow is not inheritable, this provides an explanation as to why the flow cannot be inherited
     * @return an explanation as to why the flow cannot be inherited, or <code>null</code> if the flow is inheritable
     */
    String getExplanation();

    static FlowInheritability inheritable() {
        return new FlowInheritability() {
            @Override
            public boolean isInheritable() {
                return true;
            }

            @Override
            public String getExplanation() {
                return null;
            }

            @Override
            public String toString() {
                return "FlowInheritability[inheritable=true]";
            }
        };
    }

    static FlowInheritability notInheritable(String explanation) {
        return new FlowInheritability() {
            @Override
            public boolean isInheritable() {
                return false;
            }

            @Override
            public String getExplanation() {
                return explanation;
            }

            @Override
            public String toString() {
                return "FlowInheritability[inheritable=false, explanation=" + explanation + "]";
            }
        };
    }
}
