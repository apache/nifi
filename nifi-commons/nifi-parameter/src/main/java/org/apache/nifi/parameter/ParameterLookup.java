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

import java.util.Optional;

public interface ParameterLookup {

    /**
     * Returns the Parameter with the given name
     * @param parameterName the name of the Parameter
     * @return the Parameter with the given name or an empty Optional if no Parameter exists with that name
     */
    Optional<Parameter> getParameter(String parameterName);

    /**
     * Returns false if any Parameters are available, true if no Parameters have been defined
     * @return true if empty
     */
    boolean isEmpty();

    /**
     * Indicates the current Version of the Parameter Context. Each time that the Parameter Context is updated, its version is incremented. This allows
     * other components to know whether or not the values have changed since some other point in time. The version may or may not be persisted across
     * restarts of the application.
     *
     * @return the current version
     */
    long getVersion();


    ParameterLookup EMPTY = new ParameterLookup() {
        @Override
        public Optional<Parameter> getParameter(final String parameterName) {
            return Optional.empty();
        }

        @Override
        public long getVersion() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }
    };
}
