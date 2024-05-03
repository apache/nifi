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

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Represents only an ID reference for a ParameterContext.
 */
public class ReferenceOnlyParameterContext extends StandardParameterContext {

    public ReferenceOnlyParameterContext(final String id) {
        super(id, String.format("Reference-Only Parameter Context [%s]", id), ParameterReferenceManager.EMPTY, null, null, null);
    }

    /**
     * A ParameterContext's identity is its identifier.
     * @param obj Another object
     * @return Whether this is equal to the object
     */
    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof ReferenceOnlyParameterContext) {
            final ReferenceOnlyParameterContext other = (ReferenceOnlyParameterContext) obj;
            return (getIdentifier().equals(other.getIdentifier()));
        } else {
            return false;
        }
    }

    /**
     * A ParameterContext's identity is its identifier.
     * @return The hash code
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getClass().getName()).append(getIdentifier()).toHashCode();
    }
}
