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

package org.apache.nifi.c2.protocol.api;

import io.swagger.annotations.ApiModel;

import java.io.Serializable;
import java.util.Objects;

@ApiModel(
        value = "FlowUri",
        description = "Uniform Resource Identifier for flows, used to uniquely identify a flow version ")
public class FlowUri implements Serializable {
    private static final long serialVersionUID = 1L;

    private String uriString;

    public FlowUri() {
    }

    public FlowUri(String flowUriString) {
        this.uriString = flowUriString;
    }

    @Override
    public String toString() {
        return uriString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final FlowUri flowUri = (FlowUri) o;
        return Objects.equals(uriString, flowUri.uriString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uriString);
    }
}
