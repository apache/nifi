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
import io.swagger.annotations.ApiModelProperty;
import java.util.Objects;
import java.util.Set;
import org.apache.nifi.c2.protocol.component.api.RuntimeManifest;

@ApiModel
public class AgentManifest extends RuntimeManifest {
    private static final long serialVersionUID = 1L;

    @ApiModelProperty("All supported operations by agent")
    private Set<SupportedOperation> supportedOperations;

    public AgentManifest() {
        super();
    }

    public AgentManifest(RuntimeManifest manifest) {
        super();
        setAgentType(manifest.getAgentType());
        setIdentifier(manifest.getIdentifier());
        setBundles(manifest.getBundles());
        setBuildInfo(manifest.getBuildInfo());
        setSchedulingDefaults(manifest.getSchedulingDefaults());
        setVersion(manifest.getVersion());
    }

    public Set<SupportedOperation> getSupportedOperations() {
        return supportedOperations;
    }

    public void setSupportedOperations(Set<SupportedOperation> supportedOperations) {
        this.supportedOperations = supportedOperations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        AgentManifest that = (AgentManifest) o;
        return Objects.equals(supportedOperations, that.supportedOperations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), supportedOperations);
    }


    @Override
    public String toString() {
        return "AgentManifest{" +
            "supportedOperations=" + supportedOperations +
            "}, " + super.toString();
    }
}
