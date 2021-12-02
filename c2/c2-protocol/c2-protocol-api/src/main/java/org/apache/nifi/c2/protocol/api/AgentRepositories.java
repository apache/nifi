/*
 * Apache NiFi - MiNiFi
 * Copyright 2014-2018 The Apache Software Foundation
 *
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
import java.io.Serializable;

@ApiModel
public class AgentRepositories implements Serializable {
    private static final long serialVersionUID = 6350263064169212602L;

    private AgentRepositoryStatus flowfile;
    private AgentRepositoryStatus provenance;

    @ApiModelProperty
    public AgentRepositoryStatus getFlowfile() {
        return flowfile;
    }

    public void setFlowfile(AgentRepositoryStatus flowfile) {
        this.flowfile = flowfile;
    }

    @ApiModelProperty
    public AgentRepositoryStatus getProvenance() {
        return provenance;
    }

    public void setProvenance(AgentRepositoryStatus provenance) {
        this.provenance = provenance;
    }
}
