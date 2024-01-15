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
package org.apache.nifi.web.api.dto;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlType;

/**
 * A component referencing a parameter provider.  The only allowed component at this time is a ParameterContext.
 */
@XmlType(name = "parameterProviderReferencingComponent")
public class ParameterProviderReferencingComponentDTO {

    private String id;
    private String name;

    /**
     * @return id for this component referencing a parameter provider
     */
    @Schema(description = "The id of the component referencing a parameter provider."
    )
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return name for this component referencing a parameter provider
     */
    @Schema(description = "The name of the component referencing a parameter provider."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
