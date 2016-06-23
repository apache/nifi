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

import com.wordnik.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

/**
 * Current revision for this NiFi.
 */
@XmlType(name = "revision")
public class RevisionDTO {

    private String clientId;
    private Long version;
    private String lastModifier;

    /* getters / setters */
    /**
     * A client identifier used to make a request. By including a client identifier, the API can allow multiple requests without needing the current revision. Due to the asynchronous nature of
     * requests/responses this was implemented to allow the client to make numerous requests without having to wait for the previous response to come back.
     *
     * @return The client id
     */
    @ApiModelProperty(
            value = "A client identifier used to make a request. By including a client identifier, the API can allow multiple requests without needing the current revision. Due to the asynchronous "
            + "nature of requests/responses this was implemented to allow the client to make numerous requests without having to wait for the previous response to come back"
    )
    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * NiFi employs an optimistic locking strategy where the client must include a revision in their request when performing an update. In a response, this field represents the updated base version.
     *
     * @return The revision
     */
    @ApiModelProperty(
            value = "NiFi employs an optimistic locking strategy where the client must include a revision in their request when performing an update. In a response to a mutable flow request, this "
                    + "field represents the updated base version."
    )
    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    /**
     * @return The user that last modified the flow
     */
    @ApiModelProperty(
            value = "The user that last modified the flow.",
            readOnly = true
    )
    public String getLastModifier() {
        return lastModifier;
    }

    public void setLastModifier(String lastModifier) {
        this.lastModifier = lastModifier;
    }

}
