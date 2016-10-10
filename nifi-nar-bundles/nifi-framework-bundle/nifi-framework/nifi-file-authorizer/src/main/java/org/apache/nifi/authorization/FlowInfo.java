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
package org.apache.nifi.authorization;

import org.apache.nifi.web.api.dto.PortDTO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FlowInfo {

    private final String rootGroupId;

    private final List<PortDTO> ports;

    public FlowInfo(final String rootGroupId, final List<PortDTO> ports) {
        this.rootGroupId = rootGroupId;
        this.ports = (ports == null ? Collections.unmodifiableList(Collections.EMPTY_LIST) :
                Collections.unmodifiableList(new ArrayList<>(ports)) );
    }

    public String getRootGroupId() {
        return rootGroupId;
    }

    public List<PortDTO> getPorts() {
        return ports;
    }

}
