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
package org.apache.nifi.controller.status.history;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class StandardStatusHistory implements StatusHistory {
    private final List<StatusSnapshot> snapshots;
    private final Date generated;
    private final Map<String, String> componentDetails;

    public StandardStatusHistory(final List<StatusSnapshot> snapshots, final Map<String, String> componentDetails, final Date generated) {
        this.snapshots = snapshots;
        this.generated = generated;
        this.componentDetails = componentDetails;
    }

    @Override
    public List<StatusSnapshot> getStatusSnapshots() {
        return snapshots;
    }

    @Override
    public Date getDateGenerated() {
        return generated;
    }

    @Override
    public Map<String, String> getComponentDetails() {
        return componentDetails;
    }
}
