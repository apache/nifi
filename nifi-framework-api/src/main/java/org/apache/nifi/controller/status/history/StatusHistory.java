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

/**
 * Represents a collection of historical status values for a component
 */
public interface StatusHistory {

    /**
     * @return a Date indicating when this report was generated
     */
    Date getDateGenerated();

    /**
     * @return a Map of component field names and their values. The order in
     * which these values are displayed is dependent on the natural ordering of
     * the Map returned
     */
    Map<String, String> getComponentDetails();

    /**
     * @return List of snapshots for a given component
     */
    List<StatusSnapshot> getStatusSnapshots();

}
