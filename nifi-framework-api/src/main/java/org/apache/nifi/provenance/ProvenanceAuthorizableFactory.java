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

package org.apache.nifi.provenance;

import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.web.ResourceNotFoundException;

public interface ProvenanceAuthorizableFactory {

    /**
     * Generates an Authorizable object for the Data of the component with the given ID. This includes
     * provenance data and queue's on outgoing relationships.
     *
     * @param componentId the ID of the component to which the Data belongs
     *
     * @return the Authorizable that can be use to authorize access to provenance events
     * @throws ResourceNotFoundException if no component can be found with the given ID
     */
    Authorizable createDataAuthorizable(String componentId);

}
