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
package org.apache.nifi.lookup.script;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.StringLookupService;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A Controller service that allows the user to script the lookup operation to be performed (by LookupAttribute, e.g.)
 */
@Tags({"lookup", "script", "invoke", "groovy", "python", "jython", "jruby", "ruby", "javascript", "js", "lua", "luaj"})
@CapabilityDescription("Allows the user to provide a scripted LookupService instance in order to enrich records from "
        + "an incoming flow file. The script is expected to return an optional string value rather than an arbitrary object (record, e.g.). "
        + "Also the scripted lookup service should implement StringLookupService, otherwise the getValueType() method must be implemented even "
        + "though it will be ignored, as SimpleScriptedLookupService returns String as the value type on the script's behalf. Please note that due to "
        + "a bug in Jython that remains unresolved, it is not possible to use Jython to write a script for this service in Python.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXECUTE_CODE,
                        explanation = "Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
        }
)
public class SimpleScriptedLookupService extends BaseScriptedLookupService implements StringLookupService {

    @Override
    @SuppressWarnings("unchecked")
    public Set<String> getRequiredKeys() {
        return lookupService.get().getRequiredKeys();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<String> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        // Delegate the lookup() call to the scripted LookupService
        return lookupService.get().lookup(coordinates);
    }
}
