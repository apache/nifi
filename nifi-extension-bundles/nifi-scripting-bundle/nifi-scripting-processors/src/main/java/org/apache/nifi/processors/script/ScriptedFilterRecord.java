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
package org.apache.nifi.processors.script;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.Relationship;

import java.util.Optional;
import java.util.Set;

@Tags({"record", "filter", "script", "groovy"})
@CapabilityDescription(
    "This processor provides the ability to filter records out from FlowFiles using the user-provided script. " +
    "Every record will be evaluated by the script which must return with a boolean value. " +
    "Records with \"true\" result will be routed to the \"matching\" relationship in a batch. " +
    "Other records will be filtered out."
)
@SeeAlso(classNames = {
    "org.apache.nifi.processors.script.ScriptedTransformRecord",
    "org.apache.nifi.processors.script.ScriptedValidateRecord",
    "org.apache.nifi.processors.script.ScriptedPartitionRecord"
})
public class ScriptedFilterRecord extends ScriptedRouterProcessor<Boolean> {
    static final Relationship RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("success")
            .description(
                "Matching records of the original FlowFile will be routed to this relationship. " +
                "If there are no matching records, no FlowFile will be routed here."
            )
            .build();

    static final Relationship RELATIONSHIP_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description(
                "After successful procession, the incoming FlowFile will be transferred to this relationship. " +
                "This happens regardless the number of filtered or remaining records.")
            .build();

    static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("In case of any issue during processing the incoming FlowFile, the incoming FlowFile will be routed to this relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            RELATIONSHIP_ORIGINAL,
            RELATIONSHIP_FAILURE,
            RELATIONSHIP_SUCCESS
    );

    public ScriptedFilterRecord() {
        super(Boolean.class);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected Relationship getOriginalRelationship() {
        return RELATIONSHIP_ORIGINAL;
    }

    @Override
    protected Relationship getFailureRelationship() {
        return RELATIONSHIP_FAILURE;
    }

    @Override
    protected Optional<Relationship> resolveRelationship(final Boolean scriptResult) {
        return scriptResult ? Optional.of(RELATIONSHIP_SUCCESS) : Optional.empty();
    }
}
