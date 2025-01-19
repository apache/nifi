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

@Tags({"record", "validate", "script", "groovy"})
@CapabilityDescription(
        "This processor provides the ability to validate records in FlowFiles using the user-provided script. " +
        "The script is expected to have a record as incoming argument and return with a boolean value. " +
        "Based on this result, the processor categorizes the records as \"valid\" or \"invalid\" and routes them to the respective relationship in batch. " +
        "Additionally the original FlowFile will be routed to the \"original\" relationship or in case of unsuccessful processing, to the \"failed\" relationship."
)
@SeeAlso(classNames = {
        "org.apache.nifi.processors.script.ScriptedTransformRecord",
        "org.apache.nifi.processors.script.ScriptedFilterRecord",
        "org.apache.nifi.processors.script.ScriptedPartitionRecord"
})
public class ScriptedValidateRecord extends ScriptedRouterProcessor<Boolean> {

    static final Relationship RELATIONSHIP_VALID = new Relationship.Builder()
            .name("valid")
            .description(
                "FlowFile containing the valid records from the incoming FlowFile will be routed to this relationship. " +
                "If there are no valid records, no FlowFile will be routed to this Relationship.")
            .build();

    static final Relationship RELATIONSHIP_INVALID = new Relationship.Builder()
            .name("invalid")
            .description(
                "FlowFile containing the invalid records from the incoming FlowFile will be routed to this relationship. " +
                "If there are no invalid records, no FlowFile will be routed to this Relationship.")
            .build();

    static final Relationship RELATIONSHIP_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description(
                "After successful procession, the incoming FlowFile will be transferred to this relationship. " +
                "This happens regardless the FlowFiles might routed to \"valid\" and \"invalid\" relationships.")
            .build();

    static final Relationship RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("In case of any issue during processing the incoming flow file, the incoming FlowFile will be routed to this relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            RELATIONSHIP_VALID,
            RELATIONSHIP_INVALID,
            RELATIONSHIP_ORIGINAL,
            RELATIONSHIP_FAILURE
    );

    public ScriptedValidateRecord() {
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
        return Optional.of(scriptResult ? RELATIONSHIP_VALID : RELATIONSHIP_INVALID);
    }
}
