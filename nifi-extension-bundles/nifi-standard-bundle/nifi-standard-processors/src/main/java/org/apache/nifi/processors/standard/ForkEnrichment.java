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

package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.enrichment.EnrichmentRole;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@SupportsBatching(defaultDuration = DefaultRunDuration.TWENTY_FIVE_MILLIS)
@SideEffectFree
@SeeAlso(JoinEnrichment.class)
@WritesAttributes({
    @WritesAttribute(attribute = "enrichment.group.id", description = "The Group ID to use in order to correlate the 'original' FlowFile with the 'enrichment' FlowFile."),
    @WritesAttribute(attribute = "enrichment.role", description = "The role to use for enrichment. This will either be ORIGINAL or ENRICHMENT.")
})
@CapabilityDescription("Used in conjunction with the JoinEnrichment processor, this processor is responsible for adding the attributes that are necessary for the JoinEnrichment processor to perform" +
    " its function. Each incoming FlowFile will be cloned. The original FlowFile will have appropriate attributes added and then be transferred to the 'original' relationship. The clone will have" +
    " appropriate attributes added and then be routed to the 'enrichment' relationship. See the documentation for the JoinEnrichment processor (and especially its Additional Details) for more" +
    " information on how these Processors work together and how to perform enrichment tasks in NiFi by using these Processors.")
@Tags({"fork", "join", "enrich", "record"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class ForkEnrichment extends AbstractProcessor {
    static final String ENRICHMENT_ROLE = "enrichment.role";
    static final String ENRICHMENT_GROUP_ID = "enrichment.group.id";

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("The incoming FlowFile will be routed to this relationship, after adding appropriate attributes.")
        .build();
    static final Relationship REL_ENRICHMENT = new Relationship.Builder()
        .name("enrichment")
        .description("A clone of the incoming FlowFile will be routed to this relationship, after adding appropriate attributes.")
        .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_ORIGINAL,
            REL_ENRICHMENT
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final String groupId = UUID.randomUUID().toString();

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ENRICHMENT_GROUP_ID, groupId);
        attributes.put(ENRICHMENT_ROLE, EnrichmentRole.ORIGINAL.name());
        original = session.putAllAttributes(original, attributes);

        attributes.put(ENRICHMENT_ROLE, EnrichmentRole.ENRICHMENT.name());
        FlowFile enrichment = session.clone(original);
        enrichment = session.putAllAttributes(enrichment, attributes);

        session.transfer(original, REL_ORIGINAL);
        session.transfer(enrichment, REL_ENRICHMENT);
    }
}
