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

package org.apache.nifi.migration;

import org.apache.nifi.processor.Relationship;

/**
 * <p>
 * RelationshipConfiguration can be used to convey how a Processor's configuration should be migrated from an old configuration
 * to the latest configuration. This provides the ability to split a single Relationship into multiple, or to rename Relationships.
 * </p>
 * <p>
 * Note that it does not provide for the ability to merge multiple Relationships into a single Relationship, as doing so would cause unexpected
 * data duplication. Consider, for example, that a Processor has two Connections. The first has Relationship A while the second has Relationship B.
 * If we allows A and B to be merged together into C, anything that gets transferred to C would now need to go to both of those Connections, creating
 * data duplication.
 * </p>
 * <p>
 * Additionally, there is no option to remove a Relationship. This is because if a Relationship is to be removed, it can be simply dropped from the Processor.
 * Any existing connection that has that Relationship will be "ghosted" in the UI to show the user that it's no longer supported, but NiFi and the Processor
 * will continue to behave as expected.
 * </p>
 */
public interface RelationshipConfiguration {

    /**
     * Changes the Relationship from the given older name to the updated name
     * @param relationshipName the old name of the relationship
     * @param newName the new name to use
     * @return <code>true</code> if the Relationship is renamed, <code>false</code> if the given Relationship does not exist
     * @throws IllegalStateException if both the relationship name and the new name already are defined as relationships
     */
    boolean renameRelationship(String relationshipName, String newName);

    /**
     * <p>
     * Splits the given Relationship into multiple new relationships. Each connection that has the Relationship as a selected Relationship will be updated to
     * have all of the new Relationships as selected relationships instead. If the existing Relationship is auto-terminated, all of the newly defined ones
     * will be as well. If the existing Relationship is auto-retried, so will be the new Relationships.
     * </p>
     *
     * <p>
     * It is possible to split an existing relationship into the same relationship and additional relationships. For example, it is
     * valid to call this method as:
     * </p>
     * <pre><code>
     * relationshipConfiguration.splitRelationship("A", "A", "B", "C");
     * </code></pre>
     *
     * <p>
     * In order to split the "A" relationship into three relationships: "A", "B", and "C". However, upon restart, NiFi will have already split the "A"
     * relationship into three. So relationships "B" and "C" will already exist, resulting in an {@code IllegalStateException}. Therefore, if splitting
     * a relationship into multiple that include the original relationship, it is important to guard against this by checking of the new relationships
     * exist first:
     * </p>
     *
     * <pre><code>
     * if (!relationshipConfiguration.hasRelationship("B")) {
     *     relationshipConfiguration.splitRelationship("A", "A", "B", "C");
     * }
     * </code></pre>
     *
     * <p>
     * This ensures that we do not attempt to split relationship "A" if it has already been done.
     * </p>
     *
     * @param relationshipName the name of the existing relationship
     * @param newRelationshipName the first of the new relationship names
     * @param additionalRelationshipNames additional names for the new relationship
     * @return <code>true</code> if the Relationship is split, <code>false</code> if the given Relationship does not exist
     * @throws IllegalStateException if the given relationship name exists and one of the given new relationships already exists
     */
    boolean splitRelationship(String relationshipName, String newRelationshipName, String... additionalRelationshipNames);

    /**
     * Indicates whether or not the relationship with the given name exists in the configuration
     * @param relationshipName the name of the relationship
     * @return <code>true</code> if the relationship exists, <code>false</code> if the relationship is not known
     */
    boolean hasRelationship(String relationshipName);

    /**
     * Indicates whether or not the relationship with the given name exists in the configuration
     * @param relationship the relationship to check
     * @return <code>true</code> if the relationship exists, <code>false</code> if the relationship is not known
     */
    default boolean hasRelationship(Relationship relationship) {
        return hasRelationship(relationship.getName());
    }
}
