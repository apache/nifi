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

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.validation.RuleViolation;
import org.apache.nifi.validation.RuleViolationsManager;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MockRuleViolationsManager implements RuleViolationsManager {
    @Override
    public void upsertComponentViolations(final String subjectId, final Collection<RuleViolation> violations) {

    }

    @Override
    public void upsertGroupViolations(final VersionedProcessGroup processGroup, final Collection<RuleViolation> violations, final Map<VersionedComponent, Collection<RuleViolation>> componentToRuleViolations) {

    }

    @Override
    public Collection<RuleViolation> getRuleViolationsForSubject(final String subjectId) {
        return List.of();
    }

    @Override
    public Collection<RuleViolation> getRuleViolationsForGroup(final String groupId) {
        return List.of();
    }

    @Override
    public Collection<RuleViolation> getRuleViolationsForGroups(final Collection<String> groupIds) {
        return List.of();
    }

    @Override
    public Collection<RuleViolation> getAllRuleViolations() {
        return List.of();
    }

    @Override
    public void removeRuleViolationsForSubject(final String subjectId) {

    }

    @Override
    public void removeRuleViolationsForRule(final String ruleId) {

    }

    @Override
    public void cleanUp() {

    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
