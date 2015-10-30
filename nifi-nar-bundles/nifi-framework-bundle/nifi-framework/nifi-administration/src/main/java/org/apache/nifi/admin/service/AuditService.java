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
package org.apache.nifi.admin.service;

import org.apache.nifi.action.Action;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.history.PreviousValue;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Allows NiFi actions to be audited.
 */
public interface AuditService {

    /**
     * Adds the specified actions.
     *
     * @param actions to add
     * @throws AdministrationException if failed to add
     */
    void addActions(Collection<Action> actions);

    /**
     * @param componentId identifier of the component
     * @return Finds the previous values for the specified property in the
     * specified component. Returns null if there are none
     */
    Map<String, List<PreviousValue>> getPreviousValues(String componentId);

    /**
     * Get the actions within the given date range.
     *
     * @param actionQuery query
     * @return history of actions
     * @throws AdministrationException ae
     */
    History getActions(HistoryQuery actionQuery);

    /**
     * Get the actions starting with firstActionId, returning up to maxActions.
     *
     * @param firstActionId the offset
     * @param maxActions the number of actions to return
     * @return history of actions matching the above conditions
     */
    History getActions(final int firstActionId, final int maxActions);

    /**
     * Get the details for the specified action id. If the action cannot be
     * found, null is returned.
     *
     * @param actionId identifier of action
     * @return the action
     */
    Action getAction(Integer actionId);

    /**
     * Purges all action's that occurred before the specified end date.
     *
     * @param end the stopper for event purging
     * @param purgeAction the action
     * @throws AdministrationException ae
     */
    void purgeActions(Date end, Action purgeAction);
}
