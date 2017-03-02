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
package org.apache.nifi.admin.dao;

import org.apache.nifi.action.Action;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;
import org.apache.nifi.history.PreviousValue;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Action data access.
 */
public interface ActionDAO {

    /**
     * Persists the specified action.
     *
     * @param action to persist
     * @return the created Action with it's id
     * @throws DataAccessException if unable to persist
     */
    Action createAction(Action action) throws DataAccessException;

    /**
     * Finds all actions that meet the specified criteria.
     *
     * @param actionQuery query for actions
     * @return History of actions
     * @throws DataAccessException dae
     */
    History findActions(HistoryQuery actionQuery) throws DataAccessException;

    /**
     * @param componentId to get previous values of
     * @return Finds the previous values for the specified property in the
     * specified component. Returns empty list if there are none
     */
    Map<String, List<PreviousValue>> getPreviousValues(String componentId);

    /**
     * Finds the specified action.
     *
     * @param actionId action identifier
     * @return Action specified
     * @throws DataAccessException dae
     */
    Action getAction(Integer actionId) throws DataAccessException;

    /**
     * Deletes all actions up to the specified end date.
     *
     * @param endDate date to stop deleting at
     * @throws DataAccessException dae
     */
    void deleteActions(Date endDate) throws DataAccessException;
}
