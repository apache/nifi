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
package org.apache.nifi.admin.service.action;

import org.apache.nifi.admin.dao.ActionDAO;
import org.apache.nifi.admin.dao.DAOFactory;
import org.apache.nifi.history.History;
import org.apache.nifi.history.HistoryQuery;

import java.util.Date;

/**
 * Get all actions that match the specified query.
 */
public class GetActionsAction implements AdministrationAction<History> {

    private final HistoryQuery query;

    public GetActionsAction(HistoryQuery query) {
        this.query = query;
    }

    @Override
    public History execute(DAOFactory daoFactory) {
        ActionDAO actionDao = daoFactory.getActionDAO();

        // find all matching history
        History history = actionDao.findActions(query);
        history.setLastRefreshed(new Date());

        return history;
    }

}
