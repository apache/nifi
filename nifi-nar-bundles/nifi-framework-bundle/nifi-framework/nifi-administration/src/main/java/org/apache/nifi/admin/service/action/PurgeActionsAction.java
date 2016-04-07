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

import org.apache.nifi.action.Action;
import org.apache.nifi.admin.dao.ActionDAO;
import org.apache.nifi.admin.dao.DAOFactory;

import java.util.Date;

/**
 * Purges actions up to a specified end date.
 */
public class PurgeActionsAction implements AdministrationAction<Void> {

    private final Date end;
    private final Action purgeAction;

    public PurgeActionsAction(Date end, Action purgeAction) {
        this.end = end;
        this.purgeAction = purgeAction;
    }

    @Override
    public Void execute(DAOFactory daoFactory) {
        ActionDAO actionDao = daoFactory.getActionDAO();

        // remove the corresponding actions
        actionDao.deleteActions(end);

        // create a purge action
        actionDao.createAction(purgeAction);

        return null;
    }

}
