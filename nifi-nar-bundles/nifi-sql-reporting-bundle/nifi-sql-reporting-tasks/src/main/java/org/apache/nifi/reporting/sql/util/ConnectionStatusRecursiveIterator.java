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
package org.apache.nifi.reporting.sql.util;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.reporting.ReportingContext;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

public class ConnectionStatusRecursiveIterator implements Iterator<ConnectionStatus> {
    private final ReportingContext context;

    private Deque<Iterator<ProcessGroupStatus>> iteratorBreadcrumb;
    private Iterator<ConnectionStatus> connectionStatusIterator;
    private ConnectionStatus currentRow;

    public ConnectionStatusRecursiveIterator(final ReportingContext context) {
        this.context = context;
        iteratorBreadcrumb = new LinkedList<>();
    }

    @Override
    public boolean hasNext() {
        if (iteratorBreadcrumb.isEmpty()) {
            // Start the breadcrumb trail to follow recursively into process groups looking for connections
            ProcessGroupStatus rootStatus = context.getEventAccess().getControllerStatus();
            iteratorBreadcrumb.push(rootStatus.getProcessGroupStatus().iterator());
            connectionStatusIterator = rootStatus.getConnectionStatus().iterator();
        }

        currentRow = getNextConnectionStatus();
        return (currentRow != null);
    }

    @Override
    public ConnectionStatus next() {
        if (currentRow != null) {
            final ConnectionStatus result = currentRow;
            currentRow = null;
            return result;
        }

        if (hasNext()) {
            ConnectionStatus result = currentRow;
            currentRow = null;
            return result;
        } else {
            return null;
        }
    }

    private ConnectionStatus getNextConnectionStatus() {
        if (connectionStatusIterator != null && connectionStatusIterator.hasNext()) {
            return connectionStatusIterator.next();
        }
        // No more connections in this PG, so move to the next
        connectionStatusIterator = null;
        Iterator<ProcessGroupStatus> i = iteratorBreadcrumb.peek();
        if (i == null) {
            return null;
        }

        if (i.hasNext()) {
            ProcessGroupStatus nextPG = i.next();
            iteratorBreadcrumb.push(nextPG.getProcessGroupStatus().iterator());
            connectionStatusIterator = nextPG.getConnectionStatus().iterator();
            return getNextConnectionStatus();
        } else {
            // No more child PGs, remove it from the breadcrumb trail and try again
            iteratorBreadcrumb.pop();
            return getNextConnectionStatus();
        }
    }
}
