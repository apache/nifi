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
package org.apache.nifi.admin.service.transaction;

import java.io.Closeable;
import org.apache.nifi.admin.service.action.AdministrationAction;

/**
 * Defines a transaction.
 */
public interface Transaction extends Closeable {

    /**
     * Executes the specified action within the current transaction.
     *
     * @param <T> type of action to execute
     * @param action action to execute
     * @return executed action
     * @throws IllegalStateException - if there is no current transaction
     */
    <T> T execute(AdministrationAction<T> action);

    /**
     * Commits the current transaction.
     *
     * @throws TransactionException - if the transaction is unable to be
     * committed
     */
    void commit() throws TransactionException;

    /**
     * Rolls back the current transaction.
     */
    void rollback();
}
