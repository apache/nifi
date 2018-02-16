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

package org.apache.nifi.controller.repository;

import java.util.Map;
import java.util.WeakHashMap;

import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.TerminatedTaskException;

public class WeakHashMapProcessSessionFactory implements ActiveProcessSessionFactory {
    private final ProcessSessionFactory delegate;
    private final Map<ProcessSession, Boolean> sessionMap = new WeakHashMap<>();
    private boolean terminated = false;

    public WeakHashMapProcessSessionFactory(final ProcessSessionFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public synchronized ProcessSession createSession() {
        if (terminated) {
            throw new TerminatedTaskException();
        }

        final ProcessSession session = delegate.createSession();
        sessionMap.put(session, Boolean.TRUE);
        return session;
    }

    @Override
    public synchronized void terminateActiveSessions() {
        terminated = true;
        for (final ProcessSession session : sessionMap.keySet()) {
            try {
                session.rollback();
            } catch (final TerminatedTaskException tte) {
                // ignore
            }
        }

        sessionMap.clear();
    }
}
