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

package org.apache.nifi.stateless.engine;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.processor.ProcessContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CachingProcessContextFactory implements ProcessContextFactory {
    private final ProcessContextFactory delegate;
    private final Map<Connectable, ProcessContext> processContexts = new ConcurrentHashMap<>();

    public CachingProcessContextFactory(final ProcessContextFactory delegate) {
        this.delegate = delegate;
    }

    @Override
    public ProcessContext createProcessContext(final Connectable connectable) {
        final ProcessContext context = processContexts.get(connectable);
        if (context != null) {
            return context;
        }

        final ProcessContext created = delegate.createProcessContext(connectable);
        processContexts.putIfAbsent(connectable, created);
        return created;
    }
}
