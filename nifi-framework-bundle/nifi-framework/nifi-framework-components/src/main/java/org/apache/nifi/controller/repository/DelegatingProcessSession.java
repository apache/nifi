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

import org.apache.nifi.processor.ProcessSession;

/**
 * Framework-internal contract implemented by {@link ProcessSession} wrappers that forward all
 * operations to an underlying delegate Session. This allows framework code that needs to operate
 * on a concrete Session implementation (for example, {@code StandardProcessSession.migrate}, which
 * requires its target to be another {@code StandardProcessSession}) to look through any number of
 * wrapping layers and recover the underlying Session.
 *
 * Extensions must not implement this interface; it is intended for framework-internal Session
 * wrappers only.
 */
public interface DelegatingProcessSession extends ProcessSession {

    /**
     * @return the Session that this wrapper forwards operations to. May itself be a
     *         {@link DelegatingProcessSession} when wrappers are nested.
     */
    ProcessSession getDelegate();
}
