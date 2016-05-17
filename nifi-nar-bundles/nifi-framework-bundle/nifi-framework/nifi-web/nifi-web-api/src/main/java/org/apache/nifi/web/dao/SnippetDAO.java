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
package org.apache.nifi.web.dao;

import org.apache.nifi.controller.Snippet;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.SnippetDTO;

public interface SnippetDAO {

    /**
     * Copies the specified snippet and added the copy to the flow in the specified group.
     *
     * @param groupId group id
     * @param snippetId snippet id
     * @param originX x
     * @param originY y
     * @param idGenerationSeed the seed to use for generating UUID's. May be null.
     * @return snippet
     */
    FlowSnippetDTO copySnippet(String groupId, String snippetId, Double originX, Double originY, String idGenerationSeed);

    /**
     * Creates a snippet.
     *
     * @param snippetDTO snippet
     * @return The snippet
     */
    Snippet createSnippet(SnippetDTO snippetDTO);

    /**
     * Determines if the specified snippet exists.
     *
     * @param snippetId snippet id
     * @return true if the snippet exists
     */
    boolean hasSnippet(String snippetId);

    /**
     * Gets the specified snippet.
     *
     * @param snippetId The snippet id
     * @return The snippet
     */
    Snippet getSnippet(String snippetId);

    /**
     * Verifies the specified snippet can be updated.
     *
     * @param snippetDTO snippet
     */
    void verifyUpdate(SnippetDTO snippetDTO);

    /**
     * Updates the specified snippet.
     *
     * @param snippetDTO snippet
     * @return The snippet
     */
    Snippet updateSnippet(SnippetDTO snippetDTO);

    /**
     * Verifies the specified snippet can be removed.
     *
     * @param snippetId snippet id
     */
    void verifyDelete(String snippetId);

    /**
     * Deletes the specified snippet.
     *
     * @param snippetId The snippet id
     */
    void deleteSnippet(String snippetId);
}
