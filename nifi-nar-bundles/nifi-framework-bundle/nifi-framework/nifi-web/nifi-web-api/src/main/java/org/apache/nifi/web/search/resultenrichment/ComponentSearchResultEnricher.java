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
package org.apache.nifi.web.search.resultenrichment;

import org.apache.nifi.web.api.dto.search.ComponentSearchResultDTO;

/**
 * Responsible for enriching the query result based on the containing component (for example process group).
 */
public interface ComponentSearchResultEnricher {
    /**
     * Enriches the incoming result object. Might change it or set one or more unset parameter.
     *
     * @param componentSearchResult The non-enriched result.
     *
     * @return The enriched result.
     */
    ComponentSearchResultDTO enrich(ComponentSearchResultDTO componentSearchResult);
}
