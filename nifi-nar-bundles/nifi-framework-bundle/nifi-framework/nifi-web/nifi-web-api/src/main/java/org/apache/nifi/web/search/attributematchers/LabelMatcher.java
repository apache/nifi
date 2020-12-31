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
package org.apache.nifi.web.search.attributematchers;

import org.apache.nifi.controller.label.Label;
import org.apache.nifi.web.search.query.SearchQuery;

import java.util.List;

import static org.apache.nifi.web.search.attributematchers.AttributeMatcher.addIfMatching;

public class LabelMatcher implements AttributeMatcher<Label>  {
    private static final String LABEL_ID = "Id";
    private static final String LABEL_VALUE = "Value";

    @Override
    public void match(final Label component, final SearchQuery query, final List<String> matches) {
        final String searchTerm = query.getTerm();

        addIfMatching(searchTerm, component.getIdentifier(), LABEL_ID, matches);
        addIfMatching(searchTerm, component.getValue(), LABEL_VALUE, matches);
    }
}
