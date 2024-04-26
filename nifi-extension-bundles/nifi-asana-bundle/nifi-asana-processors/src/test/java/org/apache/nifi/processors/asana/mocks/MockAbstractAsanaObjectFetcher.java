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
package org.apache.nifi.processors.asana.mocks;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AbstractAsanaObjectFetcher;

public class MockAbstractAsanaObjectFetcher extends AbstractAsanaObjectFetcher {

    public Collection<AsanaObject> items = emptyList();
    public int pollCount = 0;

    @Override
    protected Iterator<AsanaObject> fetch() {
        pollCount++;
        Collection<AsanaObject> result = new ArrayList<>(items);
        items = emptyList();
        return result.iterator();
    }

    @Override
    public Map<String, String> saveState() {
        return null;
    }

    @Override
    public void loadState(Map<String, String> state) {

    }

    @Override
    public void clearState() {

    }
}
