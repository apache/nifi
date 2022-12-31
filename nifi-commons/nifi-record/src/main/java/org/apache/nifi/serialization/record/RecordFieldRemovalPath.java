/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.serialization.record;

import java.util.ArrayList;
import java.util.List;

public class RecordFieldRemovalPath {
    private final List<String> path;

    public RecordFieldRemovalPath() {
        path = new ArrayList<>();
    }

    private RecordFieldRemovalPath(final List<String> path) {
        this.path = path;
    }

    public void add(final String fieldName) {
        path.add(fieldName);
    }

    public int length() {
        return path.size();
    }

    public String head() {
        return path.get(path.size() - 1);
    }

    public RecordFieldRemovalPath tail() {
        return new RecordFieldRemovalPath(path.subList(0, path.size() - 1));
    }
}
