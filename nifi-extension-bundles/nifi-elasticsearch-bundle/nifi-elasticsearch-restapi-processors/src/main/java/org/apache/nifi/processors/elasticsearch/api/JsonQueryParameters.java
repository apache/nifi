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

package org.apache.nifi.processors.elasticsearch.api;

public class JsonQueryParameters {
    private int hitCount = 0;
    private String query;
    private String index;
    private String type;
    private String queryAttr;

    public String getQuery() {
        return query;
    }

    public void setQuery(final String query) {
        this.query = query;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(final String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public String getQueryAttr() {
        return queryAttr;
    }

    public void setQueryAttr(final String queryAttr) {
        this.queryAttr = queryAttr;
    }

    public int getHitCount() {
        return hitCount;
    }

    public void setHitCount(final int hitCount) {
        this.hitCount = hitCount;
    }

    public void addHitCount(final int hitCount) {
        this.hitCount += hitCount;
    }
}
