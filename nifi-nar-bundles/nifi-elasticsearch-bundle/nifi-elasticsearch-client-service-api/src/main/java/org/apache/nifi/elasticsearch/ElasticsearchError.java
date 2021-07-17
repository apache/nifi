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

package org.apache.nifi.elasticsearch;

import java.util.HashSet;
import java.util.Set;

public class ElasticsearchError extends RuntimeException {
    /**
     * These are names of common Elasticsearch exceptions where it is safe to assume
     * that it's OK to retry the operation instead of just sending it to an error relationship.
     */
    public static final Set<String> ELASTIC_ERROR_NAMES = new HashSet<String>(){{
        add("NoNodeAvailableException");
        add("ElasticsearchTimeoutException");
        add("ReceiveTimeoutTransportException");
        add("NodeClosedException");
    }};

    protected boolean isElastic;

    public ElasticsearchError(Exception ex) {
        super(ex);
        final boolean isKnownException = ELASTIC_ERROR_NAMES.contains(ex.getClass().getSimpleName());
        final boolean isServiceUnavailable = "ResponseException".equals(ex.getClass().getSimpleName())
                && ex.getMessage().contains("503 Service Unavailable");
        isElastic = isKnownException || isServiceUnavailable;
    }

    public boolean isElastic() {
        return isElastic;
    }
}
