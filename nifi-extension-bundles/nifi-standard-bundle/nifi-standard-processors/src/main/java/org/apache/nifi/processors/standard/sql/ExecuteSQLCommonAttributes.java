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

package org.apache.nifi.processors.standard.sql;

import org.apache.nifi.flowfile.attributes.FragmentAttributes;

public interface ExecuteSQLCommonAttributes {
    String RESULT_ROW_COUNT = "executesql.row.count";
    String RESULT_QUERY_DURATION = "executesql.query.duration";
    String RESULT_QUERY_EXECUTION_TIME = "executesql.query.executiontime";
    String RESULT_QUERY_FETCH_TIME = "executesql.query.fetchtime";
    String RESULTSET_INDEX = "executesql.resultset.index";
    String END_OF_RESULTSET_FLAG = "executesql.end.of.resultset";
    String RESULT_ERROR_MESSAGE = "executesql.error.message";
    String INPUT_FLOWFILE_UUID = "input.flowfile.uuid";
    String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
}
