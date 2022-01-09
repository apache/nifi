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

package org.apache.nifi.processors.standard;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.standard.sql.SqlWriter;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestAbstractExecuteSQL {
    private AbstractExecuteSQL testSubject;

    @Before
    public void setUp() throws Exception {
        testSubject = new AbstractExecuteSQL() {
            @Override
            protected SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context, FlowFile fileToProcess) {
                return null;
            }
        };
    }

    @Test
    public void testGetQueries() throws Exception {
        // GIVEN
        String queriesString = "SOME kind of PRE-QUERY statement;\n" +
            "AND another PRE-QUERY statment;";

        List<String> expected = Arrays.asList(
            "SOME kind of PRE-QUERY statement",
            "AND another PRE-QUERY statment"
        );

        // WHEN
        List<String> actual = testSubject.getQueries(queriesString);

        // THEN
        assertEquals(expected, actual);
    }

    @Test
    public void testGetQueriesWithEscapedSemicolon() throws Exception {
        // GIVEN
        String queriesString = "SET COMPLEX_KEY = 'KEYPART_1=value1\\;KEYPART_2=<valuePart2>\\;’ FOR SESSION;\n" +
            "SOME other PRE-QUERY statement;\n" +
            "AND another PRE-QUERY statment;";

        List<String> expected = Arrays.asList(
            "SET COMPLEX_KEY = 'KEYPART_1=value1;KEYPART_2=<valuePart2>;’ FOR SESSION",
            "SOME other PRE-QUERY statement",
            "AND another PRE-QUERY statment"
        );

        // WHEN
        List<String> actual = testSubject.getQueries(queriesString);

        // THEN
        assertEquals(expected, actual);
    }
}
