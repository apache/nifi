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
package org.apache.nifi.processors.standard.db.impl;

import org.apache.nifi.processors.standard.db.DatabaseAdapter;
import org.junit.Assert;
import org.junit.Test;

public class TestMSSQLDatabaseAdapter {
    final DatabaseAdapter db = new MSSQLDatabaseAdapter();

    @Test
    public void testGeneration() throws Exception {
        String sql1 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*","","",null,null);
        String expected1 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename";
        Assert.assertEquals(sql1,expected1);

        String sql2 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*","that=\'some\"\' value\'","",null,null);
        String expected2 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename WHERE that=\'some\"\' value\'";
        Assert.assertEquals(sql2,expected2);

        String sql3 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*","that=\'some\"\' value\'","might DESC",null,null);
        String expected3 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename WHERE that=\'some\"\' value\' ORDER BY might DESC";
        Assert.assertEquals(sql3,expected3);

        String sql4 = db.getSelectStatement("database.tablename", "","that=\'some\"\' value\'","might DESC",null,null);
        String expected4 = "SELECT * FROM database.tablename WHERE that=\'some\"\' value\' ORDER BY might DESC";
        Assert.assertEquals(sql4,expected4);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoTableName() throws Exception {
        db.getSelectStatement("", "some(set),of(columns),that,might,contain,methods,a.*","","",null,null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPagingNoOrderBy() throws Exception {
        db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*","","",10L,0L);
    }

    @Test
    public void testTOPQuery() throws Exception {
        String sql1 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*","","",100L,null);
        String expected1 = "SELECT TOP 100 some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename";
        Assert.assertEquals(sql1,expected1);

        String sql2 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*","","contain",100L,null);
        String expected2 = "SELECT TOP 100 some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename ORDER BY contain";
        Assert.assertEquals(sql2,expected2);

        String sql4 = db.getSelectStatement("database.tablename", "","that=\'some\"\' value\'","might DESC",123456L,null);
        String expected4 = "SELECT TOP 123456 * FROM database.tablename WHERE that=\'some\"\' value\' ORDER BY might DESC";
        Assert.assertEquals(sql4,expected4);
    }

    @Test
    public void testPagingQuery() throws Exception {
        String sql1 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*","","contain",100L,0L);
        String expected1 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename ORDER BY contain OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY";
        Assert.assertEquals(sql1,expected1);

        String sql2 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*","","contain",10000L,123456L);
        String expected2 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename ORDER BY contain OFFSET 123456 ROWS FETCH NEXT 10000 ROWS ONLY";
        Assert.assertEquals(sql2,expected2);

        String sql3 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*","methods='strange'","contain",10000L,123456L);
        String expected3 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename WHERE methods='strange' ORDER BY contain OFFSET 123456 ROWS FETCH NEXT 10000 ROWS ONLY";
        Assert.assertEquals(sql3,expected3);
    }

    @Test
    public void testPagingQueryUsingColumnValuesForPartitioning() {
        String sql1 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "1=1", "contain",
                100L, 0L, "contain");
        String expected1 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename WHERE 1=1 AND contain >= 0 AND contain < 100";
        Assert.assertEquals(expected1, sql1);

        String sql2 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "1=1", "contain",
                10000L, 123456L, "contain");
        String expected2 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename WHERE 1=1 AND contain >= 123456 AND contain < 133456";
        Assert.assertEquals(expected2, sql2);

        String sql3 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "methods='strange'",
                "contain", 10000L, 123456L, "contain");
        String expected3 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename WHERE methods='strange' AND contain >= 123456 AND contain < 133456";
        Assert.assertEquals(expected3, sql3);

        // Paging (limit/offset) is only supported when an orderByClause is supplied, note that it is not honored here
        String sql4 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*", "", "",
                100L, null, "contain");
        String expected4 = "SELECT some(set),of(columns),that,might,contain,methods,a.* FROM database.tablename";
        Assert.assertEquals(expected4, sql4);
    }
}
