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

public class TestHiveDatabaseAdapter {
    final DatabaseAdapter db = new HiveDatabaseAdapter();

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

    @Test
    public void testPagingQuery() throws Exception {
        String sql1 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*","","contain",100L,0L);
        String expected1 = "SELECT * FROM (SELECT some(set),of(columns),that,might,contain,methods,a.*, ROW_NUMBER() OVER(ORDER BY contain ASC) rnum FROM database.tablename ORDER BY contain) A WHERE rnum > 0 AND rnum <= 100";
        Assert.assertEquals(expected1,sql1);

        String sql2 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*","","contain",10000L,123456L);
        String expected2 = "SELECT * FROM (SELECT some(set),of(columns),that,might,contain,methods,a.*, ROW_NUMBER() OVER(ORDER BY contain ASC) rnum FROM database.tablename ORDER BY contain) A WHERE rnum > 123456 AND rnum <= 133456";
        Assert.assertEquals(expected2,sql2);

        String sql3 = db.getSelectStatement("database.tablename", "some(set),of(columns),that,might,contain,methods,a.*","methods='strange'","contain",10000L,123456L);
        String expected3 = "SELECT * FROM (SELECT some(set),of(columns),that,might,contain,methods,a.*, ROW_NUMBER() OVER(ORDER BY contain ASC) rnum FROM database.tablename WHERE methods='strange' ORDER BY contain) A WHERE rnum > 123456 AND rnum <= 133456";
        Assert.assertEquals(expected3,sql3);
    }
}
