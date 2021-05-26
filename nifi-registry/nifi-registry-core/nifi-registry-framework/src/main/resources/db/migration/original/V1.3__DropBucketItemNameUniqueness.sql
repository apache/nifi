-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE ALIAS IF NOT EXISTS EXECUTE AS $$ void executeSql(Connection conn, String sql)
throws SQLException { conn.createStatement().executeUpdate(sql); } $$;

call execute('ALTER TABLE BUCKET_ITEM DROP CONSTRAINT ' ||
    (
     SELECT DISTINCT CONSTRAINT_NAME
     FROM INFORMATION_SCHEMA.CONSTRAINTS
     WHERE TABLE_NAME = 'BUCKET_ITEM'
     AND COLUMN_LIST = 'NAME'
     AND CONSTRAINT_TYPE = 'UNIQUE'
     )
);
