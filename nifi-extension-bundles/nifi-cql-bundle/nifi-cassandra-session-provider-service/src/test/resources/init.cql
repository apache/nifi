-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

create keyspace testspace with replication = { 'class': 'SimpleStrategy', 'replication_factor': 1};

create table testspace.message
(
    sender    text,
    receiver  text,
    message   text,
    when_sent timestamp,
    primary key ( sender, receiver, when_sent )
);

create table testspace.query_test
(
    column_a text,
    column_b text,
    when     timestamp,
    primary key ( (column_a), column_b)
);

create table testspace.counter_test
(
    column_a        text,
    increment_field counter,
    primary key ( column_a )
);

create table testspace.simple_set_test
(
    username text,
    is_active boolean,
    primary key ( username )
);

