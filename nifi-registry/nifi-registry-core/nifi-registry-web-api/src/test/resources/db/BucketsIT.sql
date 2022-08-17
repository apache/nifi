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

-- test data for buckets

insert into BUCKET (id, name, description, created)
  values ('1', 'Bucket 1', 'This is test bucket 1', TIMESTAMP'2017-09-11 12:51:00.000');

insert into BUCKET (id, name, description, created)
  values ('2', 'Bucket 2', 'This is test bucket 2', TIMESTAMP'2017-09-11 12:52:00.000');

insert into BUCKET (id, name, description, created)
  values ('3', 'Bucket 3', 'This is test bucket 3', TIMESTAMP'2017-09-11 12:53:00.000');

