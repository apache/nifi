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
  values ('1', 'Bucket 1', 'This is test bucket 1', '2017-09-11');

insert into BUCKET (id, name, description, created)
  values ('2', 'Bucket 2', 'This is test bucket 2', '2017-09-12');

insert into BUCKET (id, name, description, created)
  values ('3', 'Bucket 3', 'This is test bucket 3', '2017-09-13');

-- test data for flows

insert into BUCKET_ITEM (id, name, description, created, modified, item_type, bucket_id)
  values ('1', 'Flow 1', 'This is flow 1', '2017-09-11', '2017-09-11', 'FLOW', '1');

insert into FLOW (id) values ('1');

insert into BUCKET_ITEM (id, name, description, created, modified, item_type, bucket_id)
  values ('2', 'Flow 2', 'This is flow 2', '2017-09-11', '2017-09-11', 'FLOW', '1');

insert into FLOW (id) values ('2');

insert into BUCKET_ITEM (id, name, description, created, modified, item_type, bucket_id)
  values ('3', 'Flow 3', 'This is flow 3', '2017-09-11', '2017-09-11', 'FLOW', '2');

insert into FLOW (id) values ('3');

-- test data for flow snapshots

insert into FLOW_SNAPSHOT (flow_id, version, created, created_by, comments)
  values ('1', 1, '2017-09-11', 'user1', 'This is flow 1 snapshot 1');

insert into FLOW_SNAPSHOT (flow_id, version, created, created_by, comments)
  values ('1', 2, '2017-09-12', 'user2', 'This is flow 1 snapshot 2');
