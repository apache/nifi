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

ALTER TABLE BUCKET_ITEM DROP CONSTRAINT FK__BUCKET_ITEM_BUCKET_ID;
ALTER TABLE BUCKET_ITEM ADD CONSTRAINT FK__BUCKET_ITEM_BUCKET_ID FOREIGN KEY (BUCKET_ID) REFERENCES BUCKET(ID) ON DELETE CASCADE;

ALTER TABLE FLOW DROP CONSTRAINT FK__FLOW_BUCKET_ITEM_ID;
ALTER TABLE FLOW ADD CONSTRAINT FK__FLOW_BUCKET_ITEM_ID FOREIGN KEY (ID) REFERENCES BUCKET_ITEM(ID) ON DELETE CASCADE;

ALTER TABLE FLOW_SNAPSHOT DROP CONSTRAINT FK__FLOW_SNAPSHOT_FLOW_ID;
ALTER TABLE FLOW_SNAPSHOT ADD CONSTRAINT FK__FLOW_SNAPSHOT_FLOW_ID FOREIGN KEY (FLOW_ID) REFERENCES FLOW(ID) ON DELETE CASCADE;