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
package org.apache.nifi.processors.hadoop.inotify.util;


import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.inotify.Event;

import java.util.Collections;
import java.util.Date;

public class EventTestUtils {
    public static Event.CreateEvent createCreateEvent() {
        return new Event.CreateEvent.Builder()
                .ctime(new Date().getTime())
                .groupName("group_name")
                .iNodeType(Event.CreateEvent.INodeType.DIRECTORY)
                .overwrite(false)
                .ownerName("ownerName")
                .path("/some/path/create")
                .perms(new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE))
                .replication(1)
                .symlinkTarget("/some/symlink/target")
                .build();
    }

    public static Event.CloseEvent createCloseEvent() {
        return new Event.CloseEvent("/some/path/close", 1L, 2L);
    }

    public static Event.MetadataUpdateEvent createMetadataUpdateEvent() {
        return new Event.MetadataUpdateEvent.Builder()
                .replication(0)
                .perms(new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE))
                .path("/some/path/metadata")
                .ownerName("owner")
                .acls(Collections.singletonList(new AclEntry.Builder().setName("schema").setPermission(FsAction.ALL).setScope(AclEntryScope.ACCESS).setType(AclEntryType.GROUP).build()))
                .atime(new Date().getTime())
                .groupName("groupName")
                .metadataType(Event.MetadataUpdateEvent.MetadataType.ACLS)
                .mtime(1L)
                .xAttrs(Collections.singletonList(new XAttr.Builder().setName("name").setNameSpace(XAttr.NameSpace.USER).setValue(new byte[0]).build()))
                .xAttrsRemoved(false)
                .build();
    }
}
