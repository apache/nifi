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
package org.apache.nifi.processors.box;

import static java.lang.String.valueOf;

import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxItem;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

public final class BoxFileUtils {

    public static final String BOX_URL = "https://app.box.com/file/";

    public static String getParentPath(BoxItem.Info info) {
        return "/" + info.getPathCollection().stream()
                .filter(pathItemInfo -> !pathItemInfo.getID().equals("0"))
                .map(BoxItem.Info::getName)
                .collect(Collectors.joining("/"));
    }

    public static String getFolderPath(BoxFolder.Info folderInfo) {
        final String parentFolderPath = getParentPath(folderInfo);
        return "/".equals(parentFolderPath) ? parentFolderPath + folderInfo.getName() : parentFolderPath + "/" + folderInfo.getName();
    }

    public static Map<String, String> createAttributeMap(BoxFile.Info fileInfo) {
        final Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put(BoxFileAttributes.ID, fileInfo.getID());
        attributes.put(CoreAttributes.FILENAME.key(), fileInfo.getName());
        attributes.put(CoreAttributes.PATH.key(), getParentPath(fileInfo));
        attributes.put(BoxFileAttributes.TIMESTAMP, valueOf(fileInfo.getModifiedAt()));
        attributes.put(BoxFileAttributes.SIZE, valueOf(fileInfo.getSize()));
        return attributes;
    }

}
