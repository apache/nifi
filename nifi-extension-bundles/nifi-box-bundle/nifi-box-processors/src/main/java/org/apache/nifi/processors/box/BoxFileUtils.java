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

import com.box.sdkgen.schemas.file.File;
import com.box.sdkgen.schemas.folder.Folder;
import com.box.sdkgen.schemas.foldermini.FolderMini;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.valueOf;

public final class BoxFileUtils {

    public static final String BOX_URL = "https://app.box.com/file/";

    public static String getParentIds(final File fileInfo) {
        if (fileInfo.getPathCollection() == null || fileInfo.getPathCollection().getEntries() == null) {
            return "";
        }
        return fileInfo.getPathCollection().getEntries().stream()
                .map(FolderMini::getId)
                .collect(Collectors.joining(","));
    }

    public static String getParentPath(final File fileInfo) {
        if (fileInfo.getPathCollection() == null || fileInfo.getPathCollection().getEntries() == null) {
            return "/";
        }
        return "/" + fileInfo.getPathCollection().getEntries().stream()
                .filter(pathItem -> !pathItem.getId().equals("0"))
                .map(FolderMini::getName)
                .collect(Collectors.joining("/"));
    }

    public static String getParentPath(final List<FolderMini> pathCollection) {
        if (pathCollection == null) {
            return "/";
        }
        return "/" + pathCollection.stream()
                .filter(pathItem -> !pathItem.getId().equals("0"))
                .map(FolderMini::getName)
                .collect(Collectors.joining("/"));
    }

    public static String getFolderPath(final Folder folderInfo) {
        final String parentFolderPath = getParentPathForFolder(folderInfo);
        return "/".equals(parentFolderPath) ? parentFolderPath + folderInfo.getName() : parentFolderPath + "/" + folderInfo.getName();
    }

    private static String getParentPathForFolder(final Folder folderInfo) {
        if (folderInfo.getPathCollection() == null || folderInfo.getPathCollection().getEntries() == null) {
            return "/";
        }
        return "/" + folderInfo.getPathCollection().getEntries().stream()
                .filter(pathItem -> !pathItem.getId().equals("0"))
                .map(FolderMini::getName)
                .collect(Collectors.joining("/"));
    }

    public static Map<String, String> createAttributeMap(final File fileInfo) {
        final Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put(BoxFileAttributes.ID, fileInfo.getId());
        attributes.put(CoreAttributes.FILENAME.key(), fileInfo.getName());
        attributes.put(CoreAttributes.PATH.key(), getParentPath(fileInfo));
        if (fileInfo.getModifiedAt() != null) {
            attributes.put(BoxFileAttributes.TIMESTAMP, valueOf(fileInfo.getModifiedAt()));
        }
        attributes.put(BoxFileAttributes.SIZE, valueOf(fileInfo.getSize()));
        return attributes;
    }
}
