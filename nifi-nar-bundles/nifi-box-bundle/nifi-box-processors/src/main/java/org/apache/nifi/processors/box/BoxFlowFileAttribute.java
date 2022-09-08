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

import java.util.function.Function;

public enum BoxFlowFileAttribute {
    ID(BoxFileInfo.ID, BoxFileInfo::getId),
    FILENAME(BoxFileInfo.FILENAME, BoxFileInfo::getName),
    PATH(BoxFileInfo.PATH, BoxFileInfo::getPath),
    SIZE(BoxFileInfo.SIZE, fileInfo -> String.valueOf(fileInfo.getSize())),
    TIMESTAMP(BoxFileInfo.TIMESTAMP, fileInfo -> String.valueOf(fileInfo.getTimestamp()));

    private final String name;
    private final Function<BoxFileInfo, String> fromFileInfo;

    BoxFlowFileAttribute(String attributeName, Function<BoxFileInfo, String> fromFileInfo) {
        this.name = attributeName;
        this.fromFileInfo = fromFileInfo;
    }

    public String getName() {
        return name;
    }

    public String getValue(BoxFileInfo fileInfo) {
        return fromFileInfo.apply(fileInfo);
    }
}
