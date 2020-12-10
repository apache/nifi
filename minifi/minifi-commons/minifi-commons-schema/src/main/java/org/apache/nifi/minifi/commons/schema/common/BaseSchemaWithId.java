/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.minifi.commons.schema.common;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ID_KEY;

public class BaseSchemaWithId extends BaseSchema implements WritableSchema {
    private final String wrapperName;
    private String id;

    public BaseSchemaWithId(Map map, String wrapperName) {
        id = getId(map, wrapperName);
        this.wrapperName = wrapperName;
    }

    protected String getId(Map map, String wrapperName) {
        return getOptionalKeyAsType(map, ID_KEY, String.class, wrapperName, "");
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public String getWrapperName() {
        return wrapperName.replace("{id}", StringUtil.isNullOrEmpty(id) ? "unknown" : id);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = mapSupplier.get();
        map.put(ID_KEY, id);
        return map;
    }

    @Override
    public List<String> getValidationIssues() {
        List<String> validationIssues = super.getValidationIssues();
        if (StringUtil.isNullOrEmpty(id)) {
            validationIssues.add(getIssueText(CommonPropertyKeys.ID_KEY, getWrapperName(), IT_WAS_NOT_FOUND_AND_IT_IS_REQUIRED));
        } else if (!isValidId(id)) {
            validationIssues.add(getIssueText(CommonPropertyKeys.ID_KEY, getWrapperName(), "Id value of " + id + " is not a valid UUID"));
        }
        return validationIssues;
    }

    protected boolean isValidId(String value) {
        try {
            UUID.fromString(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}