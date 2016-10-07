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
import java.util.regex.Pattern;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.ID_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.NAME_KEY;

public abstract class BaseSchemaWithIdAndName extends BaseSchema implements WritableSchema {
    public static final Pattern VALID_ID_PATTERN = Pattern.compile("[A-Za-z0-9_-]+");
    public static final String ID_DOES_NOT_MATCH_VALID_ID_PATTERN = "Id does not match valid pattern (" + VALID_ID_PATTERN + "): ";

    private final String wrapperName;
    private String id;
    private String name;

    public BaseSchemaWithIdAndName(Map map, String wrapperName) {
        id = getId(map, wrapperName);
        name = getName(map, wrapperName);
        this.wrapperName = wrapperName;
    }

    protected String getId(Map map, String wrapperName) {
        return getOptionalKeyAsType(map, ID_KEY, String.class, wrapperName, "");
    }

    protected String getName(Map map, String wrapperName) {
        return getOptionalKeyAsType(map, NAME_KEY, String.class, wrapperName, "");
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    protected void setName(String name) {
        this.name = name;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = mapSupplier.get();
        map.put(ID_KEY, id);
        map.put(NAME_KEY, name);
        return map;
    }

    @Override
    public List<String> getValidationIssues() {
        List<String> validationIssues = super.getValidationIssues();
        if (StringUtil.isNullOrEmpty(id)) {
            validationIssues.add(getIssueText(CommonPropertyKeys.ID_KEY, wrapperName, IT_WAS_NOT_FOUND_AND_IT_IS_REQUIRED));
        } else if (!VALID_ID_PATTERN.matcher(id).matches()) {
            validationIssues.add(ID_DOES_NOT_MATCH_VALID_ID_PATTERN + id);
        }
        return validationIssues;
    }
}
