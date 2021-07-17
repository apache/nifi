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
package org.apache.nifi.rules;

import java.util.HashMap;
import java.util.Map;

/**
 * An Action that should be performed given a fact has met the condition specified in a Rule.
 * The type of action is dictated by the type field and attributes are used as parameters to configure
 * the Action's executor/handler
 */
public class Action implements Cloneable{
    private String type;
    private Map<String,String> attributes;

    public Action() {
    }

    public Action(String type, Map<String, String> attributes) {
        this.type = type;
        this.attributes = attributes;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public Action clone(){
        Action action = new Action();
        action.setType(type);
        Map<String, String> attributeMap = new HashMap<>(attributes);
        action.setAttributes(attributeMap);
        return action;
    }

}
