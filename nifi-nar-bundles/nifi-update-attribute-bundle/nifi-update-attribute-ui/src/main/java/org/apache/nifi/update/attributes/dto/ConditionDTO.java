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
package org.apache.nifi.update.attributes.dto;

import java.text.Collator;
import java.util.Locale;

import javax.xml.bind.annotation.XmlType;

/**
 *
 */
@XmlType(name = "condition")
public class ConditionDTO implements Comparable<ConditionDTO> {

    private String id;
    private String expression;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    @Override
    public int compareTo(ConditionDTO that) {
        // including the id in the comparison so that the TreeSet that this
        // is stored in does not discard any entries just because their expressions
        // are equal
        final Collator collator = Collator.getInstance(Locale.US);
        final String thisCmpStr = getExpression() + "_" + getId();
        final String thatCmpStr = that.getExpression() + "_" + that.getId();
        return collator.compare(thisCmpStr, thatCmpStr);
    }
}
