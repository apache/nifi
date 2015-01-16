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
package org.apache.nifi.monitor.thresholds.ui;
/*
 * NOTE: rule is synonymous with threshold
 */

import java.util.Comparator;
import org.apache.nifi.settings.generated.Thresholds;

public class RuleComparator implements Comparator<Thresholds> {

    private final String column;

    public RuleComparator(String col) {
        column = col;
    }

    @Override
    public int compare(Thresholds rule1, Thresholds rule2) {
        if (column.compareTo("attributevalue") == 0) {
            return rule1.getId().compareTo(rule2.getId());
        }

        if (column.compareTo("size") == 0) {
            return rule1.getSize().compareTo(rule2.getSize());
        } else {
            return rule1.getCount().compareTo(rule2.getCount());
        }

    }
}
