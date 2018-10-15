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
package org.apache.nifi.attribute.expression.language;

import java.util.LinkedList;
import java.util.List;

public class SimpleUDF implements ExpressionLanguageExecutable{

    public String execute(String echo) {
        return echo == null? null : echo + "-" + echo;
    }

    @Override
    public String execute(Object... args) {
        if (args == null || args.length == 0) {
            return null;
        }
        List<String> allStrings = new LinkedList<>();
        for (Object arg : args) {
            allStrings.add(arg.toString());
        }
        return String.join("|", allStrings);
    }
}
