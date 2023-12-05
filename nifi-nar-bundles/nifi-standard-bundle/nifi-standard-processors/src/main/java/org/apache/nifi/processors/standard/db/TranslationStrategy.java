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
package org.apache.nifi.processors.standard.db;

import org.apache.nifi.components.DescribedValue;

/**
 * Enumeration of supported Database column name Translation Strategy
 */
public enum TranslationStrategy implements DescribedValue {
    REMOVE_UNDERSCORE {
        @Override
        public String getDisplayName() {
            return "Remove Underscore";
        }

        @Override
        public String getDescription() {
            return "Underscore(_) will be removed from column name with empty string Ex. Pics_1_11 become PICS111";
        }
    },
    REMOVE_SPACE {
        @Override
        public String getDisplayName() {
            return "Remove Space";
        }

        @Override
        public String getDescription() {
            return "Spaces will be removed from column name with empty string Ex. 'User Name' become 'USERNAME'";
        }
    },

    //    REMOVE_ALL_SPECIAL_CHAR("REMOVE_ALL_SPECIAL_CHAR","Remove All Special Character","Remove All Special Character"),
    REMOVE_ALL_SPECIAL_CHAR {
        @Override
        public String getDisplayName() {
            return "Remove All Special Character";
        }

        @Override
        public String getDescription() {
            return "Remove All Special Character";
        }
    },
    PATTERN{
    @Override
    public String getDisplayName() {
        return "Regular Expression";
    }

    @Override
    public String getDescription() {
        return "Remove character matched Regular Expression from column name";
    }
};


    @Override
    public String getValue() {
        return name();
    }

    }