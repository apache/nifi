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
package org.apache.nifi.processors.jmx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ListFilter {
    private String cachedListString = "";
    private Map<String,ArrayList<String>> listM = null;

    public static final boolean WHITELIST = true;
    public static final boolean BLACKLIST = false;


    public ListFilter( String listString ) {
        setListString( listString );
    }


    public void setListString( String listString ) {
        if (listString == null || listString.isEmpty() ) listString = "";

        if (!listString.equals(cachedListString) || listM == null) {
            buildList( listString );
        }
    }


    private void buildList(String listString) {
        listM = new HashMap<>();

        if (!listString.isEmpty()) {
            String[] listElementsAr = listString.replaceAll("\\s+"," ").split(",");

            for (int i = 0; i < listElementsAr.length; i++) {
                String[] listPartsAr = listElementsAr[i].split(":");
                ArrayList<String> listTypesAL = new ArrayList<String>();

                if (listPartsAr.length > 1) {
                    String[] listTypeElementsAr = listPartsAr[1].split(" ");

                    for (String s : listTypeElementsAr) {
                        if (!s.replaceAll("\\s+","").isEmpty()) {
                            listTypesAL.add(s);
                        }
                    }
                }

                listM.put(listPartsAr[0], listTypesAL);
            }
        }

        cachedListString = listString;
    }



    /*
     * domain and type values can contain regular expressions
     * First check if the domain key in matched in the Map.  If yes then check if the type
     * is matched in the ArrayList pointed to by the domain key.
     * Is in list if...
     * domain key is not matched: false
     * domain key is matched and there are no entries in the ArrayList: true
     * domain key is matched and there are entries in the ArrayList but type value is not matched: false
     * domain key is matched and there are entries in the ArrayList and the type value is matched: true
     */
    public boolean isInList(boolean listStyle, String domain, String type) {
        // not in the list...
        // whitelist = true since not defining a value for this style means the everything is included
        // blacklist = false since not defining a value for this style means nothing is excluded
        if (listM.size() == 0) {
            return (listStyle) ? true : false;
        }

        if (domain == null || domain.isEmpty()) {
            return (listStyle) ? true : false;
        }

        if (type == null) type = "";

        Set<String> keys = listM.keySet();

        for (String key : keys) {
            if (domain.matches(key)) {
                ArrayList<String> al = listM.get(key);
                if (al.size() == 0) {
                    return true;
                } else {
                    for (String s : al) {
                        if (type.matches(s)) {
                            return true;
                        }
                    }
                    return false;
                }
            }
        }
        return false;
    }


    public Map<String, ArrayList<String>> getList() {
        return listM;
    }
}
