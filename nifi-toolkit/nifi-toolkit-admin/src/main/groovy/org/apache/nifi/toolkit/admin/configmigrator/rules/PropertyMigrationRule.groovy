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

package org.apache.nifi.toolkit.admin.configmigrator.rules

/**
 * PropertyMigrationRule supports the migration of older key/value (property) based files to new versions
 * by adding any new variables to the older file while maintaining existing configurations.  Classes that extend this rule can also filter
 * any properties that are no longer needed in new installations
 */
abstract class PropertyMigrationRule extends GenericMigrationRule{

    private final static String LICENSE_COMMENTS = "# Licensed to the Apache Software Foundation (ASF) under one or more\n" +
            "# contributor license agreements.  See the NOTICE file distributed with\n" +
            "# this work for additional information regarding copyright ownership.\n" +
            "# The ASF licenses this file to You under the Apache License, Version 2.0\n" +
            "# (the \"License\"); you may not use this file except in compliance with\n" +
            "# the License.  You may obtain a copy of the License at\n" +
            "#\n" +
            "#     http://www.apache.org/licenses/LICENSE-2.0\n" +
            "#\n" +
            "# Unless required by applicable law or agreed to in writing, software\n" +
            "# distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
            "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
            "# See the License for the specific language governing permissions and\n" +
            "# limitations under the License."

    /**
     *
     * @param oldContent
     * @param upgradeContent
     * @return
     */
    @Override
    byte[] migrate(byte[] oldContent, byte[] upgradeContent) {
        def properties = new SortedProperties()
        def upgradeProperties = new SortedProperties()

        properties.load(new ByteArrayInputStream(oldContent))
        upgradeProperties.load(new ByteArrayInputStream(upgradeContent))

        Enumeration keys = (Enumeration<Object>) properties.keys()

        keys.each { key ->
            if(keyAllowed(key)) {
                upgradeProperties.put(key, properties.get(key))
            }
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream()
        upgradeProperties.store(baos,LICENSE_COMMENTS)
        baos.toByteArray()
    }

    /**
     * Return if a key should be included in the set of properties being migrated
     * @param key
     * @return boolean
     */
    abstract boolean keyAllowed(Object key);

    class SortedProperties extends Properties{

        @Override
        public Enumeration<Object> keys() {

            Enumeration<Object> keysEnum = super.keys();
            Vector<Object> keyList = new Vector<Object>();
            keysEnum.each { e -> keyList.add(e)}

            Collections.sort(keyList,new Comparator<Object>() {
                @Override
                int compare(Object o1, Object o2) {
                    o1.toString().compareTo(o2.toString())
                }
            })

            keyList.elements()
        }


    }

}
