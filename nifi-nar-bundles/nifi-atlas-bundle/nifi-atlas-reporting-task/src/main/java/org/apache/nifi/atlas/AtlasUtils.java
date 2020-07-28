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
package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasObjectId;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;

public class AtlasUtils {

    public static String toStr(Object obj) {
        return obj != null ? obj.toString() : null;
    }


    public static boolean isGuidAssigned(String guid) {
        return guid != null && !guid.startsWith("-");
    }

    public static String toQualifiedName(String namespace, String componentId) {
        return componentId + "@" + namespace;
    }

    public static String getComponentIdFromQualifiedName(String qualifiedName) {
        return qualifiedName.split("@")[0];
    }

    public static String getNamespaceFromQualifiedName(String qualifiedName) {
        return qualifiedName.split("@")[1];
    }

    public static String toTypedQualifiedName(String typeName, String qualifiedName) {
        return typeName + "::" + qualifiedName;
    }

    public static boolean isUpdated(Object current, Object arg) {
        if (current == null) {
            // Null to something.
            return arg != null;
        }

        // Something to something.
        return !current.equals(arg);
    }

    public static void updateMetadata(AtomicBoolean updatedTracker, List<String> updateAudit,
                                      String subject, Object currentValue, Object newValue) {
        if (isUpdated(currentValue, newValue)) {
            updatedTracker.set(true);
            updateAudit.add(String.format("%s changed from %s to %s", subject, currentValue, newValue));
        }
    }

    public static Optional<AtlasObjectId> findIdByQualifiedName(Set<AtlasObjectId> ids, String qualifiedName) {
        return ids.stream().filter(id -> qualifiedName.equals(id.getUniqueAttributes().get(ATTR_QUALIFIED_NAME))).findFirst();
    }

}
