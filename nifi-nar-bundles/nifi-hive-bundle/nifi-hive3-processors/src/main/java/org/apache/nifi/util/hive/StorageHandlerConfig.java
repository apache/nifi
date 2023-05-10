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
package org.apache.nifi.util.hive;

import java.util.Map;

/**
 * This interface is used to provide storage-handler-specific configuration information
 */
public interface StorageHandlerConfig {

    /**
     * Returns the class name associated with the storage handler implementation
     * @return the storage handler class name
     */
    String getStorageHandlerClassName();

    /**
     * Returns a map of the table properties needed to support this storage handler implementation
     * @return the table property map
     */
    Map<String,String> getTablePropertiesMap();
}
