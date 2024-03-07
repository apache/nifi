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

package org.apache.nifi.asset;

import java.io.File;

/**
 * An Asset is a representation of some resource that is necessary in order to run a dataflow.
 * An Asset is always accessed as a local file.
 */
public interface Asset {

    /**
     * Returns a unique identifier for the Asset
     */
    String getIdentifier();

    /**
     * Returns the name of the Asset
     */
    String getName();

    /**
     * Returns the local file that the Asset is associated with
     */
    File getFile();

}
