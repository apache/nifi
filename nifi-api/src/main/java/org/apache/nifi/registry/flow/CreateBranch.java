/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.nifi.registry.flow;

/**
 * Information for creating a branch in a flow registry.
 */
public class CreateBranch {

    private String fromBranch;
    private String newBranch;

    public CreateBranch() {
    }

    public CreateBranch(final String fromBranch, final String newBranch) {
        this.fromBranch = fromBranch;
        this.newBranch = newBranch;
    }

    public String getFromBranch() {
        return fromBranch;
    }

    public void setFromBranch(final String fromBranch) {
        this.fromBranch = fromBranch;
    }

    public String getNewBranch() {
        return newBranch;
    }

    public void setNewBranch(final String newBranch) {
        this.newBranch = newBranch;
    }
}
