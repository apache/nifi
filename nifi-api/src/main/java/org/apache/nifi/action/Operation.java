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
package org.apache.nifi.action;

/**
 * Defines possible operations for a given action.
 */
public enum Operation {

    Add("Add"),
    Remove("Remove"),
    Paste("Paste"),
    Configure("Configure"),
    Move("Move"),
    Disconnect("Disconnect"),
    Connect("Connect"),
    Start("Start"),
    Stop("Stop"),
    Enable("Enable"),
    Disable("Disable"),
    Batch("Batch"),
    Purge("Purge"),
    ClearState("Clear State"),
    StartVersionControl("Start Version Control"),
    StopVersionControl("Stop Version Control"),
    CommitLocalChanges("Commit Local Changes"),
    RevertLocalChanges("Revert Local Changes"),
    ChangeVersion("Change Version");

    private final String label;

    Operation(String label) {
        this.label = label;
    }

    @Override
    public String toString() {
        return label;
    }
}
