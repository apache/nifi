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
package org.apache.nifi.controller.label;

import org.apache.nifi.authorization.resource.ComponentAuthorizable;
import org.apache.nifi.connectable.Positionable;
import org.apache.nifi.connectable.Size;
import org.apache.nifi.groups.ProcessGroup;

import java.util.Map;

public interface Label extends ComponentAuthorizable, Positionable {

    String getIdentifier();

    Map<String, String> getStyle();

    void setStyle(Map<String, String> style);

    Size getSize();

    void setSize(Size size);

    ProcessGroup getProcessGroup();

    void setProcessGroup(ProcessGroup group);

    String getValue();

    void setValue(String value);
}
