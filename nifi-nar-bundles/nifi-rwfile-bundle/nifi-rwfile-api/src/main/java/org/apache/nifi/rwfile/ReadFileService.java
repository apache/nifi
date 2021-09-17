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
package org.apache.nifi.rwfile;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Map;

@Tags({"readfile","readline"})
@CapabilityDescription("Reads the contents of a file from disk and streams it into the contents of an incoming FlowFile,Create a flowfile every time N lines are read, until all lines of the file are read. Once this is done, the file is optionally moved elsewhere or deleted to help keep the file system organized.")
public interface ReadFileService extends ControllerService {

    public void execute()  throws ProcessException;
    public Map<String,Object> readFile(String filepath,int ignore_header,String charSet,int _nrows);
    public Map<String,Object> getFragment(String filepath);
    public Map closeFile(String filepath);

}
