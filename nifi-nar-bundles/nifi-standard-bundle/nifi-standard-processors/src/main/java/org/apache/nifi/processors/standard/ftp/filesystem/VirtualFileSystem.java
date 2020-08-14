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
package org.apache.nifi.processors.standard.ftp.filesystem;

import java.util.List;

/**
 * The interface for virtual file system implementations. Provides the methods for the basic file system functionality.
 */
public interface VirtualFileSystem {

    /**
     * The virtual root folder of the virtual file system regardless of the operating system and its native file system structure.
     */
    VirtualPath ROOT = new VirtualPath("/");

    /**
     * Makes a new directory specified by the path received as a parameter.
     *
     * @param newFile The path that points to the directory that should be created.
     * @return        <code>true</code> if the new directory got created;
     *                <code>false</code> otherwise.
     */
    boolean mkdir(VirtualPath newFile);

    /**
     * Checks if the path received as a parameter already exists in the file system.
     *
     * @param virtualFile The path that may or may not exist in the file system.
     * @return            <code>true</code> if the specified path already exists in the file system;
     *                    <code>false</code> otherwise.
     */
    boolean exists(VirtualPath virtualFile);

    /**
     * Deletes the file or directory specified by the path received as a parameter.
     *
     * @param virtualFile The path pointing to the file or directory that should be deleted.
     * @return            <code>true</code> if the file or directory got deleted;
     *                    <code>false</code> otherwise.
     */
    boolean delete(VirtualPath virtualFile);

    /**
     * Lists the files and directories that are directly under the directory specified by the path received as a parameter.
     *
     * @param parent The path specifying the directory the contents of which should be listed.
     * @return       The list of paths that point to the contents of the parent directory.
     */
    List<VirtualPath> listChildren(VirtualPath parent);

    /**
     * Returns the number of all the files and folders in the file system.
     *
     * @return The number of files and folders in the file system.
     */
    int getTotalNumberOfFiles();

}
