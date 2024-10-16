/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.registry.flow.git.client;

import org.apache.nifi.registry.flow.FlowRegistryException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Client for a git repository.
 */
public interface GitRepositoryClient {

    /**
     * @return true if the client can read from the configured repository, false otherwise
     */
    boolean hasReadPermission();

    /**
     * @return true if the client can write from the configured repository, false otherwise
     */
    boolean hasWritePermission();

    /**
     * Retrieves the names of the branches.
     *
     * @return the set of branch names
     * @throws IOException if an I/O error occurs
     * @throws FlowRegistryException if a non-I/O error occurs
     */
    Set<String> getBranches() throws IOException, FlowRegistryException;

    /**
     * Retrieves the top level directory names which represent buckets.
     *
     * @param branch the branch to retrieve from
     * @return the set of directory names
     * @throws IOException if an I/O error occurs
     * @throws FlowRegistryException if a non-I/O error occurs
     */
    Set<String> getTopLevelDirectoryNames(String branch) throws IOException, FlowRegistryException;

    /**
     * Retrieves the filenames in the given directory on the given branch.
     *
     * @param directory the directory
     * @param branch the branch name
     * @return the set of filenames
     * @throws IOException if an I/O error occurs
     * @throws FlowRegistryException if a non-I/O error occurs
     */
    Set<String> getFileNames(String directory, String branch) throws IOException, FlowRegistryException;

    /**
     * Retrieves the commits for the file at the given path on the given branch.
     *
     * @param path the path of the file
     * @param branch the branch name
     * @return the list of commits
     * @throws IOException if an I/O error occurs
     * @throws FlowRegistryException if a non-I/O error occurs
     */
    List<GitCommit> getCommits(String path, String branch) throws IOException, FlowRegistryException;

    /**
     * Retrieves the content for the file at the given path on the given branch.
     *
     * @param path the path of the file
     * @param branch the branch name
     * @return the input stream for the content
     * @throws IOException if an I/O error occurs
     * @throws FlowRegistryException if a non-I/O error occurs
     */
    InputStream getContentFromBranch(String path, String branch) throws IOException, FlowRegistryException;

    /**
     * Retrieves the content for the file at the given path from the given commit.
     *
     * @param path the path of the file
     * @param commitSha the commit for the content
     * @return the input stream for the content
     * @throws IOException if an I/O error occurs
     * @throws FlowRegistryException if a non-I/O error occurs
     */
    InputStream getContentFromCommit(String path, String commitSha) throws IOException, FlowRegistryException;

    /**
     * Retrieves the SHA of the file at the given path on the given branch.
     *
     * @param path the path of the file
     * @param branch the branch
     * @return the SHA of the content
     * @throws IOException if an I/O error occurs
     * @throws FlowRegistryException if a non-I/O error occurs
     */
    Optional<String> getContentSha(String path, String branch) throws IOException, FlowRegistryException;

    /**
     * Creates a file in the repository based on the given request.
     *
     * @param request the request
     * @return the SHA of the commit that created the file
     * @throws IOException if an I/O error occurs
     * @throws FlowRegistryException if a non-I/O error occurs
     */
    String createContent(GitCreateContentRequest request) throws IOException, FlowRegistryException;

    /**
     * Deletes the file at the given path on the given branch.
     *
     * The caller of this method is responsible for closing the returned InputStream.
     *
     * @param filePath the path of the file
     * @param commitMessage the commit message
     * @param branch the branch
     * @return the input stream to the deleted content
     * @throws IOException if an I/O error occurs
     * @throws FlowRegistryException if a non-I/O error occurs
     */
    InputStream deleteContent(String filePath, String commitMessage, String branch) throws FlowRegistryException, IOException;

    /**
     * Closes any resources held by the client.
     *
     * @throws IOException if an I/O error occurs closing the client
     */
    default void close() throws IOException {

    }
}
