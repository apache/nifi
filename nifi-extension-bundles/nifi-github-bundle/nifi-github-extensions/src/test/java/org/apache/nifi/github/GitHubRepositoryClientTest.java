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

package org.apache.nifi.github;

import org.junit.jupiter.api.Test;
import org.kohsuke.github.GHCommit;
import org.kohsuke.github.GHFileNotFoundException;
import org.kohsuke.github.GHUser;
import org.kohsuke.github.GitUser;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GitHubRepositoryClientTest {

    @Test
    void testResolveCommitAuthorReturnsLoginWhenAccountResolvable() throws IOException {
        final GHUser ghUser = mock(GHUser.class);
        when(ghUser.getLogin()).thenReturn("octocat");

        final GHCommit ghCommit = mock(GHCommit.class);
        when(ghCommit.getAuthor()).thenReturn(ghUser);

        final GHCommit.ShortInfo shortInfo = mock(GHCommit.ShortInfo.class);

        final String author = GitHubRepositoryClient.resolveCommitAuthor(ghCommit, shortInfo);
        assertEquals("octocat", author);
    }

    @Test
    void testResolveCommitAuthorFallsBackToGitAuthorWhenAccountNotFound() throws IOException {
        final GHCommit ghCommit = mock(GHCommit.class);
        when(ghCommit.getAuthor()).thenThrow(new GHFileNotFoundException("/users/Copilot 404 Not Found"));

        final GitUser gitAuthor = mock(GitUser.class);
        when(gitAuthor.getName()).thenReturn("Copilot");
        final GHCommit.ShortInfo shortInfo = mock(GHCommit.ShortInfo.class);
        when(shortInfo.getAuthor()).thenReturn(gitAuthor);

        final String author = GitHubRepositoryClient.resolveCommitAuthor(ghCommit, shortInfo);
        assertEquals("Copilot", author);
    }

    @Test
    void testResolveCommitAuthorFallsBackToGitAuthorWhenNoAssociatedAccount() throws IOException {
        final GHCommit ghCommit = mock(GHCommit.class);
        when(ghCommit.getAuthor()).thenReturn(null);

        final GitUser gitAuthor = mock(GitUser.class);
        when(gitAuthor.getName()).thenReturn("Jane Developer");
        final GHCommit.ShortInfo shortInfo = mock(GHCommit.ShortInfo.class);
        when(shortInfo.getAuthor()).thenReturn(gitAuthor);

        final String author = GitHubRepositoryClient.resolveCommitAuthor(ghCommit, shortInfo);
        assertEquals("Jane Developer", author);
    }

    @Test
    void testResolveCommitAuthorReturnsPlaceholderWhenNoAuthorAvailable() throws IOException {
        final GHCommit ghCommit = mock(GHCommit.class);
        when(ghCommit.getAuthor()).thenThrow(new GHFileNotFoundException("/users/Copilot 404 Not Found"));

        final GHCommit.ShortInfo shortInfo = mock(GHCommit.ShortInfo.class);
        when(shortInfo.getAuthor()).thenReturn(null);

        final String author = GitHubRepositoryClient.resolveCommitAuthor(ghCommit, shortInfo);
        assertEquals("unknown", author);
    }
}
