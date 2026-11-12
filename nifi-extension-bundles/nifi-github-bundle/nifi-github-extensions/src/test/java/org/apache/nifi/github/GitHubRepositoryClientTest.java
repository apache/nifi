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
import org.kohsuke.github.GHCommitQueryBuilder;
import org.kohsuke.github.GHException;
import org.kohsuke.github.GHFileNotFoundException;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHUser;
import org.kohsuke.github.GitUser;
import org.kohsuke.github.PagedIterable;
import org.kohsuke.github.PagedIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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

    private static final String PATH = "bucket/flow.json";
    private static final String BRANCH = "main";
    private static final String COMMIT_SHA = "0123456789abcdef";

    @Test
    void testGetLatestCommitShaQueriesOneCommitFromBranchWithoutAuthorLookup() throws IOException {
        final GHCommit ghCommit = mock(GHCommit.class);
        when(ghCommit.getSHA1()).thenReturn(COMMIT_SHA);
        final PagedIterator<GHCommit> iterator = mockIterator();
        when(iterator.hasNext()).thenReturn(true);
        when(iterator.next()).thenReturn(ghCommit);
        final GHRepository repository = mockRepository(iterator);

        assertEquals(Optional.of(COMMIT_SHA), GitHubRepositoryClient.getLatestCommitSha(repository, PATH, BRANCH));

        final GHCommitQueryBuilder queryBuilder = repository.queryCommits();
        verify(queryBuilder).path(PATH);
        verify(queryBuilder).from(BRANCH);
        verify(queryBuilder).pageSize(1);
        verify(ghCommit).getSHA1();
        verify(repository, never()).getRef(anyString());
    }

    @Test
    void testGetLatestCommitShaEmptyWhenPathHasNoCommits() throws IOException {
        final PagedIterator<GHCommit> iterator = mockIterator();
        when(iterator.hasNext()).thenReturn(false);

        assertEquals(Optional.empty(), GitHubRepositoryClient.getLatestCommitSha(mockRepository(iterator), PATH, BRANCH));
    }

    @Test
    void testGetLatestCommitShaUnwrapsNotFound() {
        final PagedIterator<GHCommit> iterator = mockIterator();
        final FileNotFoundException notFound = new FileNotFoundException("Not Found");
        when(iterator.hasNext()).thenThrow(new GHException("Failed to retrieve", notFound));

        final FileNotFoundException thrown = assertThrows(FileNotFoundException.class,
                () -> GitHubRepositoryClient.getLatestCommitSha(mockRepository(iterator), PATH, BRANCH));
        assertEquals(notFound, thrown);
    }

    @SuppressWarnings("unchecked")
    private static PagedIterator<GHCommit> mockIterator() {
        return mock(PagedIterator.class);
    }

    @SuppressWarnings("unchecked")
    private static GHRepository mockRepository(final PagedIterator<GHCommit> iterator) {
        final PagedIterable<GHCommit> iterable = mock(PagedIterable.class);
        when(iterable.iterator()).thenReturn(iterator);
        final GHCommitQueryBuilder queryBuilder = mock(GHCommitQueryBuilder.class, RETURNS_SELF);
        when(queryBuilder.list()).thenReturn(iterable);
        final GHRepository repository = mock(GHRepository.class);
        when(repository.queryCommits()).thenReturn(queryBuilder);
        return repository;
    }
}
