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
import org.kohsuke.github.GHCompare;
import org.kohsuke.github.GHException;
import org.kohsuke.github.GHFileNotFoundException;
import org.kohsuke.github.GHRepository;
import org.kohsuke.github.GHUser;
import org.kohsuke.github.GitUser;
import org.kohsuke.github.PagedIterable;
import org.kohsuke.github.PagedIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GitHubRepositoryClientTest {

    private static final String PATH = "bucket/flow.json";
    private static final String BRANCH_REF = "refs/heads/main";
    private static final String COMMIT_SHA = "0123456789abcdef";

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

    @Test
    void testGetLatestCommitShaQueriesOneCommitFromBranchWithoutAuthorLookup() throws IOException {
        final GHCommit ghCommit = mock(GHCommit.class);
        when(ghCommit.getSHA1()).thenReturn(COMMIT_SHA);
        final PagedIterator<GHCommit> iterator = mockIterator();
        when(iterator.hasNext()).thenReturn(true);
        when(iterator.next()).thenReturn(ghCommit);
        final GHRepository repository = mockRepository(iterator);

        assertEquals(Optional.of(COMMIT_SHA), GitHubRepositoryClient.getLatestCommitSha(repository, PATH, BRANCH_REF));

        final GHCommitQueryBuilder queryBuilder = repository.queryCommits();
        verify(queryBuilder).path(PATH);
        verify(queryBuilder).from(BRANCH_REF);
        verify(queryBuilder).pageSize(1);
        verify(ghCommit).getSHA1();
        verify(repository, never()).getRef(anyString());
    }

    @Test
    void testGetLatestCommitShaQueriesFromCommit() throws IOException {
        final PagedIterator<GHCommit> iterator = mockIterator();
        when(iterator.hasNext()).thenReturn(false);
        final GHRepository repository = mockRepository(iterator);

        assertEquals(Optional.empty(), GitHubRepositoryClient.getLatestCommitSha(repository, PATH, COMMIT_SHA));

        verify(repository.queryCommits()).from(COMMIT_SHA);
    }

    @Test
    void testGetLatestCommitShaEmptyWhenPathHasNoCommits() throws IOException {
        final PagedIterator<GHCommit> iterator = mockIterator();
        when(iterator.hasNext()).thenReturn(false);

        assertEquals(Optional.empty(), GitHubRepositoryClient.getLatestCommitSha(mockRepository(iterator), PATH, BRANCH_REF));
    }

    @Test
    void testGetLatestCommitShaUnwrapsNotFound() {
        final PagedIterator<GHCommit> iterator = mockIterator();
        final FileNotFoundException notFound = new FileNotFoundException("Not Found");
        when(iterator.hasNext()).thenThrow(new GHException("Failed to retrieve", notFound));

        final FileNotFoundException thrown = assertThrows(FileNotFoundException.class,
                () -> GitHubRepositoryClient.getLatestCommitSha(mockRepository(iterator), PATH, BRANCH_REF));
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

    @Test
    void testGetChangedPathsRelativeToRepositoryPath() {
        final GHCompare compare = mockCompare(GHCompare.Status.ahead, 1,
                mockFile("flows/bucket/changed.json", null),
                mockFile("flows/bucket/renamed.json", "flows/bucket/original.json"),
                mockFile("README.md", null));

        assertEquals(Optional.of(Set.of("bucket/changed.json", "bucket/renamed.json", "bucket/original.json")),
                GitHubRepositoryClient.getChangedPaths(compare, "flows"));
    }

    @Test
    void testGetChangedPathsWithoutRepositoryPath() {
        final GHCompare compare = mockCompare(GHCompare.Status.ahead, 1,
                mockFile("bucket/changed.json", null),
                mockFile("bucket/renamed.json", "bucket/original.json"));

        assertEquals(Optional.of(Set.of("bucket/changed.json", "bucket/renamed.json", "bucket/original.json")),
                GitHubRepositoryClient.getChangedPaths(compare, null));
    }

    @Test
    void testGetChangedPathsUnknownWhenMoreThanOneCommit() {
        // The compared files are the net difference: a file changed by one commit and reverted by the next is not listed
        final GHCompare compare = mockCompare(GHCompare.Status.ahead, 2, mockFile("bucket/other.json", null));

        assertEquals(Optional.empty(), GitHubRepositoryClient.getChangedPaths(compare, null));
        verify(compare, never()).getFiles();
    }

    @Test
    void testGetChangedPathsUnknownWhenNotAhead() {
        for (final GHCompare.Status status : List.of(GHCompare.Status.behind, GHCompare.Status.diverged, GHCompare.Status.identical)) {
            final GHCompare compare = mockCompare(status, 1, mockFile("bucket/changed.json", null));

            assertEquals(Optional.empty(), GitHubRepositoryClient.getChangedPaths(compare, null), status.name());
        }
    }

    @Test
    void testGetChangedPathsUnknownWhenFileListMayBeTruncated() {
        final GHCommit.File[] files = new GHCommit.File[GitHubRepositoryClient.COMPARE_FILES_LIMIT];
        for (int i = 0; i < files.length; i++) {
            files[i] = mockFile("bucket/flow-" + i + ".json", null);
        }

        assertEquals(Optional.empty(), GitHubRepositoryClient.getChangedPaths(mockCompare(GHCompare.Status.ahead, 1, files), null));
    }

    private static GHCompare mockCompare(final GHCompare.Status status, final int aheadBy, final GHCommit.File... files) {
        final GHCompare compare = mock(GHCompare.class);
        when(compare.getStatus()).thenReturn(status);
        when(compare.getAheadBy()).thenReturn(aheadBy);
        when(compare.getFiles()).thenReturn(files);
        return compare;
    }

    private static GHCommit.File mockFile(final String fileName, final String previousFileName) {
        final GHCommit.File file = mock(GHCommit.File.class);
        when(file.getFileName()).thenReturn(fileName);
        when(file.getPreviousFilename()).thenReturn(previousFileName);
        return file;
    }
}
