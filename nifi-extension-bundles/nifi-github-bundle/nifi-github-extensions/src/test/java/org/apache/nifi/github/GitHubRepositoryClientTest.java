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
import java.util.Map;
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
    private static final String BRANCH = "main";
    private static final String BRANCH_REF = "refs/heads/main";
    private static final String COMMIT_SHA = "0123456789abcdef";
    private static final GitHubRepositoryClient.CommitFilesReader NO_COMMIT_FILES = sha -> {
        throw new AssertionError("Commit [" + sha + "] must not be read");
    };

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

        assertEquals(Optional.of(COMMIT_SHA), GitHubRepositoryClient.getLatestCommitSha(repository, PATH, BRANCH));

        final GHCommitQueryBuilder queryBuilder = repository.queryCommits();
        verify(queryBuilder).path(PATH);
        verify(queryBuilder).from(BRANCH_REF);
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

    @Test
    void testGetChangedPathsRelativeToRepositoryPath() throws IOException {
        final GHCompare compare = mockCompare(GHCompare.Status.ahead, 1,
                mockFile("flows/bucket/changed.json", null),
                mockFile("flows/bucket/renamed.json", "flows/bucket/original.json"),
                mockFile("README.md", null));

        assertEquals(Optional.of(Set.of("bucket/changed.json", "bucket/renamed.json", "bucket/original.json")),
                GitHubRepositoryClient.getChangedPaths(compare, NO_COMMIT_FILES, "flows"));
    }

    @Test
    void testGetChangedPathsWithoutRepositoryPath() throws IOException {
        final GHCompare compare = mockCompare(GHCompare.Status.identical, 0);

        assertEquals(Optional.of(Set.of()), GitHubRepositoryClient.getChangedPaths(compare, NO_COMMIT_FILES, null));
    }

    @Test
    void testGetChangedPathsIncludesFilesRevertedWithinRange() throws IOException {
        // The comparison lists no file: one commit changed the flow and the next one reverted it
        final GHCompare compare = mockCompare(GHCompare.Status.ahead, 2);
        final Map<String, List<GHCommit.File>> commitFiles = Map.of(
                "commit-0", List.of(mockFile("bucket/reverted.json", null)),
                "commit-1", List.of(mockFile("bucket/reverted.json", null), mockFile("bucket/other.json", null)));

        assertEquals(Optional.of(Set.of("bucket/reverted.json", "bucket/other.json")),
                GitHubRepositoryClient.getChangedPaths(compare, commitFiles::get, null));
    }

    @Test
    void testGetChangedPathsUnknownWhenHistoryDiverged() throws IOException {
        final GHCompare compare = mockCompare(GHCompare.Status.diverged, 1, mockFile("bucket/changed.json", null));

        assertEquals(Optional.empty(), GitHubRepositoryClient.getChangedPaths(compare, NO_COMMIT_FILES, null));
    }

    @Test
    void testGetChangedPathsUnknownWhenFileListMayBeTruncated() throws IOException {
        final GHCommit.File[] files = new GHCommit.File[GitHubRepositoryClient.COMPARE_FILES_LIMIT];
        for (int i = 0; i < files.length; i++) {
            files[i] = mockFile("bucket/flow-" + i + ".json", null);
        }

        assertEquals(Optional.empty(), GitHubRepositoryClient.getChangedPaths(mockCompare(GHCompare.Status.ahead, 1, files), NO_COMMIT_FILES, null));
    }

    @Test
    void testGetChangedPathsUnknownWhenRangeHasTooManyCommits() throws IOException {
        final GHCompare compare = mockCompare(GHCompare.Status.ahead, GitHubRepositoryClient.COMPARE_COMMITS_LIMIT + 1);

        assertEquals(Optional.empty(), GitHubRepositoryClient.getChangedPaths(compare, NO_COMMIT_FILES, null));
    }

    @Test
    void testGetChangedPathsUnknownWhenCommitListIsTruncated() throws IOException {
        final GHCompare compare = mockCompare(GHCompare.Status.ahead, 2);
        when(compare.getTotalCommits()).thenReturn(300);

        assertEquals(Optional.empty(), GitHubRepositoryClient.getChangedPaths(compare, NO_COMMIT_FILES, null));
    }

    private static GHCompare mockCompare(final GHCompare.Status status, final int commitCount, final GHCommit.File... files) {
        final GHCompare.Commit[] commits = new GHCompare.Commit[commitCount];
        for (int i = 0; i < commitCount; i++) {
            commits[i] = mock(GHCompare.Commit.class);
            when(commits[i].getSHA1()).thenReturn("commit-" + i);
        }
        final GHCompare compare = mock(GHCompare.class);
        when(compare.getStatus()).thenReturn(status);
        when(compare.getCommits()).thenReturn(commits);
        when(compare.getTotalCommits()).thenReturn(commitCount);
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
