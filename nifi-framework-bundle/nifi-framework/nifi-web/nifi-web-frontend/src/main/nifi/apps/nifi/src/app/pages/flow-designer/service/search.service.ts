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

import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpClient } from '@angular/common/http';

export interface SearchResultGroup {
    id: string;
    name: string;
}

export interface ComponentSearchResult {
    id: string;
    groupId: string;
    parentGroup: SearchResultGroup;
    versionedGroup: SearchResultGroup;
    name: string;
    matches: string[];
}

export interface SearchResults {
    processorResults: ComponentSearchResult[];
    connectionResults: ComponentSearchResult[];
    processGroupResults: ComponentSearchResult[];
    inputPortResults: ComponentSearchResult[];
    outputPortResults: ComponentSearchResult[];
    remoteProcessGroupResults: ComponentSearchResult[];
    funnelResults: ComponentSearchResult[];
    labelResults: ComponentSearchResult[];
    controllerServiceNodeResults: ComponentSearchResult[];
    parameterContextResults: ComponentSearchResult[];
    parameterProviderNodeResults: ComponentSearchResult[];
    parameterResults: ComponentSearchResult[];
}

export interface SearchResultsEntity {
    searchResultsDTO: SearchResults;
}

@Injectable({ providedIn: 'root' })
export class SearchService {
    private static readonly API: string = '../nifi-api';

    constructor(private httpClient: HttpClient) {}

    search(query: string, processGroupId: string): Observable<SearchResultsEntity> {
        return this.httpClient.get<SearchResultsEntity>(`${SearchService.API}/flow/search-results`, {
            params: {
                q: query,
                a: processGroupId
            }
        });
    }
}
