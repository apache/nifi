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
import { NiFiCommon } from '../../../service/nifi-common.service';
import { ProvenanceRequest } from '../state/provenance-event-listing';
import { LineageRequest } from '../state/lineage';

@Injectable({ providedIn: 'root' })
export class ProvenanceService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private nifiCommon: NiFiCommon
    ) {}

    getSearchOptions(): Observable<any> {
        return this.httpClient.get(`${ProvenanceService.API}/provenance/search-options`);
    }

    submitProvenanceQuery(request: ProvenanceRequest): Observable<any> {
        return this.httpClient.post(`${ProvenanceService.API}/provenance`, { provenance: { request } });
    }

    getProvenanceQuery(id: string, clusterNodeId?: string): Observable<any> {
        // TODO - cluster node id
        return this.httpClient.get(`${ProvenanceService.API}/provenance/${encodeURIComponent(id)}`);
    }

    deleteProvenanceQuery(id: string, clusterNodeId?: string): Observable<any> {
        // TODO - cluster node id
        return this.httpClient.delete(`${ProvenanceService.API}/provenance/${encodeURIComponent(id)}`);
    }

    getProvenanceEvent(id: string): Observable<any> {
        // TODO - cluster node id
        return this.httpClient.get(`${ProvenanceService.API}/provenance-events/${encodeURIComponent(id)}`);
    }

    downloadContent(id: string, direction: string): void {
        let dataUri: string = `${ProvenanceService.API}/provenance-events/${encodeURIComponent(
            id
        )}/content/${encodeURIComponent(direction)}`;

        const queryParameters: any = {};

        // TODO - cluster node id in query parameters

        if (Object.keys(queryParameters).length > 0) {
            const query: string = new URLSearchParams(queryParameters).toString();
            dataUri = `${dataUri}?${query}`;
        }

        window.open(dataUri);
    }

    viewContent(nifiUrl: string, contentViewerUrl: string, id: string, direction: string): void {
        // build the uri to the data
        let dataUri: string = `${nifiUrl}provenance-events/${encodeURIComponent(id)}/content/${encodeURIComponent(
            direction
        )}`;

        const dataUriParameters: any = {};

        // TODO - cluster node id in data uri parameters

        // include parameters if necessary
        if (Object.keys(dataUriParameters).length > 0) {
            const dataUriQuery: string = new URLSearchParams(dataUriParameters).toString();
            dataUri = `${dataUri}?${dataUriQuery}`;
        }

        // if there's already a query string don't add another ?... this assumes valid
        // input meaning that if the url has already included a ? it also contains at
        // least one query parameter
        let contentViewer: string = contentViewerUrl;
        if (contentViewer.indexOf('?') === -1) {
            contentViewer += '?';
        } else {
            contentViewer += '&';
        }

        const contentViewerParameters: any = {
            ref: dataUri
        };

        // open the content viewer
        const contentViewerQuery: string = new URLSearchParams(contentViewerParameters).toString();
        window.open(`${contentViewer}${contentViewerQuery}`);
    }

    replay(eventId: string): Observable<any> {
        const payload: any = {
            eventId
        };

        // TODO - add cluster node id in payload

        return this.httpClient.post(`${ProvenanceService.API}/provenance-events/replays`, payload);
    }

    submitLineageQuery(request: LineageRequest): Observable<any> {
        return this.httpClient.post(`${ProvenanceService.API}/provenance/lineage`, { lineage: { request } });
    }

    getLineageQuery(id: string, clusterNodeId?: string): Observable<any> {
        // TODO - cluster node id
        return this.httpClient.get(`${ProvenanceService.API}/provenance/lineage/${encodeURIComponent(id)}`);
    }

    deleteLineageQuery(id: string, clusterNodeId?: string): Observable<any> {
        // TODO - cluster node id
        return this.httpClient.delete(`${ProvenanceService.API}/provenance/lineage/${encodeURIComponent(id)}`);
    }
}
