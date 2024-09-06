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
import { HttpClient, HttpParams } from '@angular/common/http';
import { NiFiCommon } from '@nifi/shared';
import {
    DownloadFlowFileContentRequest,
    FlowFileSummary,
    ListingRequest,
    SubmitQueueListingRequest,
    ViewFlowFileContentRequest
} from '../state/queue-listing';
import { Client } from '../../../service/client.service';

@Injectable({ providedIn: 'root' })
export class QueueService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private nifiCommon: NiFiCommon,
        private client: Client
    ) {}

    getConnection(connectionId: string): Observable<any> {
        return this.httpClient.get(`${QueueService.API}/connections/${connectionId}`);
    }

    getFlowFile(flowfileSummary: FlowFileSummary): Observable<any> {
        let params = new HttpParams();
        if (flowfileSummary.clusterNodeId) {
            params = params.set('clusterNodeId', flowfileSummary.clusterNodeId);
        }

        return this.httpClient.get(this.nifiCommon.stripProtocol(flowfileSummary.uri), { params });
    }

    submitQueueListingRequest(queueListingRequest: SubmitQueueListingRequest): Observable<any> {
        return this.httpClient.post(
            `${QueueService.API}/flowfile-queues/${queueListingRequest.connectionId}/listing-requests`,
            {}
        );
    }

    pollQueueListingRequest(listingRequest: ListingRequest): Observable<any> {
        return this.httpClient.get(this.nifiCommon.stripProtocol(listingRequest.uri));
    }

    deleteQueueListingRequest(listingRequest: ListingRequest): Observable<any> {
        return this.httpClient.delete(this.nifiCommon.stripProtocol(listingRequest.uri));
    }

    downloadContent(request: DownloadFlowFileContentRequest): void {
        let dataUri = `${request.uri}/content`;

        const queryParameters: any = {};

        if (request.clusterNodeId) {
            queryParameters['clusterNodeId'] = request.clusterNodeId;
        }

        if (Object.keys(queryParameters).length > 0) {
            const query: string = new URLSearchParams(queryParameters).toString();
            dataUri = `${dataUri}?${query}`;
        }

        window.open(dataUri);
    }

    viewContent(request: ViewFlowFileContentRequest, contentViewerUrl: string): void {
        // build the uri to the data
        let dataUri = `${request.uri}/content`;

        const dataUriParameters: any = {};

        if (request.clusterNodeId) {
            dataUriParameters['clusterNodeId'] = request.clusterNodeId;
        }

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
            contentViewer += '/?';
        } else {
            contentViewer += '&';
        }

        const contentViewerParameters: any = {
            ref: dataUri,
            clientId: this.client.getClientId()
        };

        if (request.mimeType) {
            contentViewerParameters['mimeType'] = request.mimeType;
        }

        // open the content viewer
        const contentViewerQuery: string = new URLSearchParams(contentViewerParameters).toString();
        window.open(`${contentViewer}${contentViewerQuery}`);
    }
}
