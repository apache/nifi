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

import { Injectable, inject } from '@angular/core';
import { Observable } from 'rxjs';
import { HttpClient, HttpHeaders } from '@angular/common/http';

@Injectable({ providedIn: 'root' })
export class DropletsService {
    private httpClient = inject(HttpClient);

    private static readonly API: string = '../nifi-registry-api';

    getDroplets(bucketId?: string): Observable<any> {
        // const mockError: HttpErrorResponse = new HttpErrorResponse({
        //     status: 404,
        //     statusText: 'Service Unavailable',
        //     url: bucketId
        //         ? `${DropletsService.API}/items/${bucketId}`
        //         : `${DropletsService.API}/items`,
        //     error: {
        //         message: 'Mock error: unable to GET droplets.',
        //         timestamp: new Date().toISOString()
        //     }
        // });
        //
        // return throwError(() => mockError);

        if (bucketId) {
            return this.httpClient.get(`${DropletsService.API}/items/${bucketId}`);
        }
        return this.httpClient.get(`${DropletsService.API}/items`);
    }

    deleteDroplet(href: string): Observable<any> {
        // const mockError: HttpErrorResponse = new HttpErrorResponse({
        //     status: 403,
        //     statusText: 'Forbidden',
        //     url: `${DropletsService.API}/${href}?version=0`,
        //     error: {
        //         message: 'Mock error: unable to delete droplet.',
        //         timestamp: new Date().toISOString()
        //     }
        // });
        // return throwError(() => mockError);

        return this.httpClient.delete(`${DropletsService.API}/${href}?version=0`);
    }

    createNewDroplet(bucketUri: string, name: string, description: string): Observable<any> {
        // const mockError: HttpErrorResponse = new HttpErrorResponse({
        //     status: 400,
        //     statusText: 'Bad Request',
        //     url: `${DropletsService.API}/${bucketUri}/flows`,
        //     error: {
        //         message: 'Mock error: unable to import resource.',
        //         details: { name, description },
        //         timestamp: new Date().toISOString()
        //     }
        // });
        // return throwError(() => mockError);

        return this.httpClient.post(`${DropletsService.API}/${bucketUri}/flows`, {
            name: name,
            description: description
        });
    }

    uploadDroplet(flowUri: string, file: File, description: string): Observable<any> {
        // const mockError: HttpErrorResponse = new HttpErrorResponse({
        //     status: 413,
        //     statusText: 'Payload Too Large',
        //     url: `${DropletsService.API}/${flowUri}/versions/import`,
        //     error: {
        //         message: 'Mock error: uploaded file exceeds maximum size.',
        //         fileName: file?.name,
        //         timestamp: new Date().toISOString()
        //     }
        // });
        // return throwError(() => mockError);

        return this.httpClient.post(`${DropletsService.API}/${flowUri}/versions/import`, file, {
            headers: { Comments: description || '' } // using a short-circuit here because the backend does not accept null
        });
    }

    exportDropletVersionedSnapshot(dropletUri: string, versionNumber: number): Observable<any> {
        // const mockError: HttpErrorResponse = new HttpErrorResponse({
        //     status: 404,
        //     statusText: 'Not Found',
        //     url: `${DropletsService.API}/${dropletUri}/versions/${versionNumber}/export`,
        //     error: {
        //         message: 'Mock error: error exporting resource.',
        //         version: versionNumber,
        //         timestamp: new Date().toISOString()
        //     }
        // });
        // return throwError(() => mockError);

        const headers = new HttpHeaders({ 'Content-Type': 'application/json' });
        return this.httpClient.get(`${DropletsService.API}/${dropletUri}/versions/${versionNumber}/export`, {
            headers,
            observe: 'response',
            responseType: 'text'
        });
    }

    getDropletSnapshotMetadata(dropletUri: string): Observable<any> {
        // const mockError: HttpErrorResponse = new HttpErrorResponse({
        //     status: 500,
        //     statusText: 'Internal Server Error',
        //     url: `${DropletsService.API}/${dropletUri}/versions`,
        //     error: {
        //         message: 'Mock error: failed to retrieve snapshot metadata.',
        //         timestamp: new Date().toISOString()
        //     }
        // });
        // return throwError(() => mockError);

        return this.httpClient.get(`${DropletsService.API}/${dropletUri}/versions`);
    }
}
