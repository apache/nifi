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
import { Observable, throwError } from 'rxjs';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { UploadRequest } from '../types';

interface Iso8859ValidationResponse {
    valid: boolean;
    outOfRangeCharIndex?: number;
}

@Injectable({ providedIn: 'root' })
export class UploadService {
    private httpClient = inject(HttpClient);

    upload(request: UploadRequest): Observable<unknown> {
        const isoValidation = this.isIso8859(request.file.name);
        if (!isoValidation.valid) {
            return throwError(
                () =>
                    new Error(
                        `Filenames must only contain characters in the ISO-8859-1 character set. The character at index ${isoValidation.outOfRangeCharIndex} is outside of the allowed range.`
                    )
            );
        }

        const headers = new HttpHeaders({
            Filename: request.file.name,
            'Content-Type': 'application/octet-stream',
            'ngsw-bypass': ''
        });
        return this.httpClient.post(request.url, request.file, { headers, observe: 'events', reportProgress: true });
    }

    private isIso8859(text: string): Iso8859ValidationResponse {
        for (let i = 0; i < text.length; i++) {
            if (text.charCodeAt(i) > 255) {
                return {
                    valid: false,
                    outOfRangeCharIndex: i
                };
            }
        }
        return {
            valid: true
        };
    }
}
