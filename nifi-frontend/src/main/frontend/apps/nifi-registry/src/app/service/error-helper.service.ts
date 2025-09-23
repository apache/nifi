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
import { HttpErrorResponse } from '@angular/common/http';

@Injectable({ providedIn: 'root' })
export class ErrorHelper {
    getErrorString(errorResponse: HttpErrorResponse, prefix?: string): string {
        // TODO: Update to handle actual error response structure from NiFi Registry REST API
        let errorMessage = 'An unspecified error occurred.';
        if (errorResponse) {
            const err = errorResponse.error;
            if (err) {
                if (typeof err === 'string') {
                    errorMessage = err;
                } else if (typeof err === 'object') {
                    // Prefer nested message fields from our mock errors
                    errorMessage = err.message || JSON.stringify(err);
                } else {
                    errorMessage = `${err}`;
                }
            } else if (errorResponse.message) {
                errorMessage = errorResponse.message;
            } else if (errorResponse.status) {
                errorMessage = `${errorResponse.status} ${errorResponse.statusText || ''}`.trim();
            }
        }
        return prefix ? `${prefix} - [${errorMessage}]` : errorMessage;
    }
}
