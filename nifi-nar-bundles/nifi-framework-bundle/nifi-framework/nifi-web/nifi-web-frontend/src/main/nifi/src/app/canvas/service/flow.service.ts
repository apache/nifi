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
import { UpdateComponentPosition } from '../state';
import { HttpClient } from '@angular/common/http';
import { CanvasUtils } from './canvas-utils.service';

@Injectable({ providedIn: 'root' })
export class FlowService {
    private static readonly API: string = '../nifi-api';

    constructor(
        private httpClient: HttpClient,
        private canvasUtils: CanvasUtils
    ) {}

    /**
     * The NiFi model contain the url for each component. That URL is an absolute URL. Angular CSRF handling
     * does not work on absolute URLs, so we need to strip off the proto for the request header to be added.
     *
     * https://stackoverflow.com/a/59586462
     *
     * @param url
     * @private
     */
    private stripProtocol(url: string): string {
        return this.canvasUtils.substringAfterFirst(url, ':');
    }

    getFlow(processGroupId: string = 'root'): Observable<any> {
        return this.httpClient.get(FlowService.API + '/flow/process-groups/' + processGroupId);
    }

    updateComponentPosition(updateComponentPosition: UpdateComponentPosition): Observable<any> {
        return this.httpClient.put(this.stripProtocol(updateComponentPosition.uri), {
            revision: updateComponentPosition.revision,
            component: {
                id: updateComponentPosition.id,
                position: updateComponentPosition.position
            }
        });
    }
}
