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

import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { MatDialog } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';
import { UnsafeApiPathError } from '@nifi/shared';
import { firstValueFrom } from 'rxjs';
import { Client } from './client.service';
import { ErrorHelper } from './error-helper.service';
import { ExtensionTypesService } from './extension-types.service';
import { PropertyTableHelperService } from './property-table-helper.service';

describe('PropertyTableHelperService', () => {
    it.each(['', '..', 'component/id', 'component%2f..%2fsecret'])(
        'should reject unsafe component history id %s without issuing a request',
        async (componentId) => {
            TestBed.configureTestingModule({
                providers: [
                    PropertyTableHelperService,
                    provideHttpClient(),
                    provideHttpClientTesting(),
                    provideMockStore({ initialState: {} }),
                    { provide: MatDialog, useValue: {} },
                    { provide: ExtensionTypesService, useValue: {} },
                    { provide: Client, useValue: {} },
                    { provide: ErrorHelper, useValue: {} }
                ]
            });
            const service = TestBed.inject(PropertyTableHelperService);
            const http = TestBed.inject(HttpTestingController);

            await expect(firstValueFrom(service.getComponentHistory(componentId))).rejects.toBeInstanceOf(
                UnsafeApiPathError
            );
            http.expectNone(() => true);
        }
    );
});
