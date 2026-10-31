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

import { TestBed } from '@angular/core/testing';

import { StorageService } from './storage.service';

describe('StorageService', () => {
    let service: StorageService;

    beforeEach(() => {
        sessionStorage.clear();
        TestBed.configureTestingModule({});
        service = TestBed.inject(StorageService);
    });

    afterEach(() => {
        sessionStorage.clear();
    });

    it('should be created', () => {
        expect(service).toBeTruthy();
    });

    it('should return null when no return url is stored', () => {
        expect(service.getReturnUrl()).toBeNull();
    });

    it('should store and return the return url', () => {
        service.setReturnUrl('/process-groups/abc');
        expect(service.getReturnUrl()).toBe('/process-groups/abc');
    });

    it('should remove the return url', () => {
        service.setReturnUrl('/process-groups/abc');
        service.removeReturnUrl();
        expect(service.getReturnUrl()).toBeNull();
    });
});
