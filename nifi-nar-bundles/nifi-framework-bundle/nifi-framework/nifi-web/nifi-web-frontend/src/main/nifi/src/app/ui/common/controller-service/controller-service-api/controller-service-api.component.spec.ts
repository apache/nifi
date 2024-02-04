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

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ControllerServiceApi } from './controller-service-api.component';
import { Bundle } from '../../../../state/shared';

describe('ControllerServiceApi', () => {
    let component: ControllerServiceApi;
    let fixture: ComponentFixture<ControllerServiceApi>;

    const serviceType = 'org.apache.nifi.ssl.StandardRestrictedSSLContextService';
    const serviceBundle: Bundle = {
        group: 'org.apache.nifi',
        artifact: 'nifi-ssl-context-service-nar',
        version: '2.0.0-SNAPSHOT'
    };

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ControllerServiceApi]
        });
        fixture = TestBed.createComponent(ControllerServiceApi);
        component = fixture.componentInstance;
        component.type = serviceType;
        component.bundle = serviceBundle;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('should format api', () => {
        expect(component.formatControllerService(serviceType, serviceBundle)).toEqual(
            'StandardRestrictedSSLContextService 2.0.0-SNAPSHOT from org.apache.nifi - nifi-ssl-context-service-nar'
        );
    });
});
