/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { ComponentFixture, TestBed } from '@angular/core/testing';

import { FlowConfigurationHistory } from './flow-configuration-history.component';
import { RouterModule } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { FlowConfigurationHistoryListing } from '../ui/flow-configuration-history-listing/flow-configuration-history-listing.component';
import { MockComponent } from 'ng-mocks';
import { Navigation } from '../../../ui/common/navigation/navigation.component';
import { BannerText } from '../../../ui/common/banner-text/banner-text.component';

describe('FlowConfigurationHistory', () => {
    let component: FlowConfigurationHistory;
    let fixture: ComponentFixture<FlowConfigurationHistory>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [FlowConfigurationHistory],
            imports: [
                RouterModule,
                RouterTestingModule,
                MockComponent(BannerText),
                MockComponent(Navigation),
                MockComponent(FlowConfigurationHistoryListing)
            ]
        });
        fixture = TestBed.createComponent(FlowConfigurationHistory);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
