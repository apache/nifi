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

import { ExtensionCreation } from './extension-creation.component';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MatDialogRef } from '@angular/material/dialog';
import { By } from '@angular/platform-browser';

describe('ExtensionCreation', () => {
    let component: ExtensionCreation;
    let fixture: ComponentFixture<ExtensionCreation>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ExtensionCreation, NoopAnimationsModule],
            providers: [{ provide: MatDialogRef, useValue: null }]
        });
        fixture = TestBed.createComponent(ExtensionCreation);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('extensionTypesLoadingStatus', () => {
        it('should show error', () => {
            component.extensionTypesLoadingStatus = 'error';
            fixture.detectChanges();

            const errorLoadingTypes = fixture.debugElement.query(By.css('div[data-qa="error-loading-types"]'));
            const extensionTypesListing = fixture.debugElement.query(By.css('div[data-qa="extension-types-listing"]'));
            const extensionTypesSkeleton = fixture.debugElement.query(
                By.css('ngx-skeleton-loader[data-qa="extension-types-skeleton"]')
            );

            expect(errorLoadingTypes).toBeTruthy();
            expect(extensionTypesListing).toBeFalsy();
            expect(extensionTypesSkeleton).toBeFalsy();
        });

        it('should show skeleton', () => {
            component.extensionTypesLoadingStatus = 'loading';
            fixture.detectChanges();

            const errorLoadingTypes = fixture.debugElement.query(By.css('div[data-qa="error-loading-types"]'));
            const extensionTypesListing = fixture.debugElement.query(By.css('div[data-qa="extension-types-listing"]'));
            const extensionTypesSkeleton = fixture.debugElement.query(
                By.css('ngx-skeleton-loader[data-qa="extension-types-skeleton"]')
            );

            expect(errorLoadingTypes).toBeFalsy();
            expect(extensionTypesListing).toBeFalsy();
            expect(extensionTypesSkeleton).toBeTruthy();
        });

        it('should show listing', () => {
            component.extensionTypesLoadingStatus = 'success';
            fixture.detectChanges();

            const errorLoadingTypes = fixture.debugElement.query(By.css('div[data-qa="error-loading-types"]'));
            const extensionTypesListing = fixture.debugElement.query(By.css('div[data-qa="extension-types-listing"]'));
            const extensionTypesSkeleton = fixture.debugElement.query(
                By.css('ngx-skeleton-loader[data-qa="extension-types-skeleton"]')
            );

            expect(errorLoadingTypes).toBeFalsy();
            expect(extensionTypesListing).toBeTruthy();
            expect(extensionTypesSkeleton).toBeFalsy();
        });
    });
});
