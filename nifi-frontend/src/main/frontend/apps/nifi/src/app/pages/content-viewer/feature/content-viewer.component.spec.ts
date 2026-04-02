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

import { ContentViewerComponent } from './content-viewer.component';
import { provideMockStore } from '@ngrx/store/testing';
import { contentViewersFeatureKey } from '../state';
import { viewerOptionsFeatureKey } from '../state/viewer-options';
import { initialState } from '../state/viewer-options/viewer-options.reducer';
import { MatSelectModule } from '@angular/material/select';
import { ReactiveFormsModule } from '@angular/forms';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MatIconModule } from '@angular/material/icon';
import { NifiTooltipDirective } from '@nifi/shared';
import { aboutFeatureKey } from '../../../state/about';
import { initialState as aboutInitialState } from '../../../state/about/about.reducer';
import { currentUserFeatureKey } from '../../../state/current-user';
import { initialState as currentUserInitialState } from '../../../state/current-user/current-user.reducer';
import { DEFAULT_ROUTER_FEATURENAME } from '@ngrx/router-store';

describe('ContentViewerComponent', () => {
    let component: ContentViewerComponent;
    let fixture: ComponentFixture<ContentViewerComponent>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            declarations: [ContentViewerComponent],
            imports: [MatSelectModule, ReactiveFormsModule, NoopAnimationsModule, MatIconModule, NifiTooltipDirective],
            providers: [
                provideMockStore({
                    initialState: {
                        [contentViewersFeatureKey]: {
                            [viewerOptionsFeatureKey]: initialState
                        },
                        [aboutFeatureKey]: aboutInitialState,
                        [currentUserFeatureKey]: currentUserInitialState,
                        [DEFAULT_ROUTER_FEATURENAME]: {
                            state: {
                                queryParams: {}
                            }
                        }
                    }
                })
            ]
        });
        fixture = TestBed.createComponent(ContentViewerComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    describe('resolveBaseMediaType', () => {
        it('should resolve structured syntax suffix to base media type', () => {
            expect(component['resolveBaseMediaType']('application/vnd.api+json')).toEqual('application/json');
            expect(component['resolveBaseMediaType']('application/fhir+xml')).toEqual('application/xml');
            expect(component['resolveBaseMediaType']('application/some-vendor+yaml')).toEqual('application/yaml');
            expect(component['resolveBaseMediaType']('application/vnd.example.api+json')).toEqual('application/json');
            expect(component['resolveBaseMediaType']('application/soap+xml')).toEqual('application/xml');
        });

        it('should return null for media types without a structured suffix', () => {
            expect(component['resolveBaseMediaType']('application/json')).toBeNull();
            expect(component['resolveBaseMediaType']('text/plain')).toBeNull();
            expect(component['resolveBaseMediaType']('application/xml')).toBeNull();
            expect(component['resolveBaseMediaType']('text/csv')).toBeNull();
            expect(component['resolveBaseMediaType']('image/png')).toBeNull();
        });

        it('should handle avro binary type which contains a plus in the subtype', () => {
            expect(component['resolveBaseMediaType']('application/avro+binary')).toEqual('application/binary');
        });
    });

    describe('isMediaTypeCompatible', () => {
        it('should match exact media types', () => {
            expect(component['isMediaTypeCompatible']('application/json', 'application/json')).toBe(true);
            expect(component['isMediaTypeCompatible']('text/xml', 'text/xml')).toBe(true);
            expect(component['isMediaTypeCompatible']('text/plain', 'text/plain')).toBe(true);
        });

        it('should match via startsWith for media types with parameters', () => {
            expect(component['isMediaTypeCompatible']('text/plain; charset=UTF-8', 'text/plain')).toBe(true);
            expect(component['isMediaTypeCompatible']('application/json; charset=UTF-8', 'application/json')).toBe(
                true
            );
        });

        it('should match structured suffix types to their base media type', () => {
            expect(component['isMediaTypeCompatible']('application/vnd.api+json', 'application/json')).toBe(true);
            expect(component['isMediaTypeCompatible']('application/fhir+xml', 'application/xml')).toBe(true);
            expect(component['isMediaTypeCompatible']('application/fhir+xml', 'text/xml')).toBe(false);
            expect(component['isMediaTypeCompatible']('application/some-vendor+yaml', 'application/yaml')).toBe(true);
            expect(component['isMediaTypeCompatible']('application/soap+xml', 'application/xml')).toBe(true);
        });

        it('should not match unrelated media types', () => {
            expect(component['isMediaTypeCompatible']('application/json', 'application/xml')).toBe(false);
            expect(component['isMediaTypeCompatible']('text/plain', 'application/json')).toBe(false);
            expect(component['isMediaTypeCompatible']('image/png', 'application/json')).toBe(false);
        });

        it('should not match when suffix resolves to a different base type', () => {
            expect(component['isMediaTypeCompatible']('application/vnd.api+json', 'application/xml')).toBe(false);
            expect(component['isMediaTypeCompatible']('application/fhir+xml', 'application/json')).toBe(false);
        });
    });
});
