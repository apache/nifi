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

import { NfEditor } from './nf-editor.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { PropertyItem } from '../../property-table.component';
import { MockComponent } from 'ng-mocks';
import { PropertyHint } from '@nifi/shared';

import 'codemirror/addon/hint/show-hint';

describe('NfEditor', () => {
    let component: NfEditor;
    let fixture: ComponentFixture<NfEditor>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [NfEditor, HttpClientTestingModule, MockComponent(PropertyHint)]
        });
        fixture = TestBed.createComponent(NfEditor);
        component = fixture.componentInstance;
    });

    it('should create', () => {
        fixture.detectChanges();
        expect(component).toBeTruthy();
    });

    it('verify value set', () => {
        const value = 'my-group-id';
        const item: PropertyItem = {
            property: 'group.id',
            value,
            descriptor: {
                name: 'group.id',
                displayName: 'Group ID',
                description:
                    "A Group ID is used to identify consumers that are within the same consumer group. Corresponds to Kafka's 'group.id' property.",
                required: true,
                sensitive: false,
                dynamic: false,
                supportsEl: true,
                expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                dependencies: []
            },
            id: 3,
            triggerEdit: false,
            deleted: false,
            added: false,
            dirty: false,
            savedValue: value,
            type: 'required'
        };

        component.item = item;
        fixture.detectChanges();

        expect(component.nfEditorForm.get('value')?.value).toEqual(value);
        expect(component.nfEditorForm.get('value')?.disabled).toBeFalsy();
        expect(component.nfEditorForm.get('setEmptyString')?.value).toBeFalsy();

        jest.spyOn(component.ok, 'next');
        component.okClicked();
        expect(component.ok.next).toHaveBeenCalledWith(value);
    });

    it('verify empty value set', () => {
        const value = '';
        const item: PropertyItem = {
            property: 'group.id',
            value,
            descriptor: {
                name: 'group.id',
                displayName: 'Group ID',
                description:
                    "A Group ID is used to identify consumers that are within the same consumer group. Corresponds to Kafka's 'group.id' property.",
                required: true,
                sensitive: false,
                dynamic: false,
                supportsEl: true,
                expressionLanguageScope: 'Environment variables defined at JVM level and system properties',
                dependencies: []
            },
            id: 3,
            triggerEdit: false,
            deleted: false,
            added: false,
            dirty: false,
            savedValue: value,
            type: 'required'
        };

        component.item = item;
        fixture.detectChanges();

        expect(component.nfEditorForm.get('value')?.value).toEqual(value);
        expect(component.nfEditorForm.get('value')?.disabled).toBeTruthy();
        expect(component.nfEditorForm.get('setEmptyString')?.value).toBeTruthy();

        jest.spyOn(component.ok, 'next');
        component.okClicked();
        expect(component.ok.next).toHaveBeenCalledWith(value);
    });
});
