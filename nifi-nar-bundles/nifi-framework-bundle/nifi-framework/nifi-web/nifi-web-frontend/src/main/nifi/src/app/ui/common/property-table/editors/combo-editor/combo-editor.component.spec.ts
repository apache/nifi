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

import { ComboEditor } from './combo-editor.component';

describe('ComboEditor', () => {
    let component: ComboEditor;
    let fixture: ComponentFixture<ComboEditor>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ComboEditor]
        });
        fixture = TestBed.createComponent(ComboEditor);
        component = fixture.componentInstance;
        component.supportsParameters = false;
        component.item = {
            property: 'Destination',
            value: 'flowfile-attribute',
            descriptor: {
                name: 'Destination',
                displayName: 'Destination',
                description:
                    "Control if JSON value is written as a new flowfile attribute 'JSONAttributes' or written in the flowfile content. Writing to flowfile content will overwrite any existing flowfile content.",
                defaultValue: 'flowfile-attribute',
                allowableValues: [
                    {
                        allowableValue: {
                            displayName: 'flowfile-attribute',
                            value: 'flowfile-attribute'
                        },
                        canRead: true
                    },
                    {
                        allowableValue: {
                            displayName: 'flowfile-content',
                            value: 'flowfile-content'
                        },
                        canRead: true
                    }
                ],
                required: true,
                sensitive: false,
                dynamic: false,
                supportsEl: false,
                expressionLanguageScope: 'Not Supported',
                dependencies: []
            },
            id: 2,
            triggerEdit: false,
            deleted: false,
            added: false,
            dirty: false,
            type: 'required'
        };
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
