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

import { ParameterTip } from './parameter-tip.component';
import { ParameterTipInput } from '../../../../types';

describe('ParameterTip', () => {
    let component: ParameterTip;
    let fixture: ComponentFixture<ParameterTip>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ParameterTip]
        });
        fixture = TestBed.createComponent(ParameterTip);
        component = fixture.componentInstance;
    });

    function setData(value: string | null | undefined, sensitive = false): void {
        const data: ParameterTipInput = {
            parameter: {
                name: 'PARAM_A',
                description: 'desc',
                sensitive,
                value: value as string | null
            }
        };
        component.data = data;
        fixture.detectChanges();
    }

    function getValueText(): string {
        const el: HTMLElement = fixture.nativeElement;
        const unset = el.querySelector('.unset');
        if (unset) {
            return unset.textContent?.trim() ?? '';
        }
        const fontMono = el.querySelector('.font-mono');
        return fontMono?.textContent?.trim() ?? '';
    }

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('renders "No value set" when the parameter value is null', () => {
        setData(null);
        expect(getValueText()).toBe('No value set');
    });

    it('renders "No value set" when the parameter value is undefined', () => {
        setData(undefined);
        expect(getValueText()).toBe('No value set');
    });

    it('renders "Empty string set" when the parameter value is an empty string', () => {
        setData('');
        expect(getValueText()).toBe('Empty string set');
    });

    it('renders the value when the parameter has a non-empty value', () => {
        setData('hello');
        expect(getValueText()).toBe('hello');
    });
});
