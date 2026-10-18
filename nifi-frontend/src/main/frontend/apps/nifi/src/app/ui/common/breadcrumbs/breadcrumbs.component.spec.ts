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

import { Breadcrumbs } from './breadcrumbs.component';
import { RouterModule } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { BreadcrumbEntity } from '../../../state/shared';
import { Title } from '@angular/platform-browser';

describe('Breadcrumbs', () => {
    let component: Breadcrumbs;
    let fixture: ComponentFixture<Breadcrumbs>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [Breadcrumbs, RouterModule, RouterTestingModule]
        });
        fixture = TestBed.createComponent(Breadcrumbs);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    const root: BreadcrumbEntity = {
        id: 'root-id',
        permissions: { canRead: true, canWrite: true },
        versionedFlowState: '',
        breadcrumb: { id: 'root-id', name: 'NiFi Flow' }
    };

    it('preserves the root name and route without overwriting the page title', () => {
        TestBed.inject(Title).setTitle('Processors - Apache NiFi');
        fixture.componentRef.setInput('entity', root);
        fixture.componentRef.setInput('currentProcessGroupId', root.id);
        fixture.detectChanges();
        expect(fixture.nativeElement.querySelector('a').textContent.trim()).toBe('NiFi Flow');
        expect(TestBed.inject(Title).getTitle()).toBe('Processors - Apache NiFi');
        expect(component.getRoute(root)).toEqual(['/process-groups', 'root-id']);
        expect(root.breadcrumb.name).toBe('NiFi Flow');
    });

    it('preserves custom root names, child names and restricted breadcrumb identities', () => {
        expect(
            component.getBreadcrumbLabel({ ...root, breadcrumb: { ...root.breadcrumb, name: 'Company jobs' } })
        ).toBe('Company jobs');
        expect(component.getBreadcrumbLabel({ ...root, parentBreadcrumb: root })).toBe('NiFi Flow');
        expect(component.getBreadcrumbLabel({ ...root, permissions: { canRead: false, canWrite: false } })).toBe(
            'root-id'
        );
    });
});
