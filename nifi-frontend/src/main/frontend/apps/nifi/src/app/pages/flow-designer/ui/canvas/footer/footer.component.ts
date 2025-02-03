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

import { Component } from '@angular/core';
import { selectBreadcrumbs, selectCurrentProcessGroupId } from '../../../state/flow/flow.selectors';
import { Store } from '@ngrx/store';
import { CanvasState } from '../../../state';
import { Breadcrumbs } from '../../common/breadcrumbs/breadcrumbs.component';
import { AsyncPipe } from '@angular/common';

@Component({
    selector: 'fd-footer',
    templateUrl: './footer.component.html',
    imports: [Breadcrumbs, AsyncPipe],
    styleUrls: ['./footer.component.scss']
})
export class FooterComponent {
    breadcrumbs$ = this.store.select(selectBreadcrumbs);
    currentProcessGroupId$ = this.store.select(selectCurrentProcessGroupId);

    constructor(private store: Store<CanvasState>) {}
}
