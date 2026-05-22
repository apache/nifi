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

import { Store } from '@ngrx/store';
import { Observable } from 'rxjs';
import { takeUntil } from 'rxjs/operators';
import { ParameterContextEntity } from '../../../../state/shared';
import { selectConnectorParameterContext } from './connector-canvas.selectors';

/**
 * Subscribe to the connector-scoped parameter context for the current process group
 * and forward updates to a dialog while it is open. The dialog applies the value to
 * the property table so parameter references can resolve inline. The
 * `supportsParameters` flag is true when a parameter context is bound; callers that
 * need the flag (e.g. EditControllerService) can plumb it onto their instance.
 *
 * Shared between the connector canvas and controller-services effects so the
 * read-only dialog binding stays consistent across both pages.
 */
export function bindConnectorParameterContext(
    store: Store,
    teardown$: Observable<unknown>,
    apply: (parameterContext: ParameterContextEntity | null, supportsParameters: boolean) => void
): void {
    store
        .select(selectConnectorParameterContext)
        .pipe(takeUntil(teardown$))
        .subscribe((parameterContext) => apply(parameterContext, parameterContext != null));
}
