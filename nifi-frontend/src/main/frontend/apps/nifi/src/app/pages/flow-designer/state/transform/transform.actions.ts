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

import { createAction, props } from '@ngrx/store';
import { CanvasTransform } from './index';

export const transformComplete = createAction(
    '[Transform] Transform Complete',
    props<{ transform: CanvasTransform }>()
);

export const restoreViewport = createAction('[Transform] Restore Viewport');

export const restoreViewportComplete = createAction('[Transform] Restore Viewport Complete');

export const translate = createAction('[Transform] Translate', props<{ translate: [number, number] }>());

export const refreshBirdseyeView = createAction('[Transform] Refresh Birdseye View');

export const zoomIn = createAction('[Transform] Zoom In');

export const zoomOut = createAction('[Transform] Zoom Out');

export const zoomFit = createAction('[Transform] Zoom Fit', props<{ transition: boolean }>());

export const zoomActual = createAction('[Transform] Zoom Actual');
