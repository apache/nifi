import { createSelector } from '@reduxjs/toolkit';

import type { RootState } from './store';
import type { AboutState } from './aboutSlice';

export const selectAboutState = (state: RootState): AboutState => state.about;

export const selectAbout = createSelector(selectAboutState, (state) => state.about);
