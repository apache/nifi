import { configureStore } from '@reduxjs/toolkit';

import clusterSummaryReducer from './clusterSummarySlice';
import extensionTypesReducer from './extensionTypesSlice';
import loginConfigurationReducer from './loginConfigurationSlice';
import currentUserReducer from './currentUserSlice';
import flowConfigurationReducer from './flowConfigurationSlice';
import bannerTextReducer from './bannerTextSlice';
import navigationReducer from './navigationSlice';
import aboutReducer from './aboutSlice';

const store = configureStore({
  reducer: {
    extensionTypes: extensionTypesReducer,
    clusterSummary: clusterSummaryReducer,
    loginConfiguration: loginConfigurationReducer,
    currentUser: currentUserReducer,
    flowConfiguration: flowConfigurationReducer,
    bannerText: bannerTextReducer,
    navigation: navigationReducer,
    about: aboutReducer
  }
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;

export default store;
