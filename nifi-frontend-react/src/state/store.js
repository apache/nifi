import { configureStore } from '@reduxjs/toolkit';

import clusterSummaryReducer from './clusterSummarySlice';

const store = configureStore({
  reducer: {
    extensionTypes: extensionTypesReducer,
    clusterSummary: clusterSummaryReducer,
  },
});

export default store;
