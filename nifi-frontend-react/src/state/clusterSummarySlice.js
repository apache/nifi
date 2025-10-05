import { createSlice } from '@reduxjs/toolkit';

const initialState = {
  disconnectionAcknowledged: false,
  // Add other cluster summary state properties as needed
};

const clusterSummarySlice = createSlice({
  name: 'clusterSummary',
  initialState,
  reducers: {
    setDisconnectionAcknowledged(state, action) {
      state.disconnectionAcknowledged = action.payload;
    },
    // Add other reducers as needed
  },
});

export const { setDisconnectionAcknowledged } = clusterSummarySlice.actions;
export default clusterSummarySlice.reducer;
