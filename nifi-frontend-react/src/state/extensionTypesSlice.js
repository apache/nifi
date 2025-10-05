import { createSlice } from '@reduxjs/toolkit';

const extensionTypesSlice = createSlice({
  name: 'extensionTypes',
  initialState: {},
  reducers: {
    loadExtensionTypesForCanvas: (state) => {
      // Implement loading logic here
    },
  },
});

export const { loadExtensionTypesForCanvas } = extensionTypesSlice.actions;
export default extensionTypesSlice.reducer;
