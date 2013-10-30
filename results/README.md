with_min_grid_size results were produced when the Gridded and Bound Mappable Point
normalised to a grid size of 0 if the grid size was below 0.015.

without_min_grid_size results were produced after the concept of the min grid size was
removed.

See the following diff: https://github.com/robertpyke/PyThesis/commit/a6f66fc0e0c8dbe0f910fd51b22350518dbf8306

Results reported in the thesis are the "with_min_grid_size" results. The current code
produces the "without_min_grid_size" results.
