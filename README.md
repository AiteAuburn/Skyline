# Skyline
This is the final project of the Distributed and Parallel Systems: Paradigms and Models course. In this repository, there is the parallel implementation of the skyline algorithm, in two versions: by using the standard C++17 programming language and by writing in standard C++17 but leaning also on the FastFlow library.

The aims of the project is to cleverly find the optimal way to speed up the skyline algorithm, defined on tuples of variable dimensions. The timing results are shown in the `report.pdf` file.

The tests have been performed on a knl xeon phi machine, with 64 processors and 4 context each, obtaining 256 logical contexts on which can run the various threads.
