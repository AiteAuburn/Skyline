/**
* Name : Carmine
* Surname : Caserio
* Enrollment number : 490999
* Software : Skyline stream application
*
* I declare that the content of this file is in its entirety the original work of the author.
*
* Carmine Caserio
*/

#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <stdlib.h>
#include <time.h>
#include "clock.hpp"

#include "skyline.cpp"

#define NO_SEQ_ARGC 8
#define NO_PAR_ARGC 9

using namespace std;
using namespace ff;

using lui = long unsigned int;
using ui = unsigned int;

/* This is the main that is called from the command line. */
int main(int argc, char * argv[]) {
    /* Usage: ./spm_final seed max_generated_num stream_length sliding_window_size sliding_factor path
     * 'S'/'P'/'F' (for seq/par/ff computation) [number_of_workers_if_'P'_or_'F'] */
    if (argc < NO_SEQ_ARGC) {
        cout << "Usage: ./spm_final seed max_generated_num stream_length sliding_window_size sliding_factor "
             << "path 'S'/'P'/'F' (for seq/par/ff computation) [number_of_workers_if_'P'/'F']" << endl;
        return 1;
    }
    char p_par = argv[7][0];
    if ((p_par == 'S' && argc != NO_SEQ_ARGC) || (p_par == 'P' && argc != NO_PAR_ARGC) ||
        (p_par == 'F' && argc != NO_PAR_ARGC) || (p_par == 'V' && argc != NO_PAR_ARGC)) {
        cout << "Usage: ./spm_final seed max_generated_num stream_length sliding_window_size sliding_factor "
             << "path 'S'/'P'/'F' (for seq/par/ff computation) [number_of_workers_if_'P'/'F']" << endl;
        return 1;
    }
    if(p_par != 'S' && p_par != 'P' && p_par != 'F' && p_par != 'V'){
        cout << "Usage: ./spm_final seed max_generated_num stream_length sliding_window_size sliding_factor "
             << "path 'S'/'P'/'F' (for seq/par/ff computation) [number_of_workers_if_'P'/'F']" << endl;
        return 1;
    }

    int seed = atoi(argv[1]);
    int n = atoi(argv[2]);
    int stream_length = atoi(argv[3]);
    int sliding_window = atoi(argv[4]);
    int sliding_factor = atoi(argv[5]);
    string path = argv[6];

    int nw = -1;

    if (stream_length != -1 && stream_length < sliding_window) {
        cout << "Error: the stream length needs to be greater than the sliding window. " << endl;
        return 1;
    }

    if (sliding_window < sliding_factor) {
        cout << "Error: the sliding window size needs to be greater than the sliding factor. " << endl;
        return 1;
    }

    if (argc == NO_PAR_ARGC) {
        nw = atoi(argv[8]);
    }

    skyline sl(seed, n, stream_length, sliding_window, sliding_factor, p_par, nw, path);


    switch (p_par) {
        case 'S': {
            sl.start_seq();
            break;
        }
        case 'P': {
            sl.start_par();
            break;
        }
        case 'F': {
            sl.start_ff();
            break;
        }
        default:;
    }

    return 0;
}
