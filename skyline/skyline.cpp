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

#include <ff/ff.hpp>

#include "my_tuple.cpp"
#include "blocking_queue.hpp"
#include "clock.hpp"

#include <queue>
#include <tuple>
#include <atomic>
#include <string>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <fstream>
#include <iostream>
#include <cstddef>
#include <utility>

#define TERMINATION_VAL -1
#define TUPLES_IN_SKYLINE -2

using lui = long unsigned int;
using ui = unsigned int;

using namespace std;
using namespace ff;



class skyline {

private:
    ui seed = 0;
    int n = -1;
    int stream_length = -1;
    int sliding_window = -1;
    int sliding_factor = -1;
    int nw = -1;
    char par = 'S';
    string path;

    vector<thread *> tid;


    atomic<int> iter{0};

public:

    skyline(ui p_seed, int p_n, int p_stream_length,
            int p_sliding_window, int p_sliding_factor, char p_par, int p_nw, string p_path) :
            seed(p_seed), n(p_n), stream_length(p_stream_length),
            sliding_window(p_sliding_window), sliding_factor(p_sliding_factor), par(p_par), path(std::move(p_path))
    {
        srand((unsigned)seed);
        if((par == 'P') || (par == 'F'))
            nw = p_nw;
    };


    /* This function updates the skyline, by inserting a new tuple when it dominates the already present ones in the
     * skyline. The check needs to be done for all the size of the skyline, since it may happens that the new tuple
     * dominates all the others already present. */
    static bool sky_line_calc(my_tuple curr, vector<my_tuple> *sky_line) {
        bool to_add = false;
        bool exists_greater;
        lui i = 0;
        ui all_goe = 0;

        if (sky_line->empty()){
            sky_line->push_back(curr);
            return true;
        }

        vector<int> pos_to_del;

        while(i < sky_line->size()){
            /*
             * Tuple comparison:
             * the bool exists_greater becomes true if an element of the tuple curr[j] is greater than the
             * corresponding element sky_line[i][j];
             * the bool all_goe is initialized to 0 and it becomes sky_line[i].size() if all the elements
             * of curr are greater or equal than the corresponding elements of sky_line[i]
             */
            exists_greater = false;
            to_add = false;
            all_goe = 0;

            for (int j = 0; j < curr.size(); j++){
                if(curr[j] >= (*sky_line)[i][j]){
                    all_goe++;
                    if(curr[j] > (*sky_line)[i][j])
                        exists_greater = true;
                } else
                    if(exists_greater)
                        break;
            }
            if (exists_greater && all_goe == (*sky_line)[i].size()) {
                pos_to_del.push_back(i);
                to_add = true;
            }
            if (exists_greater && all_goe < (*sky_line)[i].size()){
                to_add = true;
            }
            i++;
        }
        if (to_add) {
            if(!pos_to_del.empty())
                for(int idx = pos_to_del.size() - 1; idx >= 0; idx--){
                    sky_line->erase(sky_line->begin() + pos_to_del[idx]);
                }
            sky_line->push_back(curr);
            return true;
        }
        return false;
    }


    /* This function creates a normal tuple, filled with positive random integers. */
    static my_tuple create_tuple(int pos, int sliding_factor, int n){
        vector<int> ts;
        ts.reserve(sliding_factor);
        for(int j = 0; j < sliding_factor; j++)
            ts.push_back(rand() % n);
        my_tuple t;
        t.init(&ts, pos);
        return t;
    }

    /* This function creates the special kind of tuple useful for the termination of the various threads. */
    static my_tuple create_termination_tuple(){
        my_tuple t;
        t.set_position(TERMINATION_VAL);
        return t;
    }

    /* This function creates the special kind of tuple useful for communicating the size of the skyline
     * that the collector has to pop after receiving it. */
    static my_tuple create_no_tpls_in_skyline_tuple(int els){
        my_tuple t;
        t.set_position(TUPLES_IN_SKYLINE);
        t.add(els);
        return t;
    }

    /* This function prints the skyline in the specified path. */
    static void print_skyline(const string& path, vector<my_tuple> * skyline, int iter){
        if(path == "stdout"){
            if(!skyline->empty()){
                printf("Final skyline - iter %d:\n", iter);
                for(int i = 0; i < skyline->size() - 1; i++)
                    printf("%d: %s - ", (*skyline)[i].get_position(), (*skyline)[i].print_tuple().c_str());
                printf("%d: %s\n\n", (*skyline)[skyline->size() - 1].get_position(),
                       (*skyline)[skyline->size() - 1].print_tuple().c_str());
            } else {
                printf("Final skyline - iter %d: --- empty --- \n", iter);
            }
        } else {
            ofstream results;
            results.open(path, ios_base::app);
            if(!skyline->empty()){
                results << "Final skyline - iter " << iter << ":\n";
                for(int i = 0; i < skyline->size() - 1; i++)
                    results << (*skyline)[i].get_position() << ": " << (*skyline)[i].print_tuple().c_str() << endl;
                results << (*skyline)[skyline->size() - 1].get_position() << ": "
                        << (*skyline)[skyline->size() - 1].print_tuple().c_str() << "\n\n";

            } else {
                results << "Final skyline - iter " << iter << ": --- empty --- \n";
            }
            results.close();
        }

    }

    /* This function implements the sequential version of the skyline algorithm */
    void start_seq(){
        vector<my_tuple> skyline;
        vector<my_tuple> stream;
        int idx = 0;
        int rght = sliding_window;
        int lft = 0;
        int iter = 0;

        ffTime(START_TIME);
        while(rght <= stream_length){
            for(int i = idx; i < rght; i++)
                stream.push_back(create_tuple(i, sliding_factor, n));
            auto now = timer::start();
            for(int i = lft; i < rght; i++)
                sky_line_calc(stream[i], &skyline);
            auto then = timer::micro_step(now);
            cout << then << endl << endl << endl;
            print_skyline(path, &skyline, iter);
            idx = rght;
            rght += sliding_factor;
            lft += sliding_factor;
            iter++;
        }
        ffTime(STOP_TIME);
        cout << "S: " << ffTime(GET_TIME) << " milli secs" << endl;
    }


    /* This function implements the parallel version of the skyline algorithm.
     * The parallel architecture chosen to parallelize the algorithm is the farm,
     * where each worker's job is executed on entire sliding windows. */
    void start_par(){

        vector<blocking_queue<my_tuple>> gen_to_workers(nw);
        vector<blocking_queue<my_tuple>> workers_to_coll(nw);

        /* This lambda function is executed by the emitter thread of the farm.
         * It creates and sends the tuples one sliding window at a time and at the end of the stream,
         * it sends a special kind of tuple needed for telling to the workers that the stream generator
         * is terminating and so have to do they. */
        auto stream_generator = [&](){
            int curr_w = 0;
            vector<my_tuple> local_stream;

            int idx = 0;
            int rght = sliding_window;
            int lft = 0;
            // CREATING AND SENDING SLIDING WINDOWS
            while(rght <= stream_length){
                for(int i = idx; i < rght; i++)
                    local_stream.push_back(create_tuple(i, sliding_factor, n));
                for(int i = lft; i < rght; i++)
                    gen_to_workers[curr_w].push(local_stream[i]);
                idx = rght;
                rght += sliding_factor;
                lft += sliding_factor;
                curr_w = (curr_w + 1) % nw;
            }
            // KILLING ALL WORKERS
            for(int i = 0; i < nw; i++)
                gen_to_workers[i].push(create_termination_tuple());
        };

        /* This lambda function is executed by the worker threads of the farm.
         * It pops the tuples from the queue and checks whether it is the special kind of tuple for termination.
         * - In case it is, the worker sets its own output queue to busy, so that the collector cannot access it
         *   and forwards the tuple to the collector, before exiting, so that the collector terminates, too.
         * - In case it is not, the worker do its local computation of the skyline and if the number of skyline
         *   computations becomes equal to the sliding window size, it sets its output queue to busy, pushes a
         *   special kind of tuple, for communicating to the collector the number of tuples that have to be popped
         *   from that one, inserts all the tuples of the skyline and resets to free its output queue. */
        auto worker = [&](int widx){
            vector<my_tuple> skyline;
            int curr_idx = 0;
            while(true){
                my_tuple t = gen_to_workers[widx].pop();
                // KILL THE COLLECTOR ...
                if(t.get_position() == TERMINATION_VAL){
                    workers_to_coll[widx].set_busy();
                    workers_to_coll[widx].push(t);
                    workers_to_coll[widx].set_free();
                    break;
                } else{
                    sky_line_calc(t, &skyline);
                } // ... OR COMPUTE THE SKYLINE

                curr_idx++;
                if(curr_idx == sliding_window){
                    workers_to_coll[widx].set_busy();
                    workers_to_coll[widx].push(create_no_tpls_in_skyline_tuple(skyline.size()));
                    for(int i = 0; i < skyline.size(); i++)
                        workers_to_coll[widx].push(skyline[i]);
                    workers_to_coll[widx].set_free();
                    curr_idx = 0;
                }
            }
        };

        /* This lambda function is executed by the collector of the farm.
         * It tries to pop from the various queues, in a round robin fashion, until it doesn't find one queue
         * that is free and not empty.
         * In that case, it pops the tuple and checks whether it is the special kind of tuple used for communicating
         * the number of tuples that have to be popped since then, or the termination tuple. In the latter case,
         * it waits until all the workers sends the termination tuple. In the former case, once it know the number of
         * tuples to pop, it pops that number of tuples and at the end of the extraction, it prints in the path
         * specified from command line, the skyline that worker has computed. */
        auto collector = [&](){
            int nw_term = 0;
            int curr_w = 0;

            vector<my_tuple> skyline;
            int iter = 0;
            while(true){
                auto tp = workers_to_coll[curr_w].try_pop();
                if(!tp){
                    curr_w = (curr_w + 1) % nw;
                    continue;
                }
                my_tuple t = tp.value();
                // IF IT IS A SPECIAL TUPLE, IT POPS THE AMOUNT SPECIFIED IN THE ONLY COMPONENT OF THE TUPLE ...
                if (t.get_position() == TUPLES_IN_SKYLINE){
                    int sk_size = t[0];
                    for(int i = 0; i < sk_size; i++)
                        skyline.push_back( workers_to_coll[curr_w].pop() );
                    print_skyline(path, &skyline, iter);
                    iter++;
                    curr_w = (curr_w + 1) % nw;
                    skyline.clear();
                } else{ // ... OTHERWISE, WAITS FOR ALL THE WORKERS TO FINISH.
                    if(t.get_position() == TERMINATION_VAL){
                        nw_term++;
                        if(nw_term == nw)
                            break;
                    }
                }

            }
        };

        /* Starting of the threads. */
        tid.resize(0);

        ffTime(START_TIME);

        tid.emplace_back(new thread(collector));
        for(lui i = 0; i < (lui)nw; i++)
            tid.emplace_back(new thread(worker, i));
        stream_generator();

        /* Joining of the threads. */
        for(auto & t : tid){
            t->join();
            delete(t);
        }

        ffTime(STOP_TIME);

        /* Printing of the timing. */
        cout << "P: " << nw << ": " << ffTime(GET_TIME) << " milli secs" << endl;
    }

    /* This function implements the parallel version of the skyline algorithm using the FastFlow library.
     * The parallel architecture chosen to parallelize the algorithm is the farm without the collector,
     * where each worker's job is executed on entire sliding windows. */
    void start_ff(){
        static mutex mut_print_ff;

        /* This class has the same behaviour of the stream generator in the other version.
         * It needs a reference to the farm because it has to send to one specific worker the entire sliding window. */
        class stream_generator : public ff_node {
        private:
            int sliding_window, sliding_factor, stream_length, n, idx = 0, rght, lft = 0, curr_w = 0, nw;
            ff_farm *f;
            vector<my_tuple> local_stream;

        public:
            stream_generator(int p_sliding_window, int p_sliding_factor, int p_stream_length, int p_n, int p_nw,
                    ff_farm *fa) :
                sliding_window(p_sliding_window), sliding_factor(p_sliding_factor), stream_length(p_stream_length),
                n(p_n), nw(p_nw), f(fa), rght(p_sliding_window) {}

            my_tuple gen_tuple(int pos){
                my_tuple t;
                vector<int> ts;
                ts.reserve(sliding_factor);
                for(int j = 0; j < sliding_factor; j++)
                    ts.push_back(rand() % n);
                t.init(&ts, pos);
                return t;
            }

            void *svc(void* task){
                if(rght <= stream_length){
                    for(int i = idx; i < rght; i++)
                        local_stream.push_back(gen_tuple(i));
                    for(int i = lft; i < rght; i++)
                        f->getlb()->ff_send_out_to((void *)new my_tuple(local_stream[i]), curr_w);

                    curr_w = (curr_w + 1) % nw;
                    idx = rght;
                    rght += sliding_factor;
                    lft += sliding_factor;
                    return GO_ON;
                }
                ff_send_out(EOS);
                return EOS;
            }
        };

        /* This class behaves as the workers in the other version, with a single difference, that is, in here there is
         * also the print of the various skylines, and, since it may be done in parallel, and so concurrently with other
         * workers, the mutual exclusive access to the printing of the various skylines has been granted by means of a
         * mutex variable. */
        class worker : public ff_node {
        private:
            vector<my_tuple> skyline;
            int curr_idx = 0, sliding_window, iter = 0;
            string path;
        public:

            worker(int p_sliding_window, int w_idx, string p_path) : sliding_window(p_sliding_window), path(p_path) {}

            void *svc(void *task) {
                if(task != NULL){
                    auto * t = (my_tuple *)task;
                    sky_line_calc(*t, &skyline);
                    curr_idx++;

                    if(curr_idx == sliding_window){
                        {
                            unique_lock<mutex> lock(mut_print_ff);
                            print_skyline(path, &skyline, iter);
                        }
                        skyline.clear();
                        curr_idx = 0;
                        iter++;
                    }
                }
                return GO_ON;
            }
        };


        // --- farm without collector
        ff_farm farm;
        stream_generator sg(sliding_window, sliding_factor, stream_length, n, nw, &farm);

        vector< ff_node* > workers;
        for (int i = 0; i < nw; i++)
            workers.push_back(new worker(sliding_window, i, path));

        farm.add_emitter(&sg);
        farm.add_workers(workers);

        ffTime(START_TIME);
        if (farm.run_and_wait_end() < 0)
            printf("Running pipe");
        ffTime(STOP_TIME);
        cout << "F: " << nw << ": " << ffTime(GET_TIME) << " milli secs" << endl;
    }

};
