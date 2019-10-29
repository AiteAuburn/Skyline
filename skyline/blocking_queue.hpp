#ifndef SKYLINE_BLOCKING_QUEUE_HPP
#define SKYLINE_BLOCKING_QUEUE_HPP

#include <iostream>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <vector>
#include <chrono>
#include <cstddef>
#include <math.h>
#include <string>
#include <thread>
#include <experimental/optional>


using namespace std::literals::chrono_literals;

//
// needed a blocking queue
// here is a sample queue.
//

template <typename T>
class blocking_queue
{
private:
	std::mutex				d_mutex;
	std::condition_variable	d_condition;
	std::deque<T>			d_queue;
	bool                    worker_in_queue;
public:

	blocking_queue(){}

	void push(T const& value) {
		{
			std::unique_lock<std::mutex> lock(this->d_mutex);
			d_queue.push_front(value);
		}
		this->d_condition.notify_one();
	}

	T pop() {
		std::unique_lock<std::mutex> lock(this->d_mutex);
		this->d_condition.wait(lock, [=]{return !this->d_queue.empty(); });
		T rc(std::move(this->d_queue.back()));
		this->d_queue.pop_back();
		return rc;
	}

	std::experimental::optional<T> try_pop() {
        std::unique_lock<std::mutex> lock(this->d_mutex);
        if(!worker_in_queue && !this->d_queue.empty()) {
            T rc(std::move(this->d_queue.back()));
            this->d_queue.pop_back();
            return rc;
        }
        return {};
	}

	void set_busy(){
	    worker_in_queue = true;
	}

	void set_free(){
	    worker_in_queue = false;
	}

	int size() {
        std::unique_lock<std::mutex> lock(this->d_mutex);
        return(d_queue.size());
	}

};

#endif // SKYLINE_BLOCKING_QUEUE_HPP
