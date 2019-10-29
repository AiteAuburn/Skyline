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

#include <vector>
#include <iostream>

#define TUPLES_IN_SKYLINE -2

using namespace std;

/* This class has been developed so that the elements in the skyline would have a position in the stream, that is used
 * in the sky_line_calc() function into the skyline class. */
class my_tuple {
private:
    vector<int> elements;
    int position = TUPLES_IN_SKYLINE;
public:

    void init(vector<int> * els, int pos){
        elements.swap(*els);
        position = pos;
    }
    void add(int els) {
        elements.push_back(els);
    }
    unsigned int size() {
        return elements.size();
    }

    const int operator[](unsigned long idx) const {
        return elements[idx];
    }

    bool equals(const my_tuple& el) {
        return (elements == el.elements);
    }

    int get_position(){
        return position;
    }

    void set_position(int pos){
        position = pos;
    }

    string print_tuple(){
        string res("(");
        for(long unsigned int i = 0; i < elements.size() - 1; i++)
            res += to_string(elements[i]) + ", ";
        res += to_string(elements[elements.size() - 1]) + ")";
        return res;
    }

};