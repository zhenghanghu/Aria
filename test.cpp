#include<iostream>
#include <tuple>
#include<map>
#include<list>
#include<vector>

using namespace std;

#define HELLO(X) cout<<"hi"<<endl;



struct node{
    int x,y;
    void print(){
        cout<<x<<" "<<y<<endl;
    }
};

int main(){

    node p;
    p.x = 10;
    p.y = 20;
    auto &z = p.x;
    z = 12;
    cout<<p.x<<endl;


    return 0;
}