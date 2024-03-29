//
// Created by Zijian Zhou on 2023/11/19.
//

//#include "export_sm3.c"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sm3.h"
#include "sm3_2.h"


int main(){

    unsigned char* output;
    output = (unsigned char*)malloc(sizeof(unsigned char)*32);

    unsigned char input[] = "123456";

    sm3(input, 6, output);

    for(int i = 0;i<32;i++){
        printf("%02X", output[i]);
    }


    printf("\n");

    return 0;
}
