#ifndef _BASEFUN_H
#define _BASEFUN_H
char* int2string(int value){

    char* res = (char*)malloc(1000);
    memset(res, '\0', sizeof(res));

    int idx = 0;

    while(value){
        res[idx] = '0' + value%10;
        value /= 10;
        ++idx;
    }
    res[idx] = '\0';

    char temp[100];

    for(int i = 0;i < idx; ++i){
        temp[i] = res[idx-i-1];
    }
    temp[idx] = '\0';

    strcpy(res, temp);

    return res;
}

int s2int(char* value){
    int ans=0;

    for(int i = 0;i<strlen(value);++i,ans*=10) {
        ans += value[i] - '0';
    }

    return ans/10;
}
#endif