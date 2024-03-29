#ifndef _SM4_2_H
#define _SM4_2_H
#include "sm4.h"
#include "basefun.h"

int ready2encode_file(int argc, char *argv[]) {
	
	if(argc != 5) return 0;
	
	char From[200],To[200];
	unsigned char key[16] = {0x01,0x23,0x45,0x67,0x89,0xab,0xcd,0xef,0xfe,0xdc,0xba,0x98,0x76,0x54,0x32,0x10};
	unsigned char input[16];
	unsigned char output[16];
	unsigned char temp[50];
    sm4_context ctx;
    int mode;

    strcpy(From, argv[1]);
    strcpy(To, argv[2]);
    strcpy(temp, argv[3]);
    mode = s2int(argv[4]);

    for(int i = 0;i<16;++i){
    	int value = (temp[i*2] + temp[i*2+1]) % 0xFF;
    	key[i] = value;
	}
    
    FILE* get = fopen(From,"rb");
    FILE* put = fopen(To,"wb");

    if (get == NULL || put == NULL){
        printf("The File Opened Failed !\n");
    }

	if(mode == 1){
		while(1){
			int cnt = 0;
			for(int i = 0;i<16&&fread(input+i,1,1,get);i++,cnt++);
            if(cnt<16){
                for(int i=0;i<cnt;i++)
                    fwrite(input+i,1,1,put);
                break;
            }
		    sm4_setkey_enc(&ctx,key);
		    sm4_crypt_ecb(&ctx,1,16,input,output);
		    for(int i=0;i<16;i++)
		        fwrite(output+i,1,1,put);
		}
	}
	else if(mode == 0){
		while(1){
			int cnt = 0;
			for(int i = 0;i<16&&fread(input+i,1,1,get);i++,cnt++);
            if(cnt<16){
                for(int i=0;i<cnt;i++)
                    fwrite(input+i,1,1,put);
                break;
            }
		    sm4_setkey_dec(&ctx,key);
    		sm4_crypt_ecb(&ctx,0,16,input,output);
		    for(int i=0;i<cnt;i++)
		        fwrite(output+i,1,1,put);
		}
	}


	fclose(get);
	fclose(put);

	return 0;
}
#endif