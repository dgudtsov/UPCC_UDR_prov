'''
Created on 6 янв. 2023 г.

@author: denis
'''

#TODO
# - files sharding

import random
import gzip
import sys
import os
import time

from upcc_generator_template import profile_template,multi_fields

IMSI_start = 401771000000001
MSISDN_start = 77470000001

#subscribers_count = 10

# generated subs = chunk_size * chunks_count  
chunk_size = 1000
chunks_count = 5

timestamp_precision = 10000

output_dir="./generator/"

filename_prefix = "upcc_"
filename_suffix = ".gz"

# qty of each fields will be generated
generator_random_multifields = {
    "quota"     :   5,
    "subscr"    :   6,
    "pkgsubscr" :   3
}

field_template="\t{key}={value};\n"

tag_begin = "<SUBBEGIN\n"
tag_end = "<SUBEND\n"

file_begin="<BEGINFILE>\n"
file_end="<ENDFILE>\n"

#=== Code

#def create_subs(f_out,IMSI,MSISDN):
def create_subs(f_out,rng):
# range(IMSI_start+j*chunk_size,IMSI_start+(j+1)*chunk_size),range(MSISDN_start+j*chunk_size,MSISDN_start+(j+1)*chunk_size))    

    IMSI, MSISDN = IMSI_start + rng , MSISDN_start + rng

#    with open(output_dir+"/"+str(MSISDN)+".txt",'w') as f_out:
#    print ("creating "+str(MSISDN))
    
    f_out.write(tag_begin)
    
    # writing profile fields
    for k in profile_template:
        
        if k=="MSISDN":
            value=str(MSISDN)
        elif k=="SUBSCRIBERIDENTIFIER":
            value=str(IMSI)
        else:
            value = profile_template[k]    
        
        # fill template and write it
        f_out.write (field_template.format(key = k, value = value))
    
    # writing QUOTA and SUBSCRIPTION fields
    for k in generator_random_multifields:
        [ f_out.write(multi_fields[k][random.randrange(1,len(multi_fields[k])-1)]+"\n") for j in range(0,random.randrange(1,generator_random_multifields[k])) ]
#        [ f_out.write(multi_fields[k][random.randrange(1,len(multi_fields[k])-1)]+"\n") for k in range(0,random.randrange(1,len(multi_fields))) ]          
    f_out.write (tag_end)    
    
    return

if __name__ == '__main__':
    
    print("Generating: "+str(chunks_count * chunk_size)+" subscribers")
    
    print("Output to: "+output_dir)
    
    # roundup
#    for j in range(int(subscribers_count/chunk_size) + (subscribers_count % chunk_size >0 )):
    for j in range(chunks_count):
        
        timestamp = int(time.time()*timestamp_precision)
        
        with gzip.open(output_dir+filename_prefix+str(timestamp)+filename_suffix, 'wt') as f_out:
            
            f_out.write(file_begin)
    
#            for i in range(chunk_size):
            print ("file: "+filename_prefix+str(timestamp)+filename_suffix)
            print ("imsi: "+str(range(IMSI_start+j*chunk_size,IMSI_start+(j+1)*chunk_size))+" msisdn: "+str(range(MSISDN_start+j*chunk_size,MSISDN_start+(j+1)*chunk_size)))
#            print (range(MSISDN_start+j*chunk_size,MSISDN_start+(j+1)*chunk_size))
            
#            list(map(lambda imsi, msisdn: create_subs(f_out,imsi, msisdn),range(IMSI_start+j*chunk_size,IMSI_start+(j+1)*chunk_size),range(MSISDN_start+j*chunk_size,MSISDN_start+(j+1)*chunk_size)))

            list(map(lambda rng: create_subs(f_out,rng),range(j*chunk_size,(j+1)*chunk_size)))
            
            f_out.write(file_end)
    
    exit(0)