'''
Created on 6 янв. 2023 г.

@author: denis
'''

#TODO
# - files sharding

import random

from upcc_generator_template import profile_template,multi_fields

IMSI_start = 401771000000001
MSISDN_start = 77470000001

subscribers_count = 100

output_dir="./generator"

field_template="\t{key}={value};\n"

tag_begin = "<SUBBEGIN\n"
tag_end = "<SUBEND\n"

#=== Code

def create_subs(IMSI,MSISDN):

    print ("creating "+str(MSISDN))

    with open(output_dir+"/"+str(MSISDN)+".txt",'w') as f_out:
    
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
        [ f_out.write(multi_fields[random.randrange(1,len(multi_fields)-1)]+"\n") for k in range(0,random.randrange(1,len(multi_fields))) ]
              
        f_out.write (tag_end)    
    
    return

if __name__ == '__main__':
    
    print("Generating: "+str(subscribers_count)+" subscribers")
    
    print("Output to: "+output_dir)
    
    list(map(create_subs,range(IMSI_start,IMSI_start+subscribers_count),range(MSISDN_start,MSISDN_start+subscribers_count)))
    
    exit(0)