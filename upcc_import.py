#!/usr/local/bin/python3
# encoding: utf-8
'''
upcc_import -- shortdesc

upcc_import is a description

It defines classes_and_methods

@author:     user_name

@copyright:  2023 organization_name. All rights reserved.

@license:    license

@contact:    user_email
@deffield    updated: Updated
'''

import sys
import os
import time
import gzip

from datetime import timedelta

import random

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

import json

from upcc_import_template import *
from upcc_pkgsubscription import pkgsubscription
from upcc_servicequota import servicequota

__all__ = []
__version__ = 0.1
__date__ = '2023-01-05'
__updated__ = '2023-02-18'

DEBUG = 0
TESTRUN = 0
PROFILE = 0

timestamp_precision = 10000

default_chunk_size=1000000
#default_chunk_size=500000

import_dir='./import'

output_dir='./output/'
filename_prefix='i_'
filename_suffix='.ixml.gz'

export_result = 'export.csv.gz'

#=== Constants
fields_names={
    'QUOTA':("QUOTANAME","QUOTAID","INITIALVALUE","BALANCE","CONSUMPTION","STATUS","LASTRESETDATETIME","NEXTRESETDATETIME","RESETTYPE","CYCLETYPE","CUSTOMLEVEL1","CUSTOMLEVEL2","CUSTOMLEVEL3","CUSTOMSTATUS","CUMULATEINDICATOR","PREUPLOAD","PREDOWNLOAD","PRECONSUMPTION","QUOTAFLAG","UPDATEDTIME","QUOTAUNIT","RESETCAUSE","CURCUMTIMES","ACCUMBVALUE","PLMNCUSTOMERATTR","UPDATETIME2","INITIALONLINETIME","EXHAUSTDATETIME","QUOTABALANCEINFO","EFFECTIVETIME")
    ,'SUBSCRIPTION':("SERVICENAME","SERVICEID","STATUS","SUBSCRIBEDATETIME","VALIDFROMDATETIME","EXPIREDATETIME","ACTIVATIONSTATUS","SHAREFLAG","SVCPKGID","ROAMTYPE","SUBSCRIBEDTYPE","SRVVALIDPERIOD","REDIRECTTIME","REDIRECTURLID","SRVSTATUS","FLAG","CONTACTMETHOD","USEDFLAG","SERVICEBILLINGTYPE","NOTIFICATIONCYCLE","ACTSTARTDATETIME","ACTENDDATETIME","RESTTIME","MILLISECOND","ORDERTIMES","PAYMENTFLAG","ADDONFLAG")
    ,'PKGSUBSCRIPTION':("PKGNAME","PKGID","SUBSCRIBEDATETIME","VALIDFROMDATETIME","EXPIREDATETIME","ROAMTYPE","CONTACTMETHOD")
}

multi_fields = ('SUBSCRIBERGRPNAME','SUBSCRIPTION','PKGSUBSCRIPTION','QUOTA','ACCOUNT')

tag_begin = "<SUBBEGIN"
tag_end = "<SUBEND"

file_begin="<BEGINFILE>\n"
file_end="<ENDFILE>\n"

#define fields mapping from UPCC to UDR profile
upcc2profile_mappings = {
'MSISDN':'MSISDN',
'SUBSCRIBERIDENTIFIER':'IMSI',
'STATION':'Custom20',
'USRMASTERIDENTIFIER':'Custom20',
'BILLINGCYCLEDAY':'BillingDay',

'EXATTR1':'Custom1',
'EXATTR2':'Custom2',
'EXATTR3':'Custom3',
'EXATTR4':'Custom4',
'EXATTR5':'Custom5',
'EXATTR6':'Custom6',
'EXATTR7':'Custom7',
'EXATTR8':'Custom8',
'EXATTR9':'Custom9',
'EXATTR10':'Custom10',
'EXATTR11':'Custom11',
'EXATTR12':'Custom12',
'EXATTR13':'Custom13',
'EXATTR14':'Custom14',
'EXATTR15':'Custom15',
'EXATTR16':'Custom16',
'EXATTR17':'Custom17',
    }

SRVSTATUS_Frozen = 1
upcc_SUBSCRIPTION_mapping = {
    'SERVICENAME' : 'Entitlement',
    'SRVSTATUS_Frozen' : 'Custom18'
    }

# quota prefix to be added
quota_prefix='T2-'

# quota size multiplier
quota_mult = 1000

#=== Code

class UPCC_Subscriber(object):
    def __init__(self,rows_set):
#        self.fname = fname
        '''
        # stores original UPCC fields in key-value pairs
        # the following attrs are used as list (even having single value):
        # 'SUBSCRIBERGRPNAME','SUBSCRIPTION','PKGSUBSCRIPTION','QUOTA','ACCOUNT'
        '''
# stores source attributes from UPCC
        self.attrs=dict()

# stores mapped quota parameters         
        self.quota = list()

# top-up modifiers        
        self.dyn_quota = list()

# stores mapped udr profile
        self.profile=dict()

#        subscriber_begin=False
        
#        with open(self.fname,'r') as f_inp:
        # if True:
        #     for f_line in f_inp:
        #
        #         if tag_begin in f_line:
        #             subscriber_begin=True
        #             continue
        #
        #         elif tag_end in f_line:
        #             subscriber_begin=False
        #             break
        for f_line in rows_set:
                f_line_str = f_line.strip().rstrip(';')
                
                if DEBUG:
                    print (f_line)
                    print (f_line_str)

#                if subscriber_begin and len(f_line_str)>1:
                if len(f_line_str)>1:
                    # separator between attribute and its value
                    (s_key,s_value) = (f_line_str.split('=')[0], f_line_str.split('=')[1])

                    # list of attributes with multiple occurence
                    if s_key in multi_fields:
                        if s_key in self.attrs:
                            self.attrs[s_key].append(s_value)
                        else:
                            self.attrs.update({s_key: [s_value]})
                    else:
                        self.attrs.update({s_key: s_value})
        # iterate over only those fields which are defined
        # and automatically unpack complex attributes (quota, subscription, etc.)
        for field in fields_names.keys():
            # if field is defined in source and contain values (is not empty)
            if field in self.attrs:
                if len(self.attrs[field]) >0:
                    self.__unpack_field__(field)        
        
        return
    
    def __unpack_field__(self,field):
        '''
        # transforms string representation like:
        #     QUOTA=413102-DATA_D_Quota&E7242330DC433136&1024&0&0&6&20221031102855&FFFFFFFFFFFFFF&0&255&0&0&0&0&0&0&0&0&0&1667190535&1;
        # into dict structure
        '''

        for index,entity_string in enumerate(self.attrs[field]):
            # fields separator inside complex attributes (quota, subscription, etc.)
            entity_fields = entity_string.split("&")
            
            # for case when number of fields parsed less than fields were defined
            for i in range(0,len(fields_names[field])-len(entity_fields)):
                entity_fields.append(None) 
            
            entity_dict = dict()
            
            # min function is safe here
            entity_dict = {fields_names[field][i]: entity_fields[i] for i in range(0, min(len(entity_fields),len(fields_names[field])))}
            
            self.attrs[field][index] = entity_dict

        return
    
    # return number of records for attributes
    def elements(self):
        return len(self.attrs.keys())
    
    def mapping(self):
        '''
        # internal structure:
        # self.profile - dict, containing base profile attributes: MSISDN, IMSI, Ent, Tier, CustomX, etc.
        # example: {'IMSI': '401771121496414', 'MSISDN': '77089675915', 'Custom3': '0', 'Custom6': '0', 'Custom7': '0', 'Custom8': '0'}
        # self.quota - list of dicts, each dict contains: quota name and quota usage
        # example: [{'QUOTA': '409239-DATA_D_Quota', 'USAGE': '65013247'}, {'QUOTA': '40777900081-DATA_D_Quota', 'USAGE': '865069'}, {'QUOTA': '413102-DATA_D_Quota', 'USAGE': '0'}]
        '''
        
        # map all attributes were defined in upcc2profile_mappings
        [ self.profile.update({upcc2profile_mappings[k]:self.attrs[k]}) for k in self.attrs if k in upcc2profile_mappings ]
        
        # BillingDay normalization
        try:
            if int(self.profile['BillingDay'])<0 or int(self.profile['BillingDay'])>31:  
                raise ValueError
        except:
            self.profile['BillingDay'] = 0

        
        #PKGSUBSCRIPTION to SUBSCRIPTION mapping
        if 'PKGSUBSCRIPTION' in self.attrs: 
            if len(self.attrs['PKGSUBSCRIPTION'])>0 :
                # for each package
                for pkg in self.attrs['PKGSUBSCRIPTION']:
                    
                    # using pkgsubscription imported from upcc_pkgsubscription module
                    if pkg['PKGNAME'] in pkgsubscription:
                        # get list of services assigned to package
                        servicenames = pkgsubscription[pkg['PKGNAME']]
                         
                        # appending original SUBSCRIPTION list with synthetic values from package 
                        for s in servicenames:
                            self.attrs['SUBSCRIPTION'].append( dict(SERVICENAME=s) )  
                        
                    else:
                        #print("Error: PKGSUBSCRIPTION is not found: "+pkg['PKGNAME'])
                        pass
        
        # SUBSCRIPTION to Entitlement mapping
        if 'SUBSCRIPTION' in self.attrs: 
            if len(self.attrs['SUBSCRIPTION'])>0 :
                
                # Entitlement
                self.profile[upcc_SUBSCRIPTION_mapping['SERVICENAME']] = list()
                
                # Custom18
                self.profile[upcc_SUBSCRIPTION_mapping['SRVSTATUS_Frozen']] = list()
                
                for subscription in self.attrs['SUBSCRIPTION']:
                    # mapping service to entitlement
                    self.profile[upcc_SUBSCRIPTION_mapping['SERVICENAME']].append(subscription['SERVICENAME'])
                    
                    #SRVSTATUS = 1 (Frozen)
                    if 'SRVSTATUS' in subscription:
                        if subscription['SRVSTATUS'] == SRVSTATUS_Frozen:
#                            print ("frozen: " + self.profile['IMSI'] )
                            self.profile[upcc_SUBSCRIPTION_mapping['SRVSTATUS_Frozen']].append(subscription['SERVICENAME'])
                    
                    #mapping service to quota
                    if subscription['SERVICENAME'] in servicequota:
                        # get quota for service
#                        q = servicequota[subscription['SERVICENAME']]
                        quotas = servicequota[subscription['SERVICENAME']]
                        
                        for q in quotas: 
                            # check if quota is already assigned
                            q_assign=False
                            for instance in self.attrs['QUOTA']:
                                if q==instance['QUOTANAME']:
                                    q_assign=True
                                    break
                            
                            if not q_assign:
                                self.attrs['QUOTA'].append( dict(QUOTANAME=q,CONSUMPTION=0) )
#                    else:
#                        print("Error: SERVICENAME is not found in quota mapping: "+subscription['SERVICENAME'])
                    
            
            # remove duplicated entitlements
                self.profile[upcc_SUBSCRIPTION_mapping['SERVICENAME']] = list(dict.fromkeys(self.profile[upcc_SUBSCRIPTION_mapping['SERVICENAME']]))
            # remove duplicated frozen servises                
                self.profile[upcc_SUBSCRIPTION_mapping['SRVSTATUS_Frozen']] = list(dict.fromkeys(self.profile[upcc_SUBSCRIPTION_mapping['SRVSTATUS_Frozen']]))
            
            # map list into string
                if len(self.profile[upcc_SUBSCRIPTION_mapping['SRVSTATUS_Frozen']])>0:
                    self.profile[upcc_SUBSCRIPTION_mapping['SRVSTATUS_Frozen']] = ';'.join(self.profile[upcc_SUBSCRIPTION_mapping['SRVSTATUS_Frozen']])
                else:
                #remove key
                    del self.profile[upcc_SUBSCRIPTION_mapping['SRVSTATUS_Frozen']]   
                
        
        # Quota mapping
        if 'QUOTA' in self.attrs:
            if len(self.attrs['QUOTA'])>0 :
                                              
                for instance in self.attrs['QUOTA']:
                    
                    # define new dict and transfer there fields from self.attrs 
                    quota, dyn_quota = dict(), dict() 
                    
                    quota_volume = 0
                    
                    Q_INITIAL, Q_BALANCE, Q_CONSUMPTION = int(instance['INITIALVALUE']), int(instance['BALANCE']), int(instance['CONSUMPTION'])
                    
                    # BALANCE + CONSUPTION <= INITIAL
                    if Q_BALANCE + Q_CONSUMPTION <= Q_INITIAL:
                        quota_volume = Q_INITIAL - Q_BALANCE
                    
                    # BALANCE + CONSUPTION > INITIAL
                    # CONSUMPTION >= INITIAL
                    elif Q_CONSUMPTION >= Q_INITIAL:
                    
                        quota_volume = Q_CONSUMPTION
                        
                        if Q_BALANCE>0:
                           
                           # top-up = BALANCE
                           #dyn_quota['USAGE'] = Q_INITIAL - Q_BALANCE 
                           dyn_quota['VOLUME'] = Q_BALANCE
                        
                    #elif Q_CONSUMPTION < Q_INITIAL:
                    # CONSUMPTION < INITIAL
                    else:
                        quota_volume = Q_CONSUMPTION
                        
                        # top-up = (BALANCE + CONSUMPTION – INITIAL)
                        dyn_quota['VOLUME'] = Q_BALANCE + Q_CONSUMPTION - Q_INITIAL
                        
                    
                    # add prefix to quota name
                    quota['QUOTA'] = quota_prefix+instance['QUOTANAME']
                    quota['USAGE'] = quota_volume*quota_mult
                    
                    self.quota.append(quota)
                    
                    if len(dyn_quota)>0:
                        
                        #dyn_quota['QUOTA'] = dyn_quota['INSTANCE'] = quota_prefix+instance['QUOTANAME']
                        dyn_quota['QUOTA'] = dyn_quota['INSTANCE'] = quota['QUOTA']
                        dyn_quota['INSTANCE'] += str(random.randint(0,100))
                        dyn_quota['VOLUME'] *= quota_mult
                        self.dyn_quota.append(dyn_quota)  
        
        return
    
    def export(self,template_profile,template_quota=None,template_quota_usage=None,template_dyn_quota=None):
        '''
        Export mapped profile into xml using templates
        '''
        
        # generate xml set for custom fields
        xml_custom_result=""
        xml_custom_result="".join([xml_template_custom.format(Custom_Name=attr,Custom_Value=self.profile[attr]) for attr in self.profile if 'Custom' in attr])
        
        # generate xml set for entitlements fields
        xml_ent_result=""
        if upcc_SUBSCRIPTION_mapping['SERVICENAME'] in self.profile:
            xml_ent_result="".join([xml_template_entitlement.format(Entitlement=ent) for ent in self.profile[upcc_SUBSCRIPTION_mapping['SERVICENAME']] ])
        
        xml_profile = template_profile.format(MSISDN = self.profile['MSISDN'],
                                      IMSI = self.profile['IMSI'],
                                      BillingDay = self.profile['BillingDay'],
                                      ENTITLEMENT = xml_ent_result,
                                      CUSTOM = xml_custom_result )
        
        xml_quota,xml_quota_usage="",""
        # if template for quota is defined, then using it. If not then only base profile will be exported
        if template_quota is not None and template_quota_usage is not None and len(self.quota)>0:

#xml_template_quota_usage

            # enumerate counter to start txRequest id from 2
            for i,quota in enumerate(self.quota, start=2):
                
                xml_quota_usage += template_quota_usage.format( #REQ = i,
                                                  QUOTA = quota['QUOTA'],
                                                  USAGE = quota['USAGE']
                                                  )
            
            if template_dyn_quota is not None:
                for quota in self.dyn_quota:
                    xml_quota_usage += template_dyn_quota.format( #REQ = i,
                                      QUOTA = quota['QUOTA'],
                                      VOLUME = quota['VOLUME']
                                      )
                
            xml_quota = template_quota.format( #REQ = i,
                                  IMSI = self.profile['IMSI'],
                                  QUSAGE = xml_quota_usage
                                  )

            # if quota, then construct transaction
#            xml_profile = xml_template_begin_transact + xml_profile
#            xml_quota = xml_quota + xml_template_end_transact 
            
        # concat profile with quota
        return xml_template_begin_transact + xml_profile + xml_quota + xml_template_end_transact

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg

def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by user_name on %s.
  Copyright 2023 organization_name. All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-r", "--recursive", dest="recurse", action="store_true", help="recurse into subfolders [default: %(default)s]")
        parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]", default=0)
        parser.add_argument("-i", "--include", dest="include", help="only include paths matching this regex pattern. Note: exclude is given preference over include. [default: %(default)s]", metavar="RE" )
        parser.add_argument("-e", "--exclude", dest="exclude", help="exclude paths matching this regex pattern. [default: %(default)s]", metavar="RE" )
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
#        parser.add_argument(dest="paths", action='append', help="paths to folder(s) with source file(s) [default: %(default)s]", metavar="path", nargs='*', default=import_dir)
        parser.add_argument(dest="paths", help="paths to folder(s) with source file(s) [default: %(default)s]", metavar="path", nargs='*', default=import_dir)

        # Process arguments
        args = parser.parse_args()

        paths = args.paths            
        
        verbose = args.verbose
        recurse = args.recurse
        inpat = args.include
        expat = args.exclude

        if verbose > 0:
            print("Verbose mode on")
            if recurse:
                print("Recursive mode on")
            else:
                print("Recursive mode off")

        if inpat and expat and inpat == expat:
            raise CLIError("include and exclude pattern are equal! Nothing will be processed.")

        export_records_count = 0
        
        f_out = None
        
        time_start = time.time()
        
        for inpath in paths:
            ### do something with inpath ###
            print("processing "+inpath)
                       
            for inp in next(os.walk(inpath), (None, None, []))[2]:
                
                print("loading: "+inp)
                
                with gzip.open(inpath+"/"+inp, 'rt') as f_inp:
                    while True:
                        
                        f_line = f_inp.readline()
                        
                        if not f_line:
                            break
                
                        if file_begin in f_line:
                            f_begin=True
                            continue
                        
                        elif file_end in f_line:
                            f_begin=False
                            break
                        
                        elif tag_begin in f_line:
                        # prepare for the new subscriber record
                            subscriber_begin=True
                            subscriber_rows=[]
                            continue
                    
                        elif tag_end in f_line:
                        # once subscriber record is ended, flushing it into object
                            if f_begin and subscriber_begin:

                                # create new file on each chunk_size, starting from 0
                                if export_records_count%default_chunk_size == 0:
                                    timestamp = int(time.time()*timestamp_precision)
                                    print("new chunk on: ",export_records_count," : ",filename_prefix+str(timestamp)+filename_suffix)
                                    
                                    # in case new chunk close old file...
                                    if export_records_count>1:
                                        f_out.close()
                                    
                                    # and start a new one
                                    f_out = gzip.open(output_dir+filename_prefix+str(timestamp)+filename_suffix, 'at')
                                
                                export_records_count+=1
                                
                                # Extract
                                subs = UPCC_Subscriber (subscriber_rows)
                                if verbose>0:
                                    print (json.dumps(subs.attrs, indent=2, default=str))
                                
                                # Transform
                                subs.mapping()

                                # Load
                                xml_result =subs.export(xml_template['create_subs'],xml_template['create_quota'],xml_template['quota_usage'],xml_template['topup_quota'])
                                #xml_result =subs.export(xml_template['create_subs'])
                                if verbose>0: 
                                    print (xml_result)

                                f_out.write("%s\n" % xml_result)
                            
                            # wait for next subs record
                            subscriber_begin=False
                        else:
                            
                        # accumulating rows into list
                            subscriber_rows.append(f_line)
                    
#                            print("loaded elements: "+str(subs.elements()))

        print("Total records: ",export_records_count)
        print("Execution time: ", str(timedelta(seconds=time.time() - time_start)))
        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception as e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == "__main__":
    if DEBUG:
#        sys.argv.append("-h")
        sys.argv.append("-v")
        sys.argv.append("-r")
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = 'upcc_import_profile.txt'
        cProfile.run('main()', profile_filename)
#        statsfile = open("profile_stats.txt", "wb")
#        p = pstats.Stats(profile_filename, stream=statsfile)
        p = pstats.Stats(profile_filename)
#        stats = p.strip_dirs().sort_stats('cumulative')
#        p.sort_stats("time", "name").print_stats()
        p.sort_stats("cumulative").print_stats(20)
#        stats.print_stats()
#        statsfile.close()
        sys.exit(0)
    sys.exit(main())
    