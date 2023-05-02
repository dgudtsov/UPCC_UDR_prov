#!/usr/local/bin/python3
# encoding: utf-8
'''
upcc_import -- converter from Huawei UPCC export files into Oracle UDR bulk import file

upcc_import is a description

It defines classes_and_methods

@author:     Denis Gudtsov

@copyright:  2023 Jet Infosystems. All rights reserved.

@license:    Apache

@contact:    user_email
@deffield    updated: Updated
'''

import sys
import os
import time
import gzip

import pickle

import logging
from logging import StreamHandler, Formatter, FileHandler, handlers

from datetime import timedelta

import random

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

import json

from upcc_import_template import *
from upcc_pkgsubscription import pkgsubscription
from upcc_servicequota import servicequota
from asyncio.log import logger

__all__ = []
__version__ = 0.5
__date__ = '2023-01-05'
__updated__ = '2023-05-02'

DEBUG = 0
TESTRUN = 0
PROFILE = 0

timestamp_precision = 10000

time_delta = 10

default_chunk_size=1000000
#default_chunk_size=500000

#import_dir='./import'
import_dir='./csv'

# only files ends with this suffix will be imported
input_file_suffix='.txt.gz'

default_output_dir='./output/'

filename_prefix='i_'
filename_suffix='.ixml.gz'

filename_prefix_pool='i_pool_'

export_result = 'export.csv.gz'

#logging
logFilePath = "./log/export.log"
maxBytes=50000000 
backupCount=10

# persistent storage
stor_sid_imsi="./persistent/sid_imsi"

stor_imsi_pool="./persistent/imsi_pool"

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
# fake SID field for master/slave mapping
'SID':'SID',
'MSISDN':'MSISDN',
'SUBSCRIBERIDENTIFIER':'IMSI',
'STATION':'Custom20',
#'USRMASTERIDENTIFIER':'Custom20',
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

upcc_STATION_mapping = {
    '1' : 'Master',
    '2' : 'Slave'
    }

SRVSTATUS_Frozen = 1

upcc_SUBSCRIPTION_mapping = {
    'SERVICENAME' : 'Entitlement',
    'SRVSTATUS_Frozen' : 'Custom18'
    }

# hash for master to slave mapping
SID_IMSI = {
#    'SID' : 'IMSI'
    }

# list to map master IMSI to pool
IMSI_Pool = list()
#IMSI_Pool = {
# 'IMSI' : Pool object    
#    }

#master quota prefix
master_quota_prefix='CLONE-'

# quota prefix to be added
quota_prefix='T2-'

# quota size multiplier
quota_mult = 1000

# global errors counter
errors_count = 0

# counters
MSISDN_min=77999999999
MSISDN_max=0
IMSI_min=401779999999999
IMSI_max=0

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
        
        self.logger = logging.getLogger(__name__)
        
        self.__is_master__=False


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
    
    def has_master(self):
        '''
        Returns True if subs is slave and master is found (IMSI is known)
        Returns False if subs is slave and master is NOT found (IMSI is unknown)
        Returns False if subs is Master
        '''
        
        # if mapping to imsi is not done, i.e. still value of Master or Slave
        if self.profile[upcc2profile_mappings['STATION']] in list(upcc_STATION_mapping.values()):
            return False
        else:
            return True
        
        # default
        return False
    
    def is_master(self):
        '''
        Returns True if this subscriber is master and has one of 'Clone-*' quotas is defined OR subscription with 'Clone-*' is defined  
        '''
        return self.__is_master__
    
    def get_master(self):
        '''
        Returns IMSI of Master
        '''
    
        # if Master
        if self.profile[upcc2profile_mappings['STATION']] == upcc_STATION_mapping['1']:
            return self.profile['IMSI']
        
        # if Slave
        return self.profile[upcc2profile_mappings['STATION']]
    
    def mapping(self):
        '''
        # internal structure:
        # self.profile - dict, containing base profile attributes: MSISDN, IMSI, Ent, Tier, CustomX, etc.
        # example: {'IMSI': '401771121496414', 'MSISDN': '77089675915', 'Custom3': '0', 'Custom6': '0', 'Custom7': '0', 'Custom8': '0'}
        # self.quota - list of dicts, each dict contains: quota name and quota usage
        # example: [{'QUOTA': '409239-DATA_D_Quota', 'VOLUME': '65013247'}, {'QUOTA': '40777900081-DATA_D_Quota', 'USAGE': '865069'}, {'QUOTA': '413102-DATA_D_Quota', 'USAGE': '0'}]
        '''
        
        global errors_count
        
        # map all attributes were defined in upcc2profile_mappings
        [ self.profile.update({upcc2profile_mappings[k]:self.attrs[k]}) for k in self.attrs if k in upcc2profile_mappings ]
        
        if self.profile[upcc2profile_mappings['STATION']] in upcc_STATION_mapping:
            self.profile[upcc2profile_mappings['STATION']] = upcc_STATION_mapping[self.profile[upcc2profile_mappings['STATION']]]
            
            # if slave
            if self.profile[upcc2profile_mappings['STATION']] == upcc_STATION_mapping['2']:
                if self.profile[upcc2profile_mappings['SID']] in SID_IMSI:
                    self.profile[upcc2profile_mappings['STATION']] = SID_IMSI[self.profile[upcc2profile_mappings['SID']]]
                    self.logger.debug('Slave = Master SID = %s', self.profile[upcc2profile_mappings['SID']])
                    self.logger.debug('Profile: %s', json.dumps(self.profile, indent=2, default=str))
                else:
                    self.logger.error('Slave has no Master: SID = %s', self.profile[upcc2profile_mappings['SID']])
                    self.logger.debug('Profile: %s', json.dumps(self.profile, indent=2, default=str))
                    errors_count+=1
            
            # skip slaves without master
                    return False
            
        
        # BillingDay normalization
        try:
            if int(self.profile['BillingDay'])<0 or int(self.profile['BillingDay'])>31:  
                raise ValueError
        except:
            self.profile['BillingDay'] = 0

        # skip subs if there are no mandatory fields are refined
        if 'IMSI' not in self.profile or 'MSISDN' not in self.profile:
            
            self.logger.error('IMSI or MSISDN is missing for profile, SID=%s', self.profile[upcc2profile_mappings['SID']])
            self.logger.debug('Profile: %s', json.dumps(self.profile, indent=2, default=str))
            
            errors_count+=1
                        
            return False        
        
        # populate SID_IMSI dict with masters
        if self.profile[upcc2profile_mappings['STATION']] == upcc_STATION_mapping['1'] :
            if self.profile[upcc2profile_mappings['SID']] not in SID_IMSI:
                SID_IMSI[self.profile[upcc2profile_mappings['SID']]] = self.profile[upcc2profile_mappings['SUBSCRIBERIDENTIFIER']]
            else:
                self.logger.error("Duplicate SID-IMSI pair: SID=%s, IMSI=%s",self.profile[upcc2profile_mappings['SID']],self.profile[upcc2profile_mappings['SUBSCRIBERIDENTIFIER']])
        

        
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
                    
                    # as subscriber has SUBSCRIPTION=CLONE-* then assign them master marker
                    if subscription['SERVICENAME'].startswith(master_quota_prefix) and self.profile[upcc2profile_mappings['STATION']] == upcc_STATION_mapping['1'] :
                        self.__is_master__ = True
                    
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
                    
                    # if subs is master the store imsi in hash sid-imsi
#                    if instance['QUOTANAME'].startswith(master_quota_prefix) and self.profile[upcc2profile_mappings['STATION']] == upcc_STATION_mapping['1'] :
#                        SID_IMSI[self.profile[upcc2profile_mappings['SID']]] = self.profile[upcc2profile_mappings['SUBSCRIBERIDENTIFIER']]

                    if instance['QUOTANAME'].startswith(master_quota_prefix) and self.profile[upcc2profile_mappings['STATION']] == upcc_STATION_mapping['1'] :
                        self.__is_master__ = True
#                    if instance['QUOTANAME'].startswith(master_quota_prefix): self.__is_master__ = True
                    
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
                    quota['VOLUME'] = quota_volume*quota_mult
                    
                    self.quota.append(quota)
                    
                    if len(dyn_quota)>0:
                        
                        #dyn_quota['QUOTA'] = dyn_quota['INSTANCE'] = quota_prefix+instance['QUOTANAME']
                        dyn_quota['QUOTA'] = dyn_quota['INSTANCE'] = quota['QUOTA']
#                        dyn_quota['INSTANCE'] += str(random.randrange(100000,999999))
                        dyn_quota['VOLUME'] *= quota_mult
                        self.dyn_quota.append(dyn_quota)  
        
        return True
    
    def generate_quota(self,quota,template_quota=None,template_quota_usage=None):
        '''
        Prepare xml quota from template for static or dynamic quotas
        '''
        
        xml_quota = xml_quota_usage = ""
        
        # if template for quota is defined, then using it. If not then only base profile will be exported
        if template_quota is not None and template_quota_usage is not None and len(quota)>0:

            # enumerate counter to start txRequest id from 2
            for i,q in enumerate(quota, start=2):
                xml_quota_usage += template_quota_usage.format( #REQ = i,
                                                  QUOTA = q['QUOTA'],
                                                  VOLUME = q['VOLUME'],
                                                  INSTANCE = q['QUOTA']+"_"+str(random.randrange(100000,999999))
                                                  # InstanceId = <QNAME>_RAND(6) 
                                                  )
            
            xml_quota = template_quota.format( #REQ = i,
                                  IMSI = self.profile['IMSI'],
                                  MASTER = self.profile[upcc2profile_mappings['STATION']],
                                  QUOTA = xml_quota_usage
                                  )
        return xml_quota
    
    def export_quota(self,quota,template_quota=None,template_quota_usage=None):
        '''
        Universal mapping for quotas: static and dynamic
        '''
        
        if template_quota is not None and template_quota_usage is not None and len(quota)>0:
            return self.generate_quota(quota,template_quota,template_quota_usage)
        return ""
    
    #def export_profile(self,template_profile,template_quota=None,template_quota_usage=None,template_dquota=None,template_dyn_quota=None):
    def export_profile(self,template_profile):
        '''
        Export mapped profile into xml using templates
        '''
        
        global errors_count
        
        # generate xml set for custom fields
        xml_custom_result=""
        xml_custom_result="".join([xml_template_custom.format(Custom_Name=attr,Custom_Value=self.profile[attr]) for attr in self.profile if 'Custom' in attr])
        
        # generate xml set for entitlements fields
        xml_ent_result=""
        if upcc_SUBSCRIPTION_mapping['SERVICENAME'] in self.profile:
            xml_ent_result="".join([xml_template_entitlement.format(Entitlement=ent) for ent in self.profile[upcc_SUBSCRIPTION_mapping['SERVICENAME']] ])
        
        try:
            xml_profile = template_profile.format(
                                    MSISDN = self.profile['MSISDN'],
                                    IMSI = self.profile['IMSI'],
                                    BillingDay = self.profile['BillingDay'],
                                    ENTITLEMENT = xml_ent_result,
                                    CUSTOM = xml_custom_result,
                                    MASTER = self.profile[upcc2profile_mappings['STATION']]
                                    )
        except:
            self.logger.error('Key error for profile, SID=%s', self.profile[upcc2profile_mappings['SID']])
            self.logger.debug('Profile: %s', json.dumps(self.profile, indent=2, default=str))
            
            errors_count+=1
            
            return ""
        
        return xml_profile

        # xml_quota = self.export_quota(self.quota,template_quota,template_quota_usage) 
        # xml_dyn_quota = self.export_quota (self.dyn_quota,template_dquota,template_dyn_quota)
        # xml_quota_usage = ""
        #
        # # if template for quota is defined, then using it. If not then only base profile will be exported
        # if template_quota is not None and template_quota_usage is not None and len(self.quota)>0:
        #     xml_quota = self.generate_quota(self.quota,template_quota,template_quota_usage)
        #
        # if template_dquota is not None and template_dyn_quota is not None and len(self.dyn_quota)>0:
        #     xml_dyn_quota = self.generate_quota(self.dyn_quota,template_dquota,template_dyn_quota)
        #
        # # concat profile with quotas
        # return xml_template_begin_transact + xml_profile + xml_quota + xml_dyn_quota + xml_template_end_transact


class Pool(UPCC_Subscriber):
#    def __init__(self, master):
    def __init__(self):
        '''
        Create new pool class
#        Input: imsi of master subscriber
        '''

        # stores mapped quota parameters         
        self.quota = list()

        # top-up modifiers
        self.dyn_quota = list()

        # stores mapped udr profile
        self.profile=dict()
        
        # Entitlements
        self.profile[upcc_SUBSCRIPTION_mapping['SERVICENAME']] = list()
        
#        # assign IMSI of master to Pool ID
        self.profile['IMSI'] = ""
        self.profile['MSISDN'] = ""
        self.profile['BillingDay'] = 0
        
        
        self.logger = logging.getLogger(__name__)
        
        pass
    
    def mapping(self, subs):

    ### Transfer values from Subscriber profile to Pool profile with Clone-* prefix
        # store to Entitlement only items with Clone-* prefix        
        self.profile[upcc_SUBSCRIPTION_mapping['SERVICENAME']] = [ ent for ent in subs.profile[upcc_SUBSCRIPTION_mapping['SERVICENAME']] if master_quota_prefix in ent]

        # quota from subs to pool
        self.quota = [ quota for quota in subs.quota if quota['QUOTA'].startswith(quota_prefix+master_quota_prefix) ]
        
        # dynquota from subs to pool
        self.dyn_quota = [ dyn_quota for dyn_quota in subs.dyn_quota if dyn_quota['QUOTA'].startswith(quota_prefix+master_quota_prefix)]
    ###
        
        # just to keep continuance
        self.profile[upcc2profile_mappings['SID']] = subs.profile[upcc2profile_mappings['SID']]
        
        
        self.profile['IMSI'] = subs.profile['IMSI']
        
        self.profile[upcc2profile_mappings['STATION']] = subs.get_master()
        
        
        # if master
#        if subs.profile[upcc2profile_mappings['STATION']] in list(upcc_STATION_mapping.values()):
#            self.profile[upcc2profile_mappings['STATION']] = subs.profile['IMSI'] 
        
#        xml_ent_result="".join([ ent for ent in subs.profile[upcc_SUBSCRIPTION_mapping['SERVICENAME']] if master_quota_prefix in ent])
        
#        print (xml_ent_result)
        
        return
    
    def add_slave(self,subscriber):
        '''
        Add slave subscriber into pool
        Input: UPCC_Subscriber instance of class
        '''
        
        # add CLONE-* subscription to pool
        
        # add CLONE-* quota to pool
        
        pass

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

    global errors_count
    
    global SID_IMSI
    global IMSI_Pool
    
    global MSISDN_min
    global MSISDN_max
    global IMSI_min
    global IMSI_max
    
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

  Created by Denis Gudtsov on %s.
  Copyright 2023 Jet Infosystems. All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
        # parser.add_argument("-r", "--recursive", dest="recurse", action="store_true", help="recurse into subfolders [default: %(default)s]")
        parser.add_argument("-v", "--verbose", dest="verbose", action="count", help="set verbosity level [default: %(default)s]", default=0)
        # parser.add_argument("-i", "--include", dest="include", help="only include paths matching this regex pattern. Note: exclude is given preference over include. [default: %(default)s]", metavar="RE" )
        # parser.add_argument("-e", "--exclude", dest="exclude", help="exclude paths matching this regex pattern. [default: %(default)s]", metavar="RE" )
        parser.add_argument('-V', '--version', action='version', version=program_version_message)
        
        parser.add_argument("-f", "--format", dest="format", required=False, choices=['csv','raw'], help="format: csv or raw, [default: %(default)s]", default='csv')
        
        parser.add_argument("-a", "--action", dest="action", required=False, choices=['create','delete'], help="action: create or delete, [default: %(default)s]", default='create')

        parser.add_argument("-o", "--output", dest="output_dir", help="output directory [default: %(default)s]", default=default_output_dir)
        
        parser.add_argument("-t", "--test", dest="test", action="count", help="test import, without writing output result [default: %(default)s]", default=None)
        
#        parser.add_argument(dest="paths", action='append', help="paths to folder(s) with source file(s) [default: %(default)s]", metavar="path", nargs='*', default=import_dir)
        parser.add_argument(dest="paths", help="paths to folder(s) with source file(s) [default: %(default)s]", metavar="path", nargs='*', default=import_dir)

        # Process arguments
        args = parser.parse_args()

        paths = args.paths            
        
        verbose = args.verbose
        # recurse = args.recurse
        # inpat = args.include
        # expat = args.exclude
        format = args.format
        test = args.test
        action = args.action
        
        output_dir = args.output_dir
        
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        
        if test:
            formatter = logging.Formatter('[%(asctime)s :TEST-RUN: %(levelname)s] %(message)s')
        else:
            formatter = logging.Formatter('[%(asctime)s: %(levelname)s] %(message)s')
        
        #console
        handler = StreamHandler(stream=sys.stdout)
        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)
        logger.propagate=False
        
        #file
#        file_handler = handlers.TimedRotatingFileHandler(filename=logFilePath, when='midnight', backupCount=10)
        file_handler = handlers.RotatingFileHandler(filename=logFilePath, maxBytes=maxBytes , backupCount=backupCount)

        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)

        logger.addHandler(file_handler)
        logger.addHandler(handler)
        
        logger.handlers[0].doRollover()
        
        logger.info('Application started')
        logger.info('Logging output to: %s',logFilePath)
        
        if test:
            logger.info('TEST run, without writing result')
        else:
            logger.info('Output to: %s',output_dir)
        
        logger.info('Loading persistent SID_IMSI from: %s', stor_sid_imsi)
#        logger.info('Loading persistent from: %s', stor_imsi_pool)
        
        try:
            with gzip.open(stor_sid_imsi + '.pickle', 'rb') as f:
                SID_IMSI = pickle.load(f)
        except:
            logger.info('file is not found')
        
        if verbose > 0:
            logger.info("Verbose mode on")
            # if recurse:
            #     logger.info("Recursive mode on")
            # else:
            #     logger.info("Recursive mode off")

        # if inpat and expat and inpat == expat:
        #     raise CLIError("include and exclude pattern are equal! Nothing will be processed.")

        export_records_count = 0
        
        f_out = None
        
        time_start = previous_time = time.time()

        timestamp = int(time.time()*timestamp_precision)
        
        logger.info("pool dumping to: "+filename_prefix_pool+str(timestamp)+filename_suffix)
        if not test:
            f_pool = gzip.open(output_dir+filename_prefix_pool+str(timestamp)+filename_suffix, 'at')
        
        for inpath in paths:
            ### do something with inpath ###
#            print("processing "+inpath)
            logger.info("processing "+inpath)
                       
#            for inp in next(os.walk(inpath), (None, None, []))[2]:

            for path, directories, files in os.walk(inpath):
                files.sort()
                for inp in files:
                    if inp.endswith(input_file_suffix):                
                
                        logger.info("loading: "+inp)
                        
                        with gzip.open(inpath+"/"+inp, 'rt') as f_inp:
                            
                            while True:
                                
                                f_line = f_inp.readline()
                                
                                if not f_line:
                                    break
                                
                                if format == 'csv':
                                    
                                    pass
                                    subscriber_rows = f_line.split('\t')
                                    
                                    f_begin = subscriber_begin = subscriber_end = True
                                
                                else:
                        
                                    if file_begin in f_line:
                                        f_begin=True
                                        continue
                                    
                                    elif file_end in f_line:
                                        f_begin=False
                                        break
                                    
                                    elif tag_begin in f_line:
                                    # prepare for the new subscriber record
                                        subscriber_begin=True
                                        subscriber_end=False
                                        subscriber_rows=[]
                                        continue
                                
                                    elif tag_end in f_line:
                                        subscriber_end=True
                                        
                                    else:
                                    # accumulating rows into list
                                        subscriber_rows.append(f_line)
                                                                        
                                        
                            # once subscriber record is ended, flushing it into object
                                if f_begin and subscriber_begin and subscriber_end:
        
                                    # create new file on each chunk_size, starting from 0
                                    if export_records_count%default_chunk_size == 0:
                                        
                                        if export_records_count > 0:
                                            logger.info("%s records processed: %s",str(default_chunk_size), str(timedelta(seconds=time.time() - int(timestamp / timestamp_precision))))
                                            logger.info("records per second: %s",str(int(default_chunk_size / (time.time() - int(timestamp / timestamp_precision)))))
                                        
                                        timestamp = int(time.time()*timestamp_precision)
                                        logger.info("new chunk on: "+str(export_records_count)+" : "+filename_prefix+str(timestamp)+filename_suffix)
                                        
                                        # in case new chunk close old file...
                                        if export_records_count>1 and not test:
                                            f_out.close()
                                        
                                        if not test:
                                        # and start a new one
                                            f_out = gzip.open(output_dir+filename_prefix+str(timestamp)+filename_suffix, 'at')
                                    
                                    if len(subscriber_rows)>0:
                                    
                                        export_records_count+=1
                                        
                                        
                                         
                                        if time.time()  - previous_time  > time_delta:
                                            previous_time = time.time()
                                            logger.info("processed records: %s", str(export_records_count))
                                        
                                        # Extract
                                        subs = UPCC_Subscriber (subscriber_rows)
                                        if verbose>0:
                                            logger.debug(json.dumps(subs.attrs, indent=2, default=str))
                                        
                                        # Transform
                                        if subs.mapping() :
                                            
                                            if int(subs.profile['IMSI']) > IMSI_max: IMSI_max = int(subs.profile['IMSI'])  
                                            if int(subs.profile['IMSI']) < IMSI_min: IMSI_min = int(subs.profile['IMSI'])
                                        
                                            if int(subs.profile['MSISDN']) > MSISDN_max: MSISDN_max = int(subs.profile['MSISDN'])  
                                            if int(subs.profile['MSISDN']) < MSISDN_min: MSISDN_min = int(subs.profile['MSISDN'])
            
                                            # Load
                                            # xml_template_begin_transact + xml_profile + xml_quota + xml_dyn_quota + xml_template_end_transact
                                            xml_result = xml_template_begin_transact
                                            xml_result_pool = ""
                                            
                #                                xml_result += subs.export_profile(xml_template['create_subs'],xml_template['create_quota'],xml_template['quota_usage'],xml_template['create_dquota'],xml_template['topup_quota'])
                                            
                                            if action == 'delete':
                                                xml_result += subs.export_profile(xml_template['delete_subs'])
                                            else:
                                            
                                                xml_result += subs.export_profile(xml_template['create_subs'])
# TODO: remove quota with clone- prefix                                             
                                                xml_result += subs.export_quota(subs.quota, xml_template['create_quota'], xml_template['quota_usage'])
                                                xml_result += subs.export_quota(subs.dyn_quota, xml_template['create_dquota'], xml_template['topup_quota'])
                                            
                                            # if subs is master, then create pool
                                            if subs.is_master():
                                                
                                                #pool = Pool(subs.get_master())
                                                pool = Pool()
                                                pool.mapping(subs)
                                                
                                                #IMSI_Pool[subs.get_master()] = "1"
                                                IMSI_Pool.append(subs.get_master())
                                                
                                                if action == 'create':
                                                    
                                                    xml_result_pool = xml_template_begin_transact
                                                    
                                                    # create pool and add master as first member
                                                    xml_result_pool += pool.export_profile(xml_template['create_pool'])
                                                    
                                                    xml_result += pool.export_profile(xml_template['pool_member_master'])
                                                    
                                                    xml_result_pool += pool.export_quota(pool.quota, xml_template['pool_quota'], xml_template['quota_usage'])
                                                    xml_result_pool += pool.export_quota(pool.dyn_quota, xml_template['pool_dquota'], xml_template['topup_quota'])
                                                
                                                    xml_result_pool += xml_template_end_transact
                                                
                                            # if subs is slave, add him into pool
                                            elif subs.has_master():
                                                #pool = Pool(subs.get_master())
                                                pool = Pool()
                                                pool.mapping(subs)
                                                
                                                # check if pool has not been created for master, then create it from slave
                                                # the only issue is: slave doesn't has SUBSCRIPTION=CLONE-*
                                                
                                                if subs.get_master() not in IMSI_Pool:
                                                    logger.warning("Creating Pool from Slave SID = %s ", str(subs.profile[upcc2profile_mappings['SID']]))

                                                    xml_result_pool = xml_template_begin_transact

                                                    if action == 'delete':
                                                        xml_result_pool += pool.export_profile(xml_template['delete_pool'])
                                                    else:                                                    
                                                        # create pool and add master as first member
                                                        xml_result_pool += pool.export_profile(xml_template['create_pool'])
                                                        
                                                        xml_result += pool.export_profile(xml_template['pool_member_master'])
                                                        
                                                        #IMSI_Pool[subs.get_master()] = "1"
                                                        IMSI_Pool.append(subs.get_master())
                                                        
                                                    xml_result_pool += xml_template_end_transact
                                                
                                                if action == 'create':
                                                    xml_result += pool.export_profile(xml_template['pool_member'])
                                                    
                                                    # virtual quotas on slave, QUOTAFLAG = 1
                                                    # # Pool Quota modifiers from slaves ?
                                                    # pool_quota = ""
                                                    # pool_quota += pool.export_quota(pool.quota, xml_template['pool_quota'], xml_template['quota_usage'])
                                                    # pool_quota += pool.export_quota(pool.dyn_quota, xml_template['pool_dquota'], xml_template['topup_quota'])
                                                    #
                                                    # if len(pool_quota)>0:
                                                    #     xml_result_pool = xml_template_begin_transact + pool_quota + xml_template_end_transact 
                                                
# TODO: check virtual quota

                                            
                                            xml_result += xml_template_end_transact
                                            
                                            #xml_result =subs.export(xml_template['create_subs'])
                                            if verbose>0: 
                                                logger.debug (xml_result)
                                                logger.debug (xml_result_pool)
                
                                            if not test:
                                                f_out.write("%s\n" % xml_result)
                                                
                                                if len(xml_result_pool)>0:
                                                    f_pool.write("%s\n" % xml_result_pool)
                                                
                                            # # Pool
                                            # if subs.has_master():
                                            #     master_imsi = subs.get_master()
                                            #
                                            #     if master_imsi not in IMSI_Pool:
                                            #         IMSI_Pool[master_imsi] = Pool(subs.get_master())
                                            #
                                            #     pool = IMSI_Pool[master_imsi]
                                            #
                                            #     pool.add_slave(subs)
                                
                                # wait for next subs record
                                subscriber_begin=False
                                subscriber_end=False
        
                                
            #                            print("loaded elements: "+str(subs.elements()))

        #
        # timestamp = int(time.time()*timestamp_precision)
        # logger.info("pool dumping to: "+filename_prefix+str(timestamp)+filename_suffix)
        #
        # if not test:
        # # and start a new one
        #     f_pool = gzip.open(output_dir+filename_prefix_pool+str(timestamp)+filename_suffix, 'at')
        #
        # for imsi,p in enumerate(IMSI_Pool):
        #     logger.debug("Pool Master IMSI: "+str(imsi))
        #
        #     xml_result = xml_template_begin_transact
        #
        #     xml_result = p.export(xml_template['create_pool'])
        #
        #     xml_result += xml_template_end_transact
        #
        #     if verbose>0: 
        #         logger.debug (xml_result)
        #
        #     if not test:
        #         f_pool.write("%s\n" % xml_result)
        #
        if not test:
            f_pool.close()
        
        logger.info("Total records: "+str(export_records_count))
        logger.info("SID_IMSI records: "+str(len(SID_IMSI)))
        logger.info("Pools records: "+str(len(IMSI_Pool)))
        
        logger.info("MSISDN range: %s - %s", str(MSISDN_min), str(MSISDN_max))
        logger.info("IMSI range: %s - %s", str(IMSI_min), str(IMSI_max))
        
        logger.info("Execution time: " + str(timedelta(seconds=time.time() - time_start)))
        logger.info("Total errors: %s check log file at %s",str(errors_count),logFilePath)
        
        logger.info('Storing persistent SID_IMSI to: %s', stor_sid_imsi)
        
        with gzip.open(stor_sid_imsi+'.pickle', 'wb') as f:
            pickle.dump(SID_IMSI, f)
        
        logger.info('Done, exiting')
        
        # with open(stor_imsi_pool+'.pickle', 'wb') as f:
        #     pickle.dump(IMSI_Pool, f)
        
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
    