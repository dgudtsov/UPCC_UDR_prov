#!/usr/local/bin/python2.7
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

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

import json

__all__ = []
__version__ = 0.1
__date__ = '2023-01-05'
__updated__ = '2023-01-05'

DEBUG = 0
TESTRUN = 0
PROFILE = 0

default_chunk_size=10000000

import_dir='./import'

output_dir='./output/'
filename_prefix='i_'
filename_suffix='.ixml.gz'

export_result = 'export.csv.gz'

fields_names={
    'QUOTA':("QUOTANAME","QUOTAID","INITIALVALUE","BALANCE","CONSUMPTION","STATUS","LASTRESETDATETIME","NEXTRESETDATETIME","RESETTYPE","CYCLETYPE","CUSTOMLEVEL1","CUSTOMLEVEL2","CUSTOMLEVEL3","CUSTOMSTATUS","CUMULATEINDICATOR","PREUPLOAD","PREDOWNLOAD","PRECONSUMPTION", "QUOTAFLAG","UPDATEDTIME","QUOTAUNIT","CURCUMTIMES","ACCUMBVALUE")
    ,'SUBSCRIPTION':("SERVICENAME","SERVICEID","STATUS","SUBSCRIBEDATETIME","VALIDFROMDATETIME","EXPIREDATETIME","ACTIVATIONSTATUS","SHAREFLAG","SVCPKGID","ROAMTYPE","SUBSCRIBEDTYPE","VALIDPERIOD","REDIRECTTIME","REDIRECTURLID","SRVSTATUS","FLAG","CONTACTMETHOD","USEDFLAG","SERVICEBILLINGTYPE","NOTIFICATIONCYCLE","ACTSTARTDATETIME","ACTENDDATETIME","RESTTIME","MILLISECOND")
    ,'PKGSUBSCRIPTION':("PKGNAME","PKGID","SUBSCRIBEDATETIME","VALIDFROMDATETIME","EXPIREDATETIME","ROAMTYPE","CONTACTMETHOD")
}

class UPCC_Subscriber(object):
    def __init__(self,fname):
        self.fname = fname
        
        # stores original UPCC fields in key-value pairs
        # the following attrs are used as list (even having single value):
        # 'SUBSCRIBERGRPNAME','SUBSCRIPTION','PKGSUBSCRIPTION','QUOTA','ACCOUNT'
        self.attrs=dict()
        
        with open(self.fname,'r') as f_inp:
            for f_line in f_inp:
                
                if "<SUBBEGIN" in f_line:
                    self.subscriber_begin=True
                    continue
                
                elif "<SUBEND" in f_line:
                    self.subscriber_begin=False
                    break

                f_line_str = f_line.strip().rstrip(';')
                
                if DEBUG:
                    print (f_line)
                    print (f_line_str)

                if self.subscriber_begin:
                    (s_key,s_value) = (f_line_str.split('=')[0], f_line_str.split('=')[1])

                    # list of attributes with multiple occurence
                    if s_key in ('SUBSCRIBERGRPNAME','SUBSCRIPTION','PKGSUBSCRIPTION','QUOTA','ACCOUNT'):
                        if s_key in self.attrs:
                            self.attrs[s_key].append(s_value)
                        else:
                            self.attrs.update({s_key: [s_value]})
                    else:
                        self.attrs.update({s_key: s_value})
        
        # iterate over only those fields which are defined
        for field in fields_names.keys():
            # if field is defined in source and contain values (is not empty)
            if field in self.attrs:
                if len(self.attrs[field]) >0:
                    self.__unpack_field__(field)        
        
        return
    
    # transforms string representation like:
    #     QUOTA=413102-DATA_D_Quota&E7242330DC433136&1024&0&0&6&20221031102855&FFFFFFFFFFFFFF&0&255&0&0&0&0&0&0&0&0&0&1667190535&1;
    # into dict structure
    def __unpack_field__(self,field):

        for index,entity_string in enumerate(self.attrs[field]):
            entity_fields = entity_string.split("&")
            
            # for case when number of fields parsed less than fields were defined
            for i in range(0,len(fields_names[field])-len(entity_fields)):
                entity_fields.append(None) 
            
            entity_dict = dict()
            
            # min function is safe here
            entity_dict = {fields_names[field][i]: entity_fields[i] for i in range(0, min(len(entity_fields),len(fields_names[field])))}
            
            self.attrs[field][index] = entity_dict

        return
    
    def elements(self):
        return len(self.attrs.keys())

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

        for inpath in paths:
            ### do something with inpath ###
            print("processing "+inpath)
                       
            for inp in next(os.walk(inpath), (None, None, []))[2]:
                print("loading: "+inp)
                subs = UPCC_Subscriber (inpath+"/"+inp)
                print("loaded elements: "+str(subs.elements()))
                
                if DEBUG:
                    print (json.dumps(subs.attrs, indent=2, default=str))
            
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
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())