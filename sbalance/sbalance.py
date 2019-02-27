#!/usr/bin/env python
# Copyright (c) 2019 Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>,
#
# sbalance: query remaining slurm billing balance
from __future__ import print_function

import getpass
import subprocess
import argparse
import math
import csv

from pprint import pprint
from collections import OrderedDict

from .config import __version__, __author__, __license__

Verbosity = type('Verbosity', (), {'INFO':1, 'WARNING':2, 'DEBUG':5})

DEBUG = False

SACCT_COMMAND = 'sacct'
SACCT_BEGIN_DATE = '01/01/19'
SACCT_USAGE_FIELDS = ('jobid', 'user', 'account','qos','state','alloctres','elapsedraw','partition')
SACCT_USAGE_STATES = ('CD',     # COMPLETED
                      'F',      # FAILED
                      'TO',      # TIMEOUT
                      'CA'
)

SACCTMGR_COMMAND = 'sacctmgr'
SACCTMGR_QOS_FIELDS = ('name','grptresmins','flags','description')
SACCTMGR_QOS_NODECAY_FLAG = 'NoDecay'
SACCTMGR_ASSOC_FIELDS = ('account','user','qos', 'defaultqos')

__verbose_print = None

def query_qos():
    qos_info = {}
    qos_cmd = [SACCTMGR_COMMAND,
               '--noheader','show', 'qos','-P',
               'format=' + ','.join(SACCTMGR_QOS_FIELDS)
    ]

    __verbose_print('[SLURM]: ' + ' '.join(qos_cmd), level=Verbosity.INFO)

    # Query all available QoS
    qos_output_raw = subprocess.check_output(qos_cmd, universal_newlines=True).splitlines()
    qos_output = csv.DictReader(qos_output_raw, fieldnames=SACCTMGR_QOS_FIELDS, delimiter='|')

    for qos in qos_output:
        # Check only QoS with NoDecay flags
        if SACCTMGR_QOS_NODECAY_FLAG in qos['flags'].split(','):
            qos_info[qos['name']] = qos
            qos_info[qos['name']]['grptresmins'] = {k:int(v) for k,v in (x.split('=') for x in qos['grptresmins'].strip().split(','))}

    __verbose_print(qos_info, level=Verbosity.DEBUG)

    return qos_info

def query_assocs(qos_info, user_list=None, verbose=0):
    assoc_info = OrderedDict()
    assoc_cmd = [SACCTMGR_COMMAND,
                   '--noheader', 'show', 'assoc','-P',
                   'format=' + ','.join(SACCTMGR_ASSOC_FIELDS)
    ]

    __verbose_print('[SLURM]: ' +' '.join(assoc_cmd), level=Verbosity.INFO)

    # Assume user only have one QoS
    assoc_output_raw = subprocess.check_output(assoc_cmd, universal_newlines=True).splitlines()
    assoc_output = csv.DictReader(assoc_output_raw, fieldnames=SACCTMGR_ASSOC_FIELDS, delimiter='|')
    
    for assoc in assoc_output:
        if not assoc['user']:
            continue

        if not user_list or assoc['user'] in user_list:
            qos_list = assoc['qos'].split(',')

            for qos in qos_list:
                if qos in qos_info:
                    assoc_info[(assoc['account'], qos)] = qos_info[qos]
                else:
                    assoc_info[(assoc['account'], qos)] = {}
                
                # Has default QoS
                if len(qos_list) > 1:
                    assoc_info[(assoc['account'], qos)]['default'] = (qos == assoc['defaultqos'])
                else:
                    assoc_info[(assoc['account'], qos)]['default'] = None

    __verbose_print(assoc_info, level=Verbosity.DEBUG)

    return assoc_info

def query_usage(assoc_list=None, verbose=0):
    usage_info = {}
    usage_cmd = [SACCT_COMMAND,
                 '-aPX', '--noheader', '--noconvert',
                 '--format=' + ','.join(SACCT_USAGE_FIELDS),
                 #'--state=' + ','.join(SACCT_USAGE_STATES),    # Looking for completed, failed, and timed out jobs.
                 '--start=' + SACCT_BEGIN_DATE                 # Start from the begining of service 
    ]
    if assoc_list:
        usage_cmd.append('-q')
        usage_cmd.append(','.join([x[1] for x in assoc_list.keys()]))

    
    __verbose_print('[SLURM]: ' +' '.join(usage_cmd), level=Verbosity.INFO)

    usage_output_raw = subprocess.check_output(usage_cmd, universal_newlines=True).splitlines()
    usage_output = csv.DictReader(usage_output_raw, fieldnames=SACCT_USAGE_FIELDS, delimiter='|')

    for usage in usage_output:
        account_qos = (usage['account'], usage['qos'])

        # Skip if AllocTres is not presented
        if not usage['alloctres']:
            continue

        alloc_tres = {k:int(v.replace('M','')) for k,v in (x.split('=') for x in usage['alloctres'].strip().split(','))}
        
        #Convert seconds to minute
        elapsed_mins = int(usage['elapsedraw']) / 60.0

        if usage_info.get(account_qos, None):
            for tres in alloc_tres:
                if usage_info[account_qos].get(tres, None):
                    usage_info[account_qos][tres] += alloc_tres[tres] * elapsed_mins
                else:
                    usage_info[account_qos][tres] = alloc_tres[tres] * elapsed_mins
            usage_info[account_qos]['elaspsed_mins'] += elapsed_mins
        else:
            usage_info[account_qos] = {}
            for tres in alloc_tres:
                usage_info[account_qos][tres] = alloc_tres[tres] * elapsed_mins
            usage_info[account_qos]['elaspsed_mins'] = elapsed_mins

    
    __verbose_print(usage_info, level=Verbosity.DEBUG)

    return usage_info

def print_user_balance_table(user, user_account, user_usage, units='', col_width=15):
    if units == 'k':
        su_units = 'kSU'
        su_factor = 1.0e3
    elif units == 'm':
        su_units = 'MSU'
        su_factor = 1.0e6
    else:
        su_units = 'SU'
        su_factor = 1

    table_format = "{:<{col_width}s} {:<{col_width}s} {:{col_width}s} {:>{col_width}s} {:>{col_width}s} {:>{col_width}s} {:>{col_width}s}".replace('{col_width}', str(col_width))

    print("Account balances for user: %s" % user)
    print()
    print(table_format.format('Account', 'QoS', 'Description', 'Allocation({})'.format(su_units), 'Remaining({})'.format(su_units), 'Remaining(%)', 'Used({})'.format(su_units), col_width=col_width)) 
    print(('-'*col_width + ' ') * 7 )
    
    for assoc in user_account:
        account = assoc[0]
        qos = assoc[1]

        balance = None
        limits = None
        usage = None    

        if user_account[assoc].get('grptresmins', None):
            limits = user_account[assoc]['grptresmins']['billing'] / su_factor
            
            if user_usage.get(assoc, None):
                usage = math.ceil(user_usage[assoc]['billing']) / su_factor
            else:
                usage = 0
            
            balance = limits - usage
            balance_percent = balance * 100.0 / limits

        
        print(table_format.format(account,
                                  qos, 
                                  user_account[assoc].get('description', ''), 
                                  "{:{col_width}.2f}".format(limits, col_width=col_width) if limits != None else 'unlimited', 
                                  "{:{col_width}.2f}".format(balance, col_width=col_width) if balance != None else '', 
                                  "{:{col_width}.2f}".format(balance_percent, col_width=col_width) if balance != None else '', 
                                  "{:{col_width}.2f}".format(usage, col_width=col_width) if usage != None else '', 
                                  ))
    print()

def print_user_balance(user, user_account, user_usage, units=''):
    if units == 'k':
        su_units = 'kSU'
        su_factor = 1.0e3
    elif units == 'm':
        su_units = 'MSU'
        su_factor = 1.0e6
    else:
        su_units = 'SU'
        su_factor = 1

    print("Account balances for user: %s" % user)

    for assoc in user_account:
        account = assoc[0]
        qos = assoc[1]

        if user_account[assoc]['default'] == None or user_account[assoc]['default']:
            print(account + ": " )
        else:
            print(account + ":" + qos + ": " )

        if user_account[assoc].get('grptresmins', None):
            limits = user_account[assoc]['grptresmins']['billing'] / su_factor
            
            if user_usage.get(assoc, None):
                usage = user_usage[assoc]['billing'] / su_factor
            else:
                usage = 0
            
            balance = limits - usage
            balance_percent = balance * 100.0 / limits

            if user_account[assoc].get('description', None):
                print("\t{:20} {:>18s}".format("Description:", user_account[assoc]['description']))    
            print("\t{:20} {:15.2f} {}".format("Allocation:", limits, su_units))
            print("\t{:20} {:15.2f} {} ({:6.2f}%)".format("Remaining Balance:", balance, su_units, balance_percent))
            print("\t{:20} {:15.2f} {}".format("Used:", usage, su_units))
        else:
            print("\t{:20} {:>18}".format("Allocation:", "unlimited"))

def parse_args():
    slurm_version = str(subprocess.check_output(['sshare', '--version']))

    parser = argparse.ArgumentParser(prog='sbalance', description='Query slurm account balance.')
    version = "sbalance " + __version__ + " with " + slurm_version
    parser.add_argument(
        '-V', '--version', action='version', version=version)
    parser.add_argument(
        '-k', action='store_const', dest='unit', const='k', help="show output in kSU (1,000 SU)")
    parser.add_argument(
        '-m', action='store_const', dest='unit', const='m', help="show output in MSU (1,000,000 SU)")
    parser.add_argument(
        '-t', '--table', action='store_const', dest='pformat', const='table', help="print output as table")
    parser.add_argument(
        '-v', '--verbose', action='count', help="verbose mode (multiple -v's increase verbosity)")

    return parser.parse_args()

def main():
    args = parse_args()  

    if args.verbose:
        def verbose_print(*a, **k):
            if k.pop('level', 0) <= args.verbose:
                pprint(*a, **k)
    else:
        verbose_print = lambda *a, **k: None

    global __verbose_print
    __verbose_print = verbose_print

    __verbose_print(args, level=Verbosity.DEBUG)

    user = getpass.getuser()
    
    qos_list = query_qos()
    user_assocs = query_assocs(qos_list)
    user_usage = query_usage(user_assocs)

    if args.pformat == 'table':
        print_user_balance_table(user, user_assocs, user_usage, args.unit)
    else:
        print_user_balance(user, user_assocs, user_usage, args.unit)
