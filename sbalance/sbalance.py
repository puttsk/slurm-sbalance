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

from collections import OrderedDict

__version__ = '0.1b'
__author__ = 'Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>'
__license__ = 'MIT'

Verbosity = type('Verbosity', (), {'INFO':1, 'WARNING':2, 'DEBUG':5})

DEBUG = False

SACCT_COMMAND = 'sacct'
SACCT_BEGIN_DATE = '01/01/19'
SACCT_USAGE_FIELDS = ('jobid', 'user', 'account','qos','state','alloctres','elapsedraw','partition')
SACCT_USAGE_STATES = ('CD',     # COMPLETED
                      'F',      # FAILED
                      'TO'      # TIMEOUT
)

SACCTMGR_COMMAND = 'sacctmgr'
SACCTMGR_QOS_FIELDS = ('name','grptresmins','flags')
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
            qos_info[qos['name']] = {k:int(v) for k,v in (x.split('=') for x in qos['grptresmins'].strip().split(','))}

    __verbose_print(qos_info, level=Verbosity.DEBUG)

    return qos_info

def query_accounts(qos_info, user_list=None, verbose=0):
    account_info = OrderedDict()
    account_cmd = [SACCTMGR_COMMAND,
                   '--noheader', 'show', 'assoc','-P',
                   'format=' + ','.join(SACCTMGR_ASSOC_FIELDS)
    ]

    __verbose_print('[SLURM]: ' +' '.join(account_cmd), level=Verbosity.INFO)

    # Assume user only have one QoS
    assoc_output_raw = subprocess.check_output(account_cmd, universal_newlines=True).splitlines()
    assoc_output = csv.DictReader(assoc_output_raw, fieldnames=SACCTMGR_ASSOC_FIELDS, delimiter='|')
    
    for assoc in assoc_output:
        if not assoc['user']:
            continue

        if not user_list or assoc['user'] in user_list:
            qos_list = assoc['qos'].split(',')

            for qos in qos_list:
                if qos in qos_info:
                    account_info[(assoc['account'], qos)] = qos_info[qos]
                else:
                    account_info[(assoc['account'], qos)] = {}
                
                # Has default QoS
                if len(qos_list) > 1:
                    account_info[(assoc['account'], qos)]['default'] = (qos == assoc['defaultqos'])
                else:
                    account_info[(assoc['account'], qos)]['default'] = None

    __verbose_print(account_info, level=Verbosity.DEBUG)

    return account_info

def query_usage(qos_list=None, verbose=0):
    usage_info = {}
    usage_cmd = [SACCT_COMMAND,
                 '-aPX', '--noheader', '--noconvert',
                 '--format=' + ','.join(SACCT_USAGE_FIELDS),
                 '--state=' + ','.join(SACCT_USAGE_STATES),    # Looking for completed, failed, and timed out jobs.
                 '--start=' + SACCT_BEGIN_DATE                 # Start from the begining of service 
    ]
    if qos_list:
        usage_cmd.append('-q')
        usage_cmd.append(','.join(qos_list.keys()))

    
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
                    usage_info[account_qos][tres] += math.ceil(alloc_tres[tres] * elapsed_mins)
                else:
                    usage_info[account_qos][tres] = math.ceil(alloc_tres[tres] * elapsed_mins)
            usage_info[account_qos]['elaspsed_mins'] += elapsed_mins
        else:
            usage_info[account_qos] = alloc_tres
            usage_info[account_qos]['elaspsed_mins'] = elapsed_mins

    
    __verbose_print(usage_info, level=Verbosity.DEBUG)

    return usage_info

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
            print(account + ":")
        else:
            print(account + ":" + qos + ":")

        if user_account[assoc].get('billing', None):
            limits = user_account[assoc]['billing'] / su_factor
            
            if user_usage.get(assoc, None):
                usage = user_usage[assoc]['billing'] / su_factor
            else:
                usage = 0
            
            balance = limits - usage
            balance_percent = balance * 100.0 / limits

            print("\tAllocation:\t\t%12.2f %s" % (limits, su_units))
            print("\tRemaining Balance:\t%12.2f %s (%0.2f%%)" % (balance, su_units, balance_percent))
            print("\tUsage:\t\t\t%12.2f %s" % (usage, su_units))
        else:
            print("\tAllocation:\t\t%12s" % 'unlimited')

def parse_args():
    slurm_version = str(subprocess.check_output(['sshare', '--version']))

    parser = argparse.ArgumentParser(prog='sbalance', description='Query slurm account balance.')
    version = "sbalance v." + __version__ + " with " + slurm_version
    parser.add_argument(
        '-V', '--version', action='version', version=version)
    parser.add_argument(
        '-k', action='store_const', dest='unit', const='k', help="show output in kSU (1,000 SU)")
    parser.add_argument(
        '-m', action='store_const', dest='unit',    const='m', help="show output in MSU (1,000,000 SU)")
    parser.add_argument(
        '-v', '--verbose', action='count', help="verbose mode (multiple -v's increase verbosity)")

    return parser.parse_args()

def main():
    args = parse_args()  

    if args.verbose:
        def verbose_print(*a, **k):
            if k.pop('level', 0) <= args.verbose:
                print(*a, **k)
    else:
        verbose_print = lambda *a, **k: None

    global __verbose_print
    #__verbose_print = lambda *a, **k: print(a, k) if args.verbose else lambda *a, **k: None
    __verbose_print = verbose_print

    __verbose_print(args, level=Verbosity.DEBUG)

    user = getpass.getuser()
    
    qos_list = query_qos()
    user_account = query_accounts(qos_list)
    user_usage = query_usage(qos_list)

    print_user_balance(user, user_account, user_usage, args.unit)

if __name__ == "__main__":
    main()
