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
import json
import sys

from io import StringIO

import numpy as np
import pandas as pd

from .config import __version__, __author__, __license__, SACCT_BEGIN_DATE

Verbosity = type('Verbosity', (), {'INFO':1, 'WARNING':2, 'DEBUG':5})

DEBUG = False

SACCT_COMMAND = 'sacct'
SACCT_USAGE_FIELDS = ('jobid', 'user', 'account','qos','state','alloctres','elapsedraw','partition')
SACCT_USAGE_STATES = ('CD',     # COMPLETED
                      'F',      # FAILED
                      'TO',     # TIMEOUT
                      'CA'      # CANCEL
)

SACCTMGR_COMMAND = 'sacctmgr'
SACCTMGR_QOS_FIELDS = ('name','grptresmins','flags','description')
SACCTMGR_QOS_NODECAY_FLAG = 'NoDecay'
SACCTMGR_ASSOC_FIELDS = ('account','user','qos', 'defaultqos')

__verbose_print = None

def parse_args():
    slurm_version = str(subprocess.check_output(['sshare', '--version']).decode())

    parser = argparse.ArgumentParser(prog='sbalance', description='Query slurm account balance.')
    version = "sbalance " + __version__ + " with " + slurm_version

    parser.add_argument(
        '-d','--detail', action='store_true', help="display SU usage per users")
    parser.add_argument(
        '-S', '--start', action='store', default=SACCT_BEGIN_DATE, help="starting date")
    parser.add_argument(
        '-o', '--output', action='store', help="output file")
    
    format_parser = parser.add_argument_group('format', 'output format')
    format_parser.add_argument(
        '--format', action='store', dest='format', help="output format. Valid options: table, csv, json. Default: table", default='table')
    format_parser.add_argument(
        '-c', '--csv', action='store_const', dest='format', const='csv', help="print output as csv")
    format_parser.add_argument(
        '-t', '--table', action='store_const', dest='format', const='table', help="print output as table")
    format_parser.add_argument(
        '-j', '--json', action='store_const', dest='format', const='json', help="print output as json")
    format_parser.add_argument(
        '-k', action='store_const', dest='unit', default='', const='k', help="show output in kSU (1,000 SU)")
    format_parser.add_argument(
        '-m', action='store_const', dest='unit', const='M', help="show output in MSU (1,000,000 SU)")
    
    parser.add_argument(
        '-v', '--verbose', action='count', help="verbose mode (multiple -v's increase verbosity)")
    parser.add_argument(
        '-V', '--version', action='version', version=version)

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

    if args.unit == 'k':
        su_units = 'kSU'
        su_factor = 1.0e-3
    elif args.unit == 'M':
        su_units = 'MSU'
        su_factor = 1.0e-6
    else:
        su_units = 'SU'
        su_factor = 1

    user = getpass.getuser()
    
    qos_cmd = [SACCTMGR_COMMAND,'show', 'qos','-P',
               'format=' + ','.join(SACCTMGR_QOS_FIELDS)
    ]
    qos_output_raw = subprocess.check_output(qos_cmd).decode('utf-8')
    qos = pd.read_csv(StringIO(qos_output_raw), sep='|')
    qos['GrpTRESMins'] = qos['GrpTRESMins'].apply(lambda tres: {k:int(v) for k,v in (x.split('=') for x in tres.strip().split(','))} if not pd.isnull(tres) else tres)
    qos['Allocation'] = qos['GrpTRESMins'].apply(lambda x: x['billing'] if not pd.isnull(x) else 'unlimited')
    
    # Remove non-accountable QoS
    qos = qos.loc[qos['Flags'] == 'NoDecay']

    assoc_cmd = [SACCTMGR_COMMAND,
                   'show', 'assoc','-P',
                   'format=' + ','.join(SACCTMGR_ASSOC_FIELDS)
    ]
    # Assume user only have one QoS
    assoc_output_raw = subprocess.check_output(assoc_cmd).decode('utf-8')
    assoc = pd.read_csv(StringIO(assoc_output_raw), sep='|')
    valid_account = assoc.loc[assoc['User'].notnull()]['QOS'].unique()

    # Query account usage
    usage_cmd = [SACCT_COMMAND,
                 '-aPX', '--noconvert',
                 '--format=' + ','.join(SACCT_USAGE_FIELDS),
                 '--start=' + args.start                             # Start from the begining of service 
    ]
    usage_cmd.append('-q')
    usage_cmd.append(','.join(valid_account))

    usage_output_raw = subprocess.check_output(usage_cmd).decode('utf-8')
    usage = pd.read_csv(StringIO(usage_output_raw), sep='|', dtype={'JobID':str, 'User': str, 'Account': str, 'QOS': str, 'State': str, 'AllocTRES': str, 'ElapsedRaw': int, 'Partition': str})
    usage['AllocTRES'] = usage['AllocTRES'].apply(lambda tres: {k:int(v.replace('M','')) for k,v in (x.split('=') for x in tres.strip().split(','))} if not pd.isnull(tres) else {})
    usage['ElapsedRaw'] = usage['ElapsedRaw'].apply(lambda x: x / 60.0 if not pd.isnull(x) else x)
    usage['Billing'] = usage.apply(lambda r: r['AllocTRES'].get(u'billing', 0) * r['ElapsedRaw'] if not pd.isnull(r['AllocTRES']) else 0, axis=1)
    
    account_usage = usage.groupby(['Account'], as_index=False).agg({'Billing':'sum'})
    
    if args.detail:
        account_usage = usage.groupby(['Account', 'User'], as_index=False).agg({'Billing':'sum'})
        
    qos = qos.rename(columns={'Name': 'Account'})

    result = pd.merge(qos, account_usage, on='Account', how='outer', sort=True)
    result = result.drop(columns=['GrpTRESMins', 'Flags'])
    result = result.rename(columns={'Billing': 'Used', 'Descr': 'Description'})
    
    if args.detail:
        result['User'] = result['User'].fillna('')

    result['Used'.format(su_units)] = result['Used'].apply(lambda x: 0 if pd.isnull(x) else int(math.ceil(x)))
    result['Used({})'.format(su_units)] = result['Used'].apply(lambda x: 0 if pd.isnull(x) else int(math.ceil(x)) * su_factor)
    result['Allocation({})'.format(su_units)] = result['Allocation'].apply(lambda x: 0 if pd.isnull(x) else x * su_factor)
    result['Remaining'] = result.apply(lambda r: (r['Allocation'] - r['Used']) if not type(r['Allocation']) is str else '' ,axis=1)
    result['Remaining({})'.format(su_units)] = result['Remaining'].apply(lambda x: 0 if pd.isnull(x) else x * su_factor)
    result['Remaining(%)'] = result.apply(lambda r: float(r['Remaining'])/r['Allocation']*100.0 if not type(r['Allocation']) is str else '' ,axis=1)
    result = result.loc[result['Account'].isin(valid_account)]

    pd.set_option('display.max_rows', None)
    pd.options.display.float_format = '{:.2f}'.format
    
    if args.detail:
        result['Used(%)'] = result.apply(lambda r: float(r['Used'])/r['Allocation']*100  if not type(r['Allocation']) is str else '' ,axis=1)
        table = pd.pivot_table(result, values=['Used({})'.format(su_units), 'Used(%)'], index=['Account', 'User'])

        if args.format == 'table':
            lines = table.to_string(col_space=12, formatters={'Used(%)':'{:.2f}'.format}).split('\n')
            lines.insert(2, '-'*len(lines[0]))
            if args.output:
                with open(args.output, 'w') as f:
                    f.write('\n'.join(lines))
                    f.write('\n')
            else:
                print()
                print('\n'.join(lines))
                print()

        elif args.format == 'csv':
            if args.output:
                table.to_csv(args.output)
            else:
                print(table.to_csv())
        elif args.format == 'json':
            if args.output:
                table.to_json(args.output, orient='table')
            else:
                print(table.to_json(orient='table'))
    else:
        if args.format == 'table':
            lines = result.to_string(index=False, columns=['Account', 'Description', 'Allocation({})'.format(su_units), 'Remaining({})'.format(su_units), 'Remaining(%)', 'Used({})'.format(su_units)], col_space=12).split('\n')
            lines.insert(1, '-'*len(lines[0]))
            if args.output:
                with open(args.output, 'w') as f:
                    f.write('\n'.join(lines))
                    f.write('\n')
            else:
                print()
                print('\n'.join(lines))
                print()
        elif args.format == 'csv':
            if args.output:
                result.to_csv(args.output, index=False)
            else:            
                print(result.to_csv(index=False))
        elif args.format == 'json':
            if args.output:
                result.to_json(args.output, index=False, orient='table')
            else:
                print(result.to_json(index=False, orient='table'))
