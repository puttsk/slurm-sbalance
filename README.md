# slurm-sbalance
`sbalance` is a Slurm utility for checking account balance. The utility calculates the remaining *service units* or *SU* left in the account. 

# Prerequisites
*  Slurm with Accounting enabled 

Currently `sbalance` has been tested with: 
* Python 2.7.5 and Python 3.6.6
* Slurm 18.08.5

# Usage
```
usage: sbalance [-h] [-V] [-k] [-m] [-v]

Query slurm account balance.

optional arguments:
  -h, --help     show this help message and exit
  -V, --version  show program's version number and exit
  -k             show output in kSU (1,000 SU)
  -m             show output in MSU (1,000,000 SU)
  -v, --verbose  verbose mode (multiple -v's increase verbosity)

```

# Setting up Slurm
`sbalance` currently support following setup. 
* Balance is limited per account
* Account limit is set through QoS with `GrpTRESMins` and `NoDecay` flag.

Following is an example setup

Creating account `tutorial` and QoS `test` with billing balance of 1000000
```
sacctmgr add qos test set GrpTRESMins=billing=1000000 Flags=NoDecay 
sacctmgr add account tutorial set QoS=test DefaultQoS=test
```

Add `test` user to account `tutorial`
```
sacctmgr add user test set Account=tutorial
```

Checking balance for `test` user
```
[test@localhost ~]$ sbalance 
Account balances for user: test
tutorial:
	Allocation:               1000000.00 SU
	Remaining Balance:         991896.00 SU ( 99.19%)
	Used:                        8104.00 SU
```

