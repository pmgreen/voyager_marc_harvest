This was hacked together quickly. There's no setup.py nor are there any tests. 

Sorry.

To run:
 * Clone the code repo
 * Get the dependencies from PyPi (see below)
 * `cp conf.ini.tmpl conf.ini` and fill it out
 * Adjust `StdOutFilter` in `harvest.py` if you want debugging output (see comment)
 * Execute `harvest.py`

Requirements: 
 * python-dateutil 1.5: pip install python-dateutil==1.5
 * paramiko: pip install paramiko
