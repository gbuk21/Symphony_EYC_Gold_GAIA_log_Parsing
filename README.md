# Symphony_EYC_Gold_GAIA_log_Parsing

To parse gaia logs and to capture sql text, return row count, elapsed time and exceptions.

Symphony EYC GOLD writes gaia logs on the unix machine where GOLD online application is hosted. python program in this repository needs to run on the same Unix machine and 
the output data is written a SQLite3 which can be converted to an Oracle DB with some minor changes.

