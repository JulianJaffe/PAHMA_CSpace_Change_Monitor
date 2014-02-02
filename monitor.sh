DATE=$(date +"%Y%m%d")
time python monitor.py | mail -s "Cspace Change Monitor $DATE" pahma-cspace@berkeley.edu
