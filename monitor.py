#!/usr/bin/env /usr/bin/python

import sys
import os
import csv
import traceback
import datetime
import re
import pgdb
from collections import defaultdict

reload(sys)
sys.setdefaultencoding('utf-8')

monitorList = ['Object number', 'Deleted', 'Object count', 'Object name', 'Collection manager',
               'Accession status', 'EFC', 'Object type', 'Field collection place', 'Associated culture']

r = re.compile(u"^.*\)'(.*)'$", re.UNICODE)

def main():
    """For each category in monitorlist, this program will either compare the current stats to the previous
    stats on file or create a new file if none exists. The idea is to produce an easy-to-read message that
    will allow mistakes and errors to be caught (relatively) immediately rather than building up un-noticed.
    This program also saves the previous files, albeit with the datestamp of the time the program compared
    it rather than when it was written. With the current set-up, this is just the next day, so it's just a
    small off-by-one bug.

    The flow for each category is open <category>.txt -> exists -> build dict -> get current -> compare to current
                                                       |                                            |
                                                       |                                            v
                                                       |                                            note changes
                                                       |
                                                       |-> doesn't exist -> get current -> write to <category>.txt

    TODO: Write smarter object count function (e.g. an increase in count from 1 to 6 probably just means that 1 was
    a placeholder value and shouldn't be suspicious, but a decrease from 6 to 1 should be investigated).
    """
    monitorDict = defaultdict(lambda: defaultdict(unicode))
    start = datetime.datetime.now()
    msg = u'Change report:\n\nDate: %s\n' % (str(start.strftime('%Y-%m-%d %H:%M:%S')))
    try:
        for monitor in monitorList:
            msg += '\nMonitoring \'%s\'\n\nThe following changes were made since last run:\n\n' % (monitor)
            fn = monitor + '.txt'
            try:
                with(open(fn, 'r')) as f:
                    reader = csv.reader(f, delimiter=',', quotechar='\x36')
                    for row in reader:
                        monitorDict[monitor][row[0]] = row[1]
                newname = os.path.splitext(fn)[0] + \
                          str(datetime.datetime.now().strftime('%Y%m%d%H%M%S')) + os.path.splitext(fn)[1]
                os.rename(fn, newname)
                result = execute(monitor)
                with(open(fn, 'w')) as f:
                    for key, value in result:
                        if value == None:
                            value = ''
                        if not (unicode(monitorDict[monitor][key]).lower() == unicode(value).lower()):
                            if monitorDict[monitor][key] == '':
                                msg += u'%s added %s\n' % (getObjNo(key).ljust(10), value)
                            elif monitor in ['Object count']:
                                if True: #replace with appropriate function when written
                                    msg += '%s  %s changed to %s\n' % (getObjNo(key).ljust(10), monitorDict[monitor][key].ljust(5),
                                                                       value)
                            elif monitor in ['EFC', 'Field collection place', 'Associated culture']:
                                try:
                                    msg += u'%s %s changed to %s\n' % (getObjNo(key).ljust(10),
                                                                          r.search(monitorDict[monitor][key]).group(1).ljust(30),
                                                                          r.search(value).group(1))
                                except AttributeError or IndexError as e:
                                    msg += 'AttributeError, trying again...\n'
                                    msg += u'%s %s changed to %s\n' % (getObjNo(key).ljust(10), monitorDict[monitor][key].ljust(30),
                                                                          value) 
                            else:
                                msg += u'%s %s changed to %s\n' % (getObjNo(key).ljust(10), monitorDict[monitor][key].ljust(20),
                                                                   value)                                
                        writer = csv.writer(f, delimiter=',', quotechar='\x36')
                        writer.writerow([key, value])
            except IOError, e:
                if e.errno == 2:
                    try:
                        result = execute(monitor)
                        with(open(fn, 'w')) as f:
                            for key, value in result:
                                writer = csv.writer(f, delimiter=',', quotechar='\x36')
                                writer.writerow([key, value])
                        msg += '\n%s monitor added\n' % (monitor)
                    except:
                        trace = traceback.format_exc().splitlines()
                        msg += 'An error Occurred:\n%s\n' % (trace)
                else:
                    trace = traceback.format_exc().splitlines()
                    msg += 'An error Occurred:\n%s\n' % (trace)
            except:
                trace = traceback.format_exc().splitlines()
                msg += 'An error Occurred:\n%s\n' % (trace)
    except Exception as e:
        trace = traceback.format_exc().splitlines()
        msg += 'An error Occurred:\n%s\n' % (trace)
    end = datetime.datetime.now()
    msg += '\nElapsed time: %s\n' % (str(end-start))
    msg += '\nGenerated by monitor.py'
    writeMsg(msg)

def execute(monitor):
    """Get the current counts/values/etc. for comparison. MONITOR is a category to monitor.
    Returns the current values from the db."""
    timeoutcommand = "set statement_timeout to 300000; SET NAMES 'utf8';"
    dbconn = pgdb.connect('###Connect String###')
    db = dbconn.cursor()
    db.execute(timeoutcommand)

    query = getQuery(monitor)

    try:
        db.execute(query)
        return db.fetchall()
    except:
        raise

def getQuery(monitor):
    """Returns the correct query for the given MONITOR category."""
    queryDict = {'Object count': 'SELECT co.id, co.numberofobjects FROM collectionobjects_common co',
                 'Object name': '''SELECT co.id, ong.objectname FROM collectionobjects_common co
                 JOIN hierarchy h ON (co.id = h.parentid AND h.name='collectionobjects_common:objectNameList' AND h.pos=0)
                 JOIN objectnamegroup ong ON (h.id = ong.id)''',
                 'Deleted': 'SELECT co.id, m.lifecyclestate FROM collectionobjects_common co JOIN misc m ON (m.id = co.id)',
                 'Object number': 'SELECT co.id, co.objectnumber FROM collectionobjects_common co',
                 'Collection manager': '''SELECT co.id, cm.item FROM collectionobjects_common co
                 JOIN collectionobjects_common_responsibledepartments cm ON (co.id = cm.id AND cm.pos=0)''',
                 'Accession status': '''SELECT co.id, osl.item FROM collectionobjects_common co
                 JOIN collectionobjects_pahma_pahmaobjectstatuslist osl ON (co.id = osl.id AND osl.pos=0)''',
                 'EFC': '''SELECT co.id, efc.item FROM collectionobjects_common co
                 JOIN collectionobjects_pahma_pahmaethnographicfilecodelist efc ON (co.id = efc.id AND efc.pos=0)''',
                 'Object type': 'SELECT co.id, co.collection FROM collectionobjects_common co',
                 'Field collection place': '''SELECT co.id, fcp.item FROM collectionobjects_common co
                 JOIN collectionobjects_pahma_pahmafieldcollectionplacelist fcp ON (co.id = fcp.id AND fcp.pos=0)''',
                 'Associated culture': '''SELECT co.id, apg.assocpeople FROM collectionobjects_common co
                 JOIN hierarchy h ON (co.id = h.parentid AND h.name='collectionobjects_common:assocPeopleGroupList' AND h.pos=0)
                 JOIN assocpeoplegroup apg ON (h.id = apg.id)'''}

    return queryDict.setdefault(monitor, '')

def getObjNo(objID):
    """Given an OBJID, returns the associated object number."""
    timeoutcommand = "set statement_timeout to 300000; SET NAMES 'utf8';"
    dbconn = pgdb.connect('###Connect String###')
    db = dbconn.cursor()
    db.execute(timeoutcommand)
    
    query = 'SELECT co.objectnumber FROM collectionobjects_common co WHERE co.id = \'%s\'' % (objID)

    try:
        db.execute(query)
        return db.fetchone()[0]
    except:
        raise

def writeMsg(msg):
    """Prints MSG and saves it to a file as a backup."""
    print msg

    fn = 'Cspace Change Monitor %s.txt' % (datetime.datetime.now().strftime('%Y-%m-%d'))
    try:
        with(open(fn, 'w')) as f:
            f.write(msg)
    except IOError:
        pass

if __name__ == "__main__":
    main()
