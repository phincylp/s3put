#!/usr/bin/python


#load modules needed
import MySQLdb
import logging
import subprocess
import os
import socket
import subprocess
from optparse import OptionParser
import boto
import ConfigParser
from boto.exception import S3ResponseError
from boto.s3.connection import OrdinaryCallingFormat

#enabling logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
fh = logging.FileHandler('/var/log/d42put/error.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(process)d - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Read configuration

config = ConfigParser.ConfigParser()
config.read('/etc/fk-3p-mysql-backup/config.cfg')
buc_name = socket.gethostname()
aws_access_key_id = config.get('creds', 'access_key')
aws_secret_access_key = config.get('creds', 'secret_key')
host = config.get('creds', 'gateway')
# String codes affecting output to shell.
BOLD = "\033[1m"
RESET = "\033[0;0m"

# Globals set with set_global_options_and_args. Contains all arguments set
# from command line.
OPTIONS = None
ARGS = None

def createBucket()
# check d42 connection and create bucket if not existsing

	conn = boto.connect_s3(aws_access_key_id="%s" % aws_access_key_id,
			aws_secret_access_key="%s" % aws_secret_access_key,
			host='%s' % host, port=80,
			is_secure=False,
			calling_format=OrdinaryCallingFormat())

	try:
		bucket = conn.get_bucket(buc_name)
		print "D42 Bucket already exists."
	except:
		bucket = conn.create_bucket(buc_name)
		print "D42 Bucket successfully created."


def set_global_options_and_args():
        '''
        Set cmd line arguments in global vars OPTIONS and ARGS.
        '''
        global OPTIONS, ARGS
        parser = OptionParser(usage="usage: %prog {snapshot|clean}")
        (OPTIONS, ARGS) = parser.parse_args()

        if len(ARGS) != 1:
                parser.error("incorrect number of arguments")

def clean():
        '''
        Start slave
        '''
        logger.info('Calling cleaning function.')
        x("""mysql -ufk_admin -pmn42DXe3d -h`hostname -I` << EOF
start slave;
EOF""")


def get_slave_postion():
        try:
                subprocess.call(['echo "SHOW MASTER STATUS:" > /tmp/mysql.pos ; mysql -ufk_admin -pmn42DXe3d -h`hostname -I` -e "show master status\G" >> /tmp/mysql.pos; echo "\n\nSHOW SLAVE STATUS:" >> /tmp/mysql.pos ; mysql -ufk_admin -pmn42DXe3d -h`hostname -I` -e "show slave status\G" >> /tmp/mysql.pos' ], shell=True)
        except:
                logger.info('Unable to take the master/slave position')

def x(cmd):
        '''
        Execute the shell command CMD.
        '''
        print(BOLD + "Command: " + RESET + cmd)

        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (stdout, stderr) = p.communicate()
        (stdout, stderr, p.pid)
        if (stdout):
                print(stdout)
        if (stderr):
                print(stderr)

def snapshot():
        '''
        Stop slave, flush tables and get the positions. We are not actually creating any snapshot here
        '''
        print "Stop slave, flush tables and get the positions"
        x("""mysql -ufk_admin -pmn42DXe3d -h`hostname -I` << EOF
stop slave;
flush tables;
FLUSH TABLES WITH READ LOCK;
EOF""")
        x("sleep 90")
        get_slave_postion()


