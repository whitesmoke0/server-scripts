#!/usr/bin/env python

# pgsync.py - Dump and restore data from one PostgreSQL database to another. The
#             log files are generated in /var/log/pgsync directory. Make sure
#             this directory exists before running the script.

from __future__ import print_function
import os
import sys
import stat
import logging
import datetime
import subprocess
from string import lower
from os.path import expanduser
from optparse import OptionParser, OptionValueError


SOURCE = 'postgres://username:password@host:port/database'
TARGET = 'postgres://username:password@host:port/database'

# logging.basicConfig(level=logging.DEBUG,
logging.basicConfig(level=logging.INFO,
  format='%(asctime)s - pgsync.py - %(levelname)-7s - %(message)s')
logger = logging.getLogger('pgsync.py')


def parse_postgres_url(url):
  """
  Parse the given postgres url and return as a dict

  postgres://username:password@host:port/database
  """
  result = {}

  split = url.split('/')
  result['database'] = split[3]
  split = split[2].split('@')
  result['user'] = split[0].split(':')[0]
  result['passwd'] = split[0].split(':')[1]
  result['host'] = split[1].split(':')[0]
  result['port'] = split[1].split(':')[1]

  logger.debug(result)
  return result

def check_call_output(args, cwd):
  """
  Call subprocess.Popen with the given arguments and wait for the child process to
  finish. Return the STDOUT and the return code
  """
  out = None
  err = None
  retcode = None

  logger.debug('<check_call_output> {0} for {1}'.format(args, cwd))
  try:
    output = subprocess.Popen(args, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = output.communicate()
    out = out.decode('utf-8').strip()
    err = err.decode('utf-8').strip()
    retcode = output.poll()
    if retcode != 0:
      raise Exception(str(retcode) + ' : ' + err)
  except Exception as e:
    logger.debug('<check_call_output> error: [{0}, {1}] - {2}'.format(args, cwd, e))

  return {
    'stdout': None if out == '' else out,
    'stderr': None if err == '' else err,
    'retcode': retcode
  }
 
def write_passwd(passwd):
  """
  Generate the password file necessary for automating pg_dump/pg_restore
  commands
  """
  passwd_file = os.path.join(expanduser('~'), '.pgpass')

  logger.debug('<write_passwd> creating password file at {0}'.format(passwd_file))
  f = open(passwd_file, 'w')
  os.fchmod(f.fileno(), stat.S_IREAD | stat.S_IWRITE)
  f.write('*:*:*:*:{0}'.format(passwd + '\n'))
  f.close()

def dump_db(source, dump_file):
  """
  Generate the dump of the source database
  """
  write_passwd(source['passwd'])

  logger.info('downloading database dump from source: {0}@{1}:{2}/{3}'.format(
    source['user'], source['host'],
    source['port'], source['database'])
  )

  dump_output = check_call_output(['pg_dump', '--verbose', '-F', 'c', '-b', '-h',
    source['host'], '-p', source['port'], '-U', source['user'], '-w',
    '-f', dump_file, source['database']], cwd=None
  )

  logger.debug(dump_output)

  return dump_output['retcode']

def restore_db(target, dump_file):
  """
  Upload the dump to the target database
  """
  write_passwd(target['passwd'])

  logger.info('uploading database dump to target: {0}@{1}:{2}/{3}'.format(
    target['user'], target['host'],
    target['port'], target['database'])
  )

  restore_output = check_call_output(['pg_restore', '--verbose', '--clean',
    '--no-acl', '--no-owner', '-h', target['host'], '-p',
    target['port'], '-U', target['user'], '-w', '-d',
    target['database'], dump_file], cwd=None
  )

  log_file = os.path.join('/var/log/pgsync', '{0}-{1}.{2}'.format(
    'pgsync',
    datetime.datetime.now().strftime('%Y%m%d-%H%M'), 'log'
  ))

  f = open(log_file, 'w')
  f.write('*'*20 + 'Output' + '*'*20 + '\n')
  f.write('' if restore_output['stdout'] is None else restore_output['stdout'])
  f.write('\n')
  f.write('*'*20 + 'Error' + '*'*20 + '\n')
  f.write('' if restore_output['stderr'] is None else restore_output['stderr'])
  f.write('\n')
  f.close()

  logger.info('generated upload log @ {0}'.format(log_file))


if __name__ == '__main__':
  logger.debug('start')

  try:
    source_db = parse_postgres_url(SOURCE)
    target_db = parse_postgres_url(TARGET)

    if(source_db['host'] == 'host' or target_db['host'] == 'host' or
        source_db['port'] == 'port' or target_db['port'] == 'port'):
      raise Exception('set SOURCE/TARGET database information correctly')

    dump_file = os.path.join('/tmp', '{0}-{1}.{2}'.format(
      'backup',
      datetime.datetime.now().strftime('%Y%m%d-%H%M'), 'dump'
    ))

    if dump_db(source_db, dump_file) == 0:
      # Will always complain of "could not execute query: ERROR:  must be owner of
      # extension plpgsql". So disable the retcode check for restore_db
      restore_db(target_db, dump_file)
    else:
        logger.error('error downloading dump from source database')

    passwd_file = os.path.join(expanduser('~'), '.pgpass')
    if os.path.isfile(passwd_file):
      os.remove(passwd_file)
    if os.path.isfile(dump_file):
      os.remove(dump_file)
  except Exception as e:
    logger.error('Error: ' + str(e))

  logger.debug('finish')
