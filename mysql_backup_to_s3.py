#!/usr/bin/env python

# vtiger_backup.py - Dump the vTiger database and upload the dump to Amazon S3
#                    bucket

import os
import logging
import datetime
import subprocess
from os.path import expanduser
from boto.s3.connection import S3Connection
from boto.s3.key import Key


USER      = ''
PASSWD    = ''
DATABASE  = ''
S3_KEY    = ''
S3_SECRET = ''
S3_BUCKET = ''
S3_FOLDER = ''


logging.basicConfig(level=logging.INFO,
  format='%(asctime)s - vtiger_backup.py - %(levelname)-7s - %(message)s')
logger = logging.getLogger('vtiger_backup.py')


def check_call_output(args, cwd, stdout=subprocess.PIPE):
  """
  Call subprocess.Popen with the given arguments and wait for the child process to
  finish. Return the STDOUT and the return code
  """
  out = None
  err = None
  retcode = None

  logger.debug('<check_call_output> {0} for {1}'.format(args, cwd))
  try:
    output = subprocess.Popen(args, cwd=cwd, stdout=stdout, stderr=subprocess.PIPE)
    out, err = output.communicate()
    out = out.decode('utf-8').strip() if out else None
    err = err.decode('utf-8').strip() if err else None
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
 
def dump_db(dump_file):
  """
  Generate the dump of the vTiger database
  """
  logger.info('<dump_db> generating the vTiger database dump')

  dump_fd = open(dump_file, 'w')

  dump_output = check_call_output(['mysqldump', '-u', USER, '-p' + PASSWD,
    '--databases', DATABASE], cwd=None, stdout=dump_fd
  )

  dump_fd.close()

  logger.debug('<dump_db> '.format(dump_output))

  return dump_output['retcode']

def upload_db_to_s3(dump_file):
  """
  Upload the dump to the Amazon S3 folder
  """
  try:
    conn = S3Connection(S3_KEY, S3_SECRET)

    bucket = conn.get_bucket(S3_BUCKET)

    k = Key(bucket)
    k.key = '{0}/{1}'.format(S3_FOLDER, dump_file.split('/')[2])
    k.set_contents_from_filename(dump_file)
    logger.info('<upload_db_to_s3> uploaded dump to Amazon S3 with key {0}'.format(k.key))
  except Exception as e:
    logger.debug('<upload_db_to_s3> error: {0}'.format(e))


if __name__ == '__main__':
  logger.debug('start')

  try:
    if (USER == '' or PASSWD == '' or DATABASE == '' or
        S3_KEY == '' or S3_SECRET == ''):
      raise Exception('please configure the script correctly')

    dump_file = os.path.join('/tmp', '{0}-{1}.{2}'.format(
      'vtiger60-backup',
      datetime.datetime.now().strftime('%Y%m%d-%H%M'), 'sql'
    ))

    if dump_db(dump_file) == 0:
      upload_db_to_s3(dump_file)
    else:
      logger.error('error downloading dump from source database')

    if os.path.isfile(dump_file):
      os.remove(dump_file)
  except Exception as e:
    logger.error('Error: ' + str(e))

  logger.debug('finish')
