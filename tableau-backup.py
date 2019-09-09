#!/usr/bin/env python3.6

"""
tableau-backup.py -- Runs tsm maintenance backup and redirects output to file

Usage:
  tableau-backup.py [-d]
  tableau-backup.py zsend [-d]
  tableau-backup.py test [-d]

Options:
  -d               Debug mode
  --noop           Run in noop mode
  zsend            Send to zabbix '1'
  test             Run "tsm status -v"


"""

import logging
import sys
import os
import json
from docopt import docopt
import subprocess
import fcntl
import re
import selectors
from pyzabbix import ZabbixMetric, ZabbixSender # pip install py-zabbix
from logging.handlers import RotatingFileHandler

test_run_args = ['tsm', 'status', '-v']
run_args = ['tsm', 'maintenance', 'backup']
config_file = 'config.json'

class ZSender(object):
    def __init__(self, config_file):
        self.l = logging.getLogger('main.zabbix_send')
        try:
            zabbix_config = open(config_file).read()
        except Exception as e:
            self.l.error(f"Error reading from file{config_file}: {e}")
            sys.exit(1)
        self.server = re.search(r'ServerActive=(.+)', zabbix_config).group(1)
        self.l.debug(f"self.server: {self.server}")
        self.hostname = re.search(r'Hostname=(.+)', zabbix_config).group(1)
        self.l.debug(f"self.hostname: {self.hostname}")

    def send(self, item, value):
        packet = [ZabbixMetric(self.hostname, item, value)]
        self.l.debug(f"Send {packet} to {self.server}")
        return ZabbixSender(zabbix_server=self.server).send(packet)

def setNonBlocking(fileobj):
    fl = fcntl.fcntl(fileobj.fileno(), fcntl.F_GETFL)
    fcntl.fcntl(fileobj.fileno(), fcntl.F_SETFL, fl | os.O_NONBLOCK)

def run_cmd(argz):
    l = logging.getLogger('main.run_cmd')
    l.debug(f"Run {argz}")
    try:
        proc = subprocess.Popen(test_run_args, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
    except Exception as e:
        l.error(e)
        return 1
    selector = selectors.DefaultSelector()
    key_stdout = selector.register(proc.stdout, selectors.EVENT_READ)
    key_stderr = selector.register(proc.stderr, selectors.EVENT_READ)
    setNonBlocking(proc.stdout)
    setNonBlocking(proc.stderr)

    rc = None
    while rc is None:
        if rc is None:
            rc = proc.poll()
        for key, events in selector.select(timeout=1):
            if key == key_stdout:
                text = proc.stdout.read()
                _ = [l.info(t) for t in list(filter(None, text.split('\n')))]
            elif key == key_stderr:
                text = proc.stderr.read()
                _ = [l.error(t) for t in list(filter(None, text.split('\n')))]

    exit_code = str(rc)
    l.info(f'exit code: {exit_code}')
    return exit_code

def main():
    l = logging.getLogger('main')
    if '-d' in sys.argv:
        print(f"argv: {sys.argv}")
        l.setLevel(logging.DEBUG)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        l.addHandler(sh)
        l.debug("Debug mode")
    elif sys.stdout.isatty():
        l.setLevel(logging.INFO)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        l.addHandler(sh)
        l.debug("sys.stdout.isatty() is True")

    else:
        l.setLevel(logging.INFO)
    argz = docopt(__doc__, argv=sys.argv[1:])
    l.debug(f"argz: {argz}")
    try:
        config_data = open(config_file)
    except Exception as e:
        l.error(f"Error while reading from {config_file}: {e}")
        sys.exit(1)
    try:
        config = json.load(config_data)
    except Exception as e:
        l.error(f"Error while parsing {config_file}: {e}")
        sys.exit(1)
    l.debug(f"{config_file} was loaded")
    fh = RotatingFileHandler(config['logging']['file'], maxBytes=int(config['logging']['maxBytes']), backupCount=int(config['logging']['backupCount']))
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    l.addHandler(fh)
    z_sender = ZSender(config_file=config['zabbix']['config'])
    zabbix_item = config['zabbix']['item']
    l.debug(f"zabbix_item: {zabbix_item}")

    if argz.get('zsend'):
        z_sender.send(item=zabbix_item, value=1)
        sys.exit(0)
    global run_args

    if argz.get('test'):
        run_args = test_run_args
    else:
        if config['tsm'].get('tsm_backup_parms'):
            run_args = run_args + config['tsm'].get('tsm_backup_parms').split()
        if config['tsm'].get('backup_filename'):
            run_args = run_args + ['-f', config['tsm'].get('backup_filename')]

    exit_code = run_cmd(argz=run_args)
    z_sender.send(item=zabbix_item, value=exit_code)

if __name__ == '__main__':
    main()
