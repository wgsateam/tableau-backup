#!/usr/bin/env python3.6

"""
tableau-backup.py -- Runs tsm maintenance backup and redirects output to file

Usage:
  tableau-backup.py [-d]
  tableau-backup.py zsend [-d]
  tableau-backup.py test [-d]
  tableau-backup.py re [-d]
  tableau-backup.py site <siteid> [-d]

Options:
  -d               Debug mode
  --noop           Run in noop mode
  zsend            Send to zabbix '1'
  test             Run "tsm status -v"
  re               Run "tsm jobs reconnect"
  site <siteid>    Run backup site. tsm sites export


"""

import logging
import sys
import os
import shutil
import json
from docopt import docopt
import subprocess
import fcntl
import re
import selectors
from pyzabbix import ZabbixMetric, ZabbixSender # pip install py-zabbix
from logging.handlers import RotatingFileHandler

backup_folder = '/var/opt/tableau/tableau_server/data/tabsvc/files/backups'
run_args_backup = 'tsm maintenance backup'
run_args_test = 'tsm status -v'
run_args_reconnect = 'tsm jobs reconnect'
run_args_site = 'tsm sites export '
run_args_export_cong = f'tsm settings export -f {backup_folder}/settings.json'
pre_args = 'source /etc/profile.d/tableau_server.sh ; '
config_file = 'config.json'
script_home = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(script_home, config_file)



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
        self.l.info(f"Send {packet} to {self.server}")
        return ZabbixSender(zabbix_server=self.server).send(packet)

def setNonBlocking(fileobj):
    fl = fcntl.fcntl(fileobj.fileno(), fcntl.F_GETFL)
    fcntl.fcntl(fileobj.fileno(), fcntl.F_SETFL, fl | os.O_NONBLOCK)

def run_cmd(argz):
    l = logging.getLogger('main.run_cmd')
    try:
        proc = subprocess.Popen(argz, bufsize=0, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8', shell=True)
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
    return str(rc)

def main():
    global run_args
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
        config_data = open(config_path)
    except Exception as e:
        l.error(f"Error while reading from {config_path}: {e}")
        sys.exit(1)
    try:
        config = json.load(config_data)
    except Exception as e:
        l.error(f"Error while parsing {config_path}: {e}")
        sys.exit(1)
    l.debug(f"{config_path} was loaded")
    fh = RotatingFileHandler(config['logging']['file'], maxBytes=int(config['logging']['maxBytes']), backupCount=int(config['logging']['backupCount']))
    fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    l.addHandler(fh)
    z_sender = ZSender(config_file=config['zabbix']['config'])
    zabbix_item = config['zabbix']['backup_item']
    l.debug(f"zabbix_item: {zabbix_item}")
    run_args_login = pre_args + f"tsm login -u {config['tsm'].get('username')} -p {config['tsm'].get('password')}"
    exit_code = run_cmd(argz=run_args_login)
    l.info(f"Login exit code: {exit_code}")
    if argz.get('zsend'):
        z_sender.send(item=zabbix_item, value=1)
        sys.exit(0)
    elif argz.get('re'):
        run_args = run_args_reconnect
    elif argz.get('test'):
        run_args = run_args_test
    elif argz.get('site'):
        zabbix_item = config['zabbix']['sitesexport_item']
        run_args = f"{run_args_site} -id {argz.get('<siteid>')} -f {argz.get('<siteid>')} -ow"
    else:
        if config['tsm'].get('tsm_backup_parms'):
            run_args = run_args_backup + f" {config['tsm'].get('tsm_backup_parms')} "
        if config['tsm'].get('backup_filename'):
            run_args = run_args + f" -f {config['tsm'].get('backup_filename')} "
        l.info(f"remove all files from {backup_folder}")
        for file in os.listdir(backup_folder):
            file_path = os.path.join(backup_folder, file)
            try:
                if os.path.isfile(file_path):
                    l.info(f"Remove {file_path}")
                    os.remove(file_path)
            except Exception as e:
                l.error(f"Error while cleaning {backup_folder}: {e}")
                z_sender.send(item=zabbix_item, value=1)


    l.info(f"Run \"{run_args_export_cong}\"")
    exit_code = run_cmd(argz=run_args_export_cong)
    l.info(f'exit code: {exit_code}')

    run_args = pre_args + run_args
    l.info(f"Run \"{run_args}\"")
    exit_code = run_cmd(argz=run_args)
    l.info(f'exit code: {exit_code}')
    z_sender.send(item=zabbix_item, value=exit_code)

if __name__ == '__main__':
    main()
