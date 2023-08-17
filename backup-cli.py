#!/usr/bin/env python3

import click
import logging
import os
import time
import json
import re
import hashlib
from sys import stdout
from TSMApi import TSMApi
from pyzabbix import ZabbixMetric, ZabbixSender # pip install py-zabbix

config_file = 'config.json'
script_home = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(script_home, config_file)


class TableauBackupCLI:
    def __init__(self, config_path, debug):
        self._logger = logging.getLogger('TableauBackupCLI')
        if debug:
            self._logger.setLevel(logging.DEBUG)
        else:
            self._logger.setLevel(logging.INFO)
        h1 = logging.StreamHandler(stdout)
        f1 = logging.Formatter('%(name)s: %(message)s')
        h1.setFormatter(f1)
        self._logger.addHandler(h1)
        self._logger.debug('Run in debug mode')
        self._config_path = config_path

    def _load_config(self):
        self._logger.debug('Load config from {}'.format(self._config_path))
        with open(self._config_path) as json_file:
            self.config =json.load(json_file)

    def _login_in_tsm(self):
        self._load_config()
        creds = self.config.get('tsm')
        self._logger.debug('Login in {} with {} username'.format(creds.get('url'), creds.get('username')))
        self.tsm = TSMApi(url=creds.get('url'))
        self.tsm.login(username=creds.get('username'), password=creds.get('password'))

    def _poll_job(self, job_id, print_msg=True, poll_interval=1):
        msg_timestamp = 0
        while True:
            resp = self.tsm.get_job(job_id=job_id)
            job_status = resp['status']
            if print_msg:
                for d in resp['detailedProgress']['progressNotes']:
                    if msg_timestamp < d['timestamp']:
                        click.echo('{}: {} - {}'.format(d['step'], d['status'], d['message']))
                        msg_timestamp = d['timestamp']
            if job_status not in ['Created', 'Running']:
                if print_msg:
                    click.echo('------------------------')
                    click.echo('{}: {}'.format(resp['status'], resp['statusMessage']))
                return resp['status']
            time.sleep(poll_interval)
    def _clean_backup_dir(self):
        backup_dir = self.config['backup']['backup_dir']
        for file in os.listdir(backup_dir):
            file_path = os.path.join(backup_dir, file)
            try:
                if os.path.isfile(file_path):
                    self._logger.info(f"Remove {file_path}")
                    os.remove(file_path)
            except Exception as e:
                self._logger.error(f"Error while cleaning {backup_dir}: {e}")

    def _send_to_zabbix(self, value):
        zab_conf = self.config.get('zabbix')
        if not zab_conf:
            click.echo('There is no zabbix section in the config file')
        try:
            zabbix_file = open(zab_conf.get('config')).read()
        except Exception as e:
            self.l.error(f"Error reading from file{zab_conf.get('config')}: {e}")
            raise e
        zab_server = re.search(r'ServerActive=(.+)', zabbix_file).group(1)
        zab_hostname = re.search(r'Hostname=(.+)', zabbix_file).group(1)
        packet = [ZabbixMetric(zab_hostname, zab_conf.get('backup_item'), value)]
        self._logger.debug(f'Send to {zab_server} packet:{packet}')
        return ZabbixSender(zabbix_server=zab_server).send(packet)

    def calculate_sha256(file_path):
        file_path = "/var/opt/tableau/tableau_server/data/tabsvc/files/backups/" + file_path
        sha256_hash = hashlib.sha256()
        with open(file_path, 'rb') as file:
            for chunk in iter(lambda: file.read(4096), b''):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()

    def write_sha256sum_to_file(file_path, sha256sum):
        file_path = "/var/opt/tableau/tableau_server/data/tabsvc/files/backups/" + file_path
        with open(file_path, 'w') as file:
            file.write(sha256sum)       

    def start(self, file, add_date, wait, zabbix, zab_test, skip_verification, timeout, clean_backup_dir, override_disk_space_check):
        self._login_in_tsm()
        if zab_test:
            click.echo('Sending to Zabbix 1')
            zab_ans = self._send_to_zabbix(1)
            self._logger.debug(f'zab_ans: {zab_ans}')
            return 0

        if clean_backup_dir:
            self._clean_backup_dir()
        self._logger.debug('Start backup: file:{}, add_date:{}, skip_verification:{}, timeout:{}, override_disk_space_check:{}'.format(file, add_date, skip_verification, timeout, override_disk_space_check))
        job_id, backup_name = self.tsm.start_backup(file, add_date, skip_verification, timeout, override_disk_space_check)
        click.echo('job id: {}'.format(job_id))
        if wait:
            job_status = self._poll_job(job_id)
            if job_status == 'Failed':
                quit(1)
            sha256sum = self.calculate_sha256(backup_name)
            self.write_sha256sum_to_file(backup_name+".sha256", sha256sum)
        if zabbix:
            job_status = self._poll_job(job_id)
            if job_status == 'Failed':
                self._send_to_zabbix(1)
            else:
                self._send_to_zabbix(0)


    def list_jobs(self):
        click.echo('coming soon...')

    def get_job(self,id):
        click.echo('coming soon...')

    def reconnect(self):
        click.echo('coming soon...')

@click.group()
@click.option('--config_path', default=config_path)
@click.option('-d', '--debug', default=False, is_flag=True)
@click.pass_context
def cli(ctx, config_path, debug):
    ctx.obj = TableauBackupCLI(config_path, debug)


@cli.command()
@click.option('--file', help='Name of backup file.', default='backup', show_default=True)
@click.option('--date', help='Appends the current date to the backup file name.', is_flag=True, default=True, show_default=True)
@click.option('--wait', help='Wait for end job.', is_flag=True, default=False, show_default=True)
@click.option('--zabbix', help='Wait and send job result to Zabbix.', is_flag=True, default=True, show_default=True)
@click.option('--zab_test', help='Send to Zabbix 1 without run job.', is_flag=True, default=False, show_default=True)
@click.option('--skip_verification', help='Do not verify integrity of the database backup.', is_flag=True, default=False, show_default=True)
@click.option('--timeout', help='Seconds to wait for command to finish', type=int, default=86400, show_default=True)
@click.option('--override_disk_space_check', help='Attempt to generate backup, despite low disk space warning.', is_flag=True, default=False, show_default=True)
@click.option('--clean_backup_dir', help='Remove all files in backup dir', is_flag=True, default=False, show_default=True)
@click.pass_obj
def start(tbcli, file, date, wait, zabbix, zab_test, skip_verification, timeout, clean_backup_dir, override_disk_space_check):
    '''Start a backup'''
    tbcli.start(file, date, wait, zabbix, zab_test, skip_verification, timeout, clean_backup_dir, override_disk_space_check)

@cli.command()
@click.pass_obj
def list(tbcli):
    '''Get a list of TSM backup jobs.'''
    tbcli.list_jobs()

@cli.command()
@click.pass_obj
def job(tbcli, id):
    '''Get the state of a previously started backup job.'''
    tbcli.get_job(id)

@cli.command()
@click.pass_obj
def latest(tbcli):
    '''Get the state of the last backup job'''
    tbcli.reconnect()


if __name__ == '__main__':
    cli()
