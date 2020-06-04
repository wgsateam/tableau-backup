#!/usr/bin/env python3

import click
import logging
import os
import time
import json
from sys import stdout
from TSMApi import TSMApi

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

    def start(self, file, add_date, wait, skip_verification, timeout):
        self._login_in_tsm()
        self._logger.debug('Start backup: file:{}, add_date:{}, skip_verification:{}, timeout:{}'.format(file, add_date, skip_verification, timeout))
        job_id = self.tsm.start_backup(file, add_date, skip_verification, timeout)
        click.echo('job id: {}'.format(job_id))
        if wait:
            job_status = self._poll_job(job_id)
            if job_status == 'Failed':
                quit(1)

    def list_jobs(self):
        click.echo('coming soon...')

    def get_job(self,id):
        click.echo('coming soon...')

    def reconnect(self):
        click.echo('coming soon...')

@click.group()
@click.option('--config_path', default=config_path)
@click.option('--debug/--no-debug', default=False)
@click.pass_context
def cli(ctx, config_path, debug):
    ctx.obj = TableauBackupCLI(config_path, debug)


@cli.command()
@click.option('--file', help='Name of backup file.', default='backup', show_default=True)
@click.option('--date', help='Appends the current date to the backup file name.', is_flag=True, default=True, show_default=True)
@click.option('--wait', help='Wait for end job.', is_flag=True, default=False, show_default=True)
@click.option('--skip_verification', help='Do not verify integrity of the database backup.', is_flag=True, default=False, show_default=True)
@click.option('--timeout', help='Seconds to wait for command to finish', type=int, default='1800', show_default=True)
@click.pass_obj
def start(tbcli, file, date, wait, skip_verification, timeout):
    '''Start a backup'''
    tbcli.start(file, date,wait, skip_verification, timeout)

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