import requests
import logging
import json
import time
import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning

class TSMApi:
    METHOD_GET = 'GET'
    METHOD_POST = 'POST'
    METHOD_DELETE = 'DELETE'

    def __init__(self, url, port=8850, version=0.5):
        self.logger = logging.getLogger('TSMApi')
        self.server_url = '{}:{}'.format(url, port)
        self.api_version = version
        self.logger.debug('base_url: "{}", api_version: "{}"'.format(self.server_url, self.api_version))
        self.session = requests.Session()
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    def _build_url(self, endpoint, params=None):
        query_string = ''
        if params is not None:
            query_string = '?' + '&'.join(params)
        return '{0}/api/{1}/{2}{3}'.format(self.server_url, self.api_version, endpoint, query_string)

    def _requests_wraper(self, url, type=METHOD_GET, data=None, headers={}, json_data=None):
        if json:
            headers.update({'content-type': 'application/json'})
            data = json.dumps(json_data)
        self.logger.debug('{}:"{}", headers: "{}"'.format(type, url, headers))
        resp = self.session.request(type, url, data=data, headers=headers, verify=False)
        try:
            resp.raise_for_status()
        except Exception as e:
            self.logger.error('status code:{}, text:{}'.format(resp.status_code, resp.text))
            raise e
        else:
            if resp.status_code == 200:
                self.logger.debug('resp json: {}'.format(resp.json()))
                return resp.json()
            else:
                self.logger.debug('success')

    def login(self, username, password):
        url = self._build_url(endpoint='login')
        auth = {'authentication': {'name': username, 'password': password}}
        self._requests_wraper(url, self.METHOD_POST, json_data=auth)

    def start_backup(self, file, add_date=True, skip_verification=False, timeout=1800):
        if add_date:
            timestamp = time.time()
            date_string = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d_%H:%M:%S')
            backup_name = '{0}_{1}'.format(file, date_string)
        else:
            backup_name = file
        self.logger.debug('Start backup file:{}, skip-verification:{}, timeout:{}'.format(backup_name, skip_verification, timeout))
        backup_params = ['jobTimeoutSeconds={0}'.format(timeout), 'writePath={0}'.format(backup_name), 'skipVerification={0}'.format(skip_verification)]
        url = self._build_url(endpoint='backupFixedFile', params=backup_params)
        resp = self._requests_wraper(url, self.METHOD_POST)
        job_id = resp.get('asyncJob').get('id')
        return job_id

    def get_jobs(self):
        url = self._build_url(endpoint='asyncJobs')
        resp = self._requests_wraper(url, self.METHOD_GET)
        return resp.get('asyncJobs')

    def get_job(self, job_id):
        url = self._build_url(endpoint='asyncJobs/{}'.format(job_id))
        resp = self._requests_wraper(url, self.METHOD_GET)
        return resp.get('asyncJob')





