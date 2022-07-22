import os
import ssl
import json
import requests
from urllib.request import urlretrieve

try:
    from urllib.parse import urlencode, urlparse
except:
    from urllib import urlencode

from websocket import create_connection, WebSocketConnectionClosedException, WebSocketTimeoutException
from requests.auth import HTTPBasicAuth
import pkg_resources  # part of setuptools


def get_api_version_number():
    # This is what we send to the server when we confirm version compatibility.
    version = pkg_resources.require("mantaray-client")[0].version  # format major.minor.revision

    version_parts = version.split(".")
    return "mantaray-clientv{0}.{1}".format(version_parts[0], version_parts[1])


def get_version_number():
    return pkg_resources.require("mantaray-client")[0].version


def get_pretty_version_string():
    return "manta-ray-client version {0}".format(get_version_number())


class Notify(object):

    def __init__(self, session, ws):
        self._session = session
        self._ws = ws

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def close(self):
        self._ws.close()
        self._session.close()

    def recv(self):
        try:
            frame = self._ws.recv()
        except (WebSocketConnectionClosedException, WebSocketTimeoutException, OSError) as e:
            return None
        if not frame:
            return None
        return json.loads(frame)

    @classmethod
    def login(cls,
              https,
              host,
              port,
              api_key,
              sslopt={'cert_reqs': ssl.CERT_NONE}):

        session = requests.session()
        protocol = 'https' if https == '1' else 'http'
        websocket = 'wss' if https == '1' else 'ws'

        url = "{0}://{1}:{2}/api/api_login".format(protocol, host, port)
        r = session.post(url,
                         auth=HTTPBasicAuth(get_api_version_number(), api_key),
                         verify=False)
        r.raise_for_status()

        cookie = requests.utils.dict_from_cookiejar(session.cookies)
        cookie_str = '{0}={1}'.format('MWA_JOB_COOKIE',
                                      cookie['MWA_JOB_COOKIE'])

        ws_url = "{0}://{1}:{2}/api/job_results".format(websocket,
                                                        host,
                                                        port)

        ws = create_connection(ws_url,
                               header={'Cookie': cookie_str},
                               sslopt=sslopt)

        return Notify(session, ws)


class Session(object):

    def __init__(self,
                 https,
                 host,
                 port,
                 session,
                 verify):

        self.protocol = 'https' if https == '1' else 'http'
        self.websocket = 'wss' if https == '1' else 'ws'
        self.host = host
        self.port = port
        self.session = session
        self.verify = verify

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        self.close()

    def close(self):
        self.session.close()

    @classmethod
    def login(cls,
              https,
              host,
              port,
              api_key,
              verify=False):

        requests.packages.urllib3.disable_warnings()

        session = requests.session()
        protocol = 'https' if https == '1' else 'http'
        url = "{0}://{1}:{2}/api/api_login".format(protocol, host, port)
        with session.post(url,
                          auth=HTTPBasicAuth(get_api_version_number(), api_key),
                          verify=verify) as r:
            r.raise_for_status()

            return Session(https,
                           host,
                           port,
                           session,
                           verify=verify)

    def submit_conversion_job(self,
                              obs_id,
                              time_res,
                              freq_res,
                              edge_width,
                              conversion,
                              calibrate,
                              flags=[]):
        data = {'obs_id': obs_id,
                'timeres': time_res,
                'freqres': freq_res,
                'edgewidth': edge_width,
                'conversion': conversion,
                'calibrate': calibrate}
        data.update(dict.fromkeys(flags, 1))
        return self.submit_conversion_job_direct(data)

    def submit_conversion_job_direct(self, parameters):
        url = "{0}://{1}:{2}/api/conversion_job".format(self.protocol, self.host, self.port)
        with self.session.post(url,
                               parameters,
                               verify=self.verify) as r:
            r.raise_for_status()
            return r.json()

    def submit_download_job(self,
                            obs_id,
                            download_type):
        data = {'obs_id': obs_id,
                'download_type': download_type}
        return self.submit_download_job_direct(data)

    def submit_download_job_direct(self, parameters):
        url = "{0}://{1}:{2}/api/download_vis_job".format(self.protocol, self.host, self.port)
        with self.session.post(url,
                               parameters,
                               verify=self.verify) as r:
            r.raise_for_status()
            return r.json()

    def submit_voltage_job_direct(self, parameters):
        url = "{0}://{1}:{2}/api/voltage_job".format(self.protocol, self.host, self.port)
        with self.session.post(url,
                               parameters,
                               verify=self.verify) as r:
            r.raise_for_status()
            return r.json()

    def get_jobs(self):
        url = "{0}://{1}:{2}/api/get_jobs".format(self.protocol, self.host, self.port)
        with self.session.get(url, verify=self.verify) as r:
            r.raise_for_status()
            return r.json()

    def cancel_job(self, job_id):
        url = "{0}://{1}:{2}/api/cancel_job".format(self.protocol, self.host, self.port)
        with self.session.get(url,
                              params=urlencode({'job_id': job_id}),
                              verify=self.verify) as r:
            r.raise_for_status()

    def download_file_product(self,
                              job_id,
                              url,
                              output_path):

        with requests.get(url, stream=True, timeout=10) as r:
            r.raise_for_status()

            with open(output_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        return output_path