import os
import ssl
import json
import requests

try:
    from urllib.parse import urlencode
except:
    from urllib import urlencode

from websocket import create_connection
from requests.auth import HTTPBasicAuth


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
        frame = self._ws.recv()
        if not frame:
            return None
        return json.loads(frame)

    @classmethod
    def login(cls,
              host,
              port,
              username,
              password,
              sslopt={'cert_reqs': ssl.CERT_NONE}):

        session = requests.session()
        url = "https://{0}:{1}/api/login".format(host, port)
        r = session.post(url,
                         auth=HTTPBasicAuth(username, password),
                         verify=False)
        r.raise_for_status()

        cookie = requests.utils.dict_from_cookiejar(session.cookies)
        cookie_str = '{0}={1}'.format('MWA_JOB_COOKIE',
                                      cookie['MWA_JOB_COOKIE'])

        ws_url = "wss://{0}:{1}/api/job_results".format(host,
                                                        port)
        ws = create_connection(ws_url,
                               header={'Cookie': cookie_str},
                               sslopt=sslopt)

        return Notify(session, ws)


class Session(object):

    def __init__(self,
                 host,
                 port,
                 session,
                 verify):

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
              host,
              port,
              username,
              password,
              verify=False):

        requests.packages.urllib3.disable_warnings()

        session = requests.session()
        url = "https://{0}:{1}/api/login".format(host, port)
        with session.post(url,
                          auth=HTTPBasicAuth(username, password),
                          verify=verify) as r:
            r.raise_for_status()

            return Session(host,
                           port,
                           session,
                           verify=verify)

    def submit_conversion_job(self,
                              obs_id,
                              time_res,
                              freq_res,
                              edge_width,
                              conversion,
                              flags=[]):
        data = {'obs_id': obs_id,
                'timeres': time_res,
                'freqres': freq_res,
                'edgewidth': edge_width,
                'conversion': conversion}
        data.update(dict.fromkeys(flags, 1))
        return self.submit_conversion_job_direct(data)

    def submit_conversion_job_direct(self, parameters):
        url = "https://{0}:{1}/api/conversion_job".format(self.host, self.port)
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
        url = "https://{0}:{1}/api/download_vis_job".format(self.host, self.port)
        with self.session.post(url,
                               parameters,
                               verify=self.verify) as r:
            r.raise_for_status()
            return r.json()

    def get_jobs(self):
        url = "https://{0}:{1}/api/get_jobs".format(self.host, self.port)
        with self.session.get(url, verify=self.verify) as r:
            r.raise_for_status()
            return r.json()

    def cancel_job(self, job_id):
        url = "https://{0}:{1}/api/cancel_job".format(self.host, self.port)
        with self.session.post(url,
                               data={'job_id': job_id},
                               verify=self.verify) as r:
            r.raise_for_status()

    def download_file_product(self,
                              job_id,
                              filename,
                              output_path,
                              chunk_size=65536):

        url = "https://{0}:{1}/api/download".format(self.host, self.port)
        params = {'file_name': filename, 'job_id': job_id}
        with self.session.get(url=url,
                              params=urlencode(params),
                              stream=True,
                              verify=self.verify) as r:
            r.raise_for_status()

            try:
                os.makedirs(output_path)
            except OSError:
                pass

            full_output_path = '{0}/{1}'.format(output_path, filename)
            with open(full_output_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)

            file_size = os.path.getsize(full_output_path)
            content_length = int(r.headers['content-length'])
            if file_size != content_length:
                raise Exception('Stream error. id: %s file: %s'
                                % (job_id, filename))

            return full_output_path
