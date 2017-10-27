import os
import csv
import sys
import Queue
import requests

from threading import Thread, RLock
from optparse import OptionParser

from mantaray.api import Notify, Session


class ParseException(Exception):

    def __init__(self, *arg):
        super(ParseException, self).__init__(*arg)
        self._line_num = None
        self._row = None

    @property
    def line_num(self):
        return self._line_num

    @property
    def row(self):
        return self._row

    @line_num.setter
    def line_num(self, value):
        self._line_num = value

    @row.setter
    def row(self, value):
        self._row = value


def parse_row(row):
    try:
        if row[1] == 'c':
            return ('submit_conversion_job',
                    (row[0], row[2], row[3], row[4], row[5]))
        elif row[1] == 'd':
            return ('submit_download_job',
                    (row[0], row[2]))
        else:
            raise ParseException('unknown type')

    except ParseException:
        raise
    except Exception:
        raise ParseException()


def parse_csv(filename):
    result = []
    with open(filename, 'rU') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            try:
                result.append(parse_row(row))
            except ParseException as e:
                e.line_num = reader.line_num
                e.row = row
                raise e

    return result


def submit_jobs(session, jobs, status_queue):
    submitted_jobs = []
    for job in jobs:
        func = getattr(session, job[0])
        job_response = func(*job[1])
        status_queue.put('Submitted job: %s ' % (job_response['job_id'],))
        submitted_jobs.append(job_response['job_id'])

    return submitted_jobs


def _remove_submitted(submit_lock,
                      submitted_jobs,
                      job_id):
    try:
        with submit_lock:
            submitted_jobs.remove(job_id)
    except:
        pass


def download_func(submit_lock,
                  submitted_jobs,
                  download_queue,
                  result_queue,
                  status_queue,
                  session,
                  output_dir):
    while True:
        item = download_queue.get()
        if not item:
            break

        job_id = int(item['row']['id'])
        products = item['row']['product']['files']

        for prod in products:
            try:
                msg = 'Download complete. id: %s file: %s' % \
                      (job_id, prod[0])

                file_path = "%s/%s" % (output_dir, prod[0])
                if os.path.isfile(file_path):
                    if os.path.getsize(file_path) == prod[1]:
                        status_queue.put(msg)
                        continue

                session.download_file_product(job_id, prod[0], output_dir)
                status_queue.put(msg)

            except Exception as e:
                result_queue.put(e)
                continue

        _remove_submitted(submit_lock,
                          submitted_jobs,
                          job_id)


def status_func(status_queue):
    while True:
        status = status_queue.get()
        if not status:
            break

        print(status)
        sys.stdout.flush()


def notify_func(notify,
                submit_lock,
                submitted_jobs,
                download_queue,
                result_queue,
                status_queue):
    while True:
        item = notify.recv()
        if not item:
            result_queue.put(None)
            break

        action = item['action']
        job_id = int(item['row']['id'])
        job_state = item['row']['job_state']
        job_type = item['row']['job_type']
        job_params = item['row']['job_params']

        with submit_lock:
            if action == 'DELETE':
                _remove_submitted(submit_lock,
                                  submitted_jobs,
                                  job_id)
                continue

            msg = 'Job id: %s type: %s params: %s' % (job_id, job_type, job_params)

            if job_id in submitted_jobs:
                if job_state == 0:
                    status_queue.put("%s: %s" % ('Queued', msg))

                elif job_state == 1:
                    status_queue.put("%s: %s" % ('Processing', msg))

                elif job_state == 2:
                    status_queue.put("%s: %s" % ('Downloading', msg))
                    download_queue.put(item)

                elif job_state == 3:
                    error_text = item['row']['error_text']
                    msg = "%s: %s; %s" % ('Error', error_text, msg)
                    result_queue.put(msg)

                    _remove_submitted(submit_lock,
                                      submitted_jobs,
                                      job_id)

                elif job_state == 4:
                    msg = "%s: %s" % ('Expired', msg)
                    result_queue.put(msg)

                    _remove_submitted(submit_lock,
                                      submitted_jobs,
                                      job_id)

                elif job_state == 5:
                    # do not consider cancelled as an error
                    msg = "%s: %s" % ('Cancelled', msg)
                    status_queue.put(msg)

                    _remove_submitted(submit_lock,
                                      submitted_jobs,
                                      job_id)


def main():
    parser = OptionParser()
    parser.add_option("-c", "--csv", dest="csvfile",
                      help="csv job file", metavar="FILE")

    parser.add_option("-d", "--dir", dest="outdir",
                      help="download directory", metavar="DIR")

    (options, args) = parser.parse_args()

    if options.csvfile is None:
        raise Exception('csvfile not specified')

    outdir = './'
    if options.outdir:
        outdir = options.outdir

    host = os.environ.get('ASVO_HOST', None)
    if not host:
        raise Exception('ASVO_HOST env variable not defined')

    port = os.environ.get('ASVO_PORT', None)
    if not port:
        raise Exception('ASVO_PORT env variable not defined')

    user = os.environ.get('ASVO_USER', None)
    if not user:
        raise Exception('ASVO_USER env variable not defined')

    passwd = os.environ.get('ASVO_PASS', None)
    if not passwd:
        raise Exception('ASVO_PASS env variable not defined')

    status_queue = Queue.Queue()
    status_thread = Thread(target=status_func, args=(status_queue,))
    status_thread.daemon = True
    status_thread.start()

    download_queue = Queue.Queue()
    result_queue = Queue.Queue()
    submit_lock = RLock()

    jobs_to_submit = parse_csv(options.csvfile)

    params = (host,
              port,
              user,
              passwd)

    session = Session.login(*params)
    submitted_jobs = submit_jobs(session, jobs_to_submit, status_queue)

    notify = Notify.login(*params)
    notify_thread = Thread(target=notify_func, args=(notify,
                                                     submit_lock,
                                                     submitted_jobs,
                                                     download_queue,
                                                     result_queue,
                                                     status_queue))

    notify_thread.daemon = True
    notify_thread.start()

    threads = []
    for i in range(4):
        t = Thread(target=download_func, args=(submit_lock,
                                               submitted_jobs,
                                               download_queue,
                                               result_queue,
                                               status_queue,
                                               session,
                                               outdir))
        threads.append(t)
        t.daemon = True
        t.start()

    results = []
    while True:
        with submit_lock:
            if len(submitted_jobs) == 0:
                break

        try:
            r = result_queue.get(timeout=1)
            if not r:
                raise Exception('Control connection lost, exiting')
            results.append(r)
        except Queue.Empty:
            continue

    for _ in threads:
        download_queue.put(None)

    for t in threads:
        t.join()

    notify.close()
    notify_thread.join()

    status_queue.put(None)
    status_thread.join()

    while not result_queue.empty():
        r = result_queue.get()
        if not r:
            continue
        results.append(r)

    results_len = len(results)

    if results_len > 0:
        print
        print('There were errors:')

    for r in results:
        print(r)

    if results_len > 0:
        sys.exit(4)

if __name__ == "__main__":
    try:
        main()
    except ParseException as e:
        print('Invalid row: %s line num: %s' %
              (e.row, e.line_num))
        sys.exit(3)

    except requests.exceptions.HTTPError as re:
        print(re.response.text)
        sys.exit(2)

    except Exception as exp:
        print(exp)
        sys.exit(1)