import os
import ssl
import csv
import sys
import requests
import json

try:
    from queue import Queue, Empty
except:
    from Queue import Queue, Empty

from threading import Thread, RLock
from optparse import OptionParser
from colorama import init, Fore, Style
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
        job_type = None
        params = dict()
        for cell in row:
            cell = cell.replace(" ", "")
            val = cell.split('=')

            if len(val) != 2:
                raise ParseException('invalid cell format, must be key=value')

            key = val[0]
            val = val[1]

            if key is None or val is None:
                raise ParseException('invalid cell format: None')

            if key == 'job_type':
                if val == 'c':
                    job_type = 'submit_conversion_job_direct'
                elif val == 'd':
                    job_type = 'submit_download_job_direct'
                else:
                    raise ParseException('unknown job_type')
            else:
                params[key] = val

        if job_type is None:
            raise ParseException('job_type cell not defined')

        # check parameters for each job type: create a validate function in api

        return [job_type, params]

    except ParseException:
        raise
    except Exception:
        raise ParseException()


def parse_csv(filename):
    result = []
    with open(filename, 'rU') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if not row:
                continue
            if row[0].strip().startswith('#'):
                continue
            try:
                result.append(parse_row(row))
            except ParseException as e:
                e.line_num = reader.line_num
                e.row = row
                raise e

    return result


def submit_jobs(session, jobs_to_submit, status_queue):
    submitted_jobs = []
    job_number = 0  # used to help point the user to which csv job had a submission problem

    # Get the list of the users existing jobs
    existing_jobs = get_job_list(session)

    for job in jobs_to_submit:
        job_number = job_number + 1

        # Get the function from the session object e.g. session.submit_conversion_job_direct
        func = getattr(session, job[0])

        try:
            # Call the session function
            job_response = func(job[1])
        except Exception:
            print("Error submitting job #{0} from csvfile. Details below:".format(job_number))
            raise
        else:
            # Get the job_id returned
            new_job_id = job_response['job_id']
            job_exists = False

            # Loop through the users existing jobs, looking for the new_job_id
            for e in existing_jobs:
                if e["row"]["id"] == new_job_id:
                    job_exists = True
                    break

            # Let user know if it was a new job or if that job already existed
            if job_exists:
                status_queue.put('Job: %s already exists' % (new_job_id,))
            else:
                status_queue.put('Submitted job: %s ' % (new_job_id,))

            submitted_jobs.append(new_job_id)

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
                # Some old downloads may not have sha1 information
                if len(prod)==3:
                    server_sha1 = prod[2]
                else:
                    server_sha1 = "(not defined)"

                msg = '%sDownload complete:%s Job id: %s%s%s file: %s%s%s server-sha1: %s%s%s' % \
                      (Fore.GREEN, Fore.RESET, Fore.LIGHTWHITE_EX+Style.BRIGHT, job_id,
                       Fore.RESET, Fore.LIGHTWHITE_EX+Style.BRIGHT, prod[0],
                       Fore.RESET, Fore.LIGHTWHITE_EX+Style.BRIGHT, server_sha1, Fore.RESET)

                file_path = "%s/%s" % (output_dir, prod[0])
                if os.path.isfile(file_path):
                    if os.path.getsize(file_path) == prod[1]:
                        status_queue.put(msg)
                        continue

                status_queue.put('%sDownloading:%s Job id: %s%s%s file: %s%s%s size: %s%s%s bytes'
                                 % (Fore.MAGENTA, Fore.RESET, Fore.LIGHTWHITE_EX+Style.BRIGHT, job_id,
                                    Fore.RESET, Fore.LIGHTWHITE_EX+Style.BRIGHT, prod[0],
                                    Fore.RESET, Fore.LIGHTWHITE_EX + Style.BRIGHT, prod[1], Fore.RESET))
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
                status_queue,
                verbose):

    while True:
        item = notify.recv()
        if not item:
            result_queue.put(None)
            break

        action = item['action']
        job_id = int(item['row']['id'])
        job_state = item['row']['job_state']

        msg = get_status_message(item, verbose)

        with submit_lock:
            if action == 'DELETE':
                status_queue.put(msg)

                _remove_submitted(submit_lock,
                                  submitted_jobs,
                                  job_id)
                continue

            if job_id in submitted_jobs:
                if job_state == 0:
                    status_queue.put(msg)

                elif job_state == 1:
                    status_queue.put(msg)

                elif job_state == 2:
                    status_queue.put(msg)

                    download_queue.put(item)

                elif job_state == 3:
                    result_queue.put(msg)

                    _remove_submitted(submit_lock,
                                      submitted_jobs,
                                      job_id)

                elif job_state == 4:
                    result_queue.put(msg)

                    _remove_submitted(submit_lock,
                                      submitted_jobs,
                                      job_id)

                elif job_state == 5:
                    # do not consider cancelled as an error
                    status_queue.put(msg)

                    _remove_submitted(submit_lock,
                                      submitted_jobs,
                                      job_id)


def get_status_message(item, verbose):
    # Format the status message
    action = item['action']
    job_id = int(item['row']['id'])
    job_state = item['row']['job_state']
    job_type = item['row']['job_type']
    job_params = item['row']['job_params']
    error_text = item['row']['error_text']

    job_type_values = {
        0: "conversion",
        1: "download visibilities",
        2: "download metadata",
        3: "download_voltage",  # not implemented
        4: "cancel job"
    }
    job_type_desc = job_type_values.get(job_type)
    obs_id = job_params["obs_id"]

    msg = '%sJob id: %s%s %sObs id: %s%s%s type: %s%s%s' % (Fore.RESET, Fore.LIGHTWHITE_EX + Style.BRIGHT,
                                                            job_id,
                                                            Fore.RESET, Fore.LIGHTWHITE_EX + Style.BRIGHT,
                                                            obs_id,
                                                            Fore.RESET, Fore.LIGHTWHITE_EX + Style.BRIGHT,
                                                            job_type_desc,
                                                            Fore.RESET)

    if verbose:
        msg = msg + '%s typeid: %s%s%s params: %s%s%s' % (Fore.RESET, Fore.LIGHTWHITE_EX + Style.BRIGHT,
                                                          job_type,
                                                          Fore.RESET, Fore.LIGHTWHITE_EX + Style.BRIGHT,
                                                          job_params,
                                                          Fore.RESET)

    if action == 'DELETE':
        msg = "%s%s: %s" % (Fore.RED, 'Deleted', msg)
    else:
        if job_state == 0:
            msg = "%s%s: %s" % (Fore.MAGENTA, 'Queued', msg)

        elif job_state == 1:
            msg = "%s%s: %s" % (Fore.BLUE, 'Processing', msg)

        elif job_state == 2:
            # Get the products and file sizes
            products = item['row']['product']['files']
            total_size = 0

            # loop through any products and get their size in bytes
            for prod in products:
                file_size = int(prod[1])
                total_size = total_size + file_size

            msg = "%s%s: %s %ssize: %s%s bytes" % (Fore.MAGENTA, 'Ready for Download', msg, Fore.RESET, Fore.LIGHTWHITE_EX + Style.BRIGHT, total_size)

        elif job_state == 3:
            msg = "%s%s: %s; %s" % (Fore.RED, 'Error', error_text, msg)

        elif job_state == 4:
            msg = "%s: %s" % ('Expired', msg)

        elif job_state == 5:
            msg = "%s%s: %s" % (Fore.RED, 'Cancelled', msg)

    return msg


def get_job_list(session):
    jobs = []

    try:
        # Get all the user's jobs via the API
        result = session.get_jobs()

        if result:
            for r in result:
                job = json.loads(r)
                jobs.append(job)

        return jobs

    except Exception as e:
        # Error getting job list
        raise Exception("Could not obtain jobs list from server: {0}".format(e))


def get_jobs_status(session, status_queue, verbose):
    # Returns the number of jobs the user has and places a status message for each one
    jobs = get_job_list(session)

    if jobs:
        for j in jobs:
            msg = get_status_message(j, verbose)
            status_queue.put(msg)

    return len(jobs)


def enqueue_all_ready_to_download_jobs(session, download_queue, status_queue, verbose):
    submitted_jobs = []
    jobs = get_job_list(session)

    for j in jobs:
        job_state = j['row']['job_state']

        # Check is ready for download
        if job_state == 2:
            job_id = j['row']['id']
            submitted_jobs.append(job_id)
            msg = get_status_message(j, verbose)
            status_queue.put(msg)
            download_queue.put(j)

    return submitted_jobs


def check_job_is_downloadable_and_enqueue(session, download_queue, job_id):
    submitted_jobs = []
    jobs = get_job_list(session)
    found_job = None

    # Check this is job owned by the user
    for j in jobs:
        j_job_id = int(j['row']['id'])

        if j_job_id == int(job_id):
            found_job = j
            break

    if found_job:
        # Check is ready for download
        job_state = found_job['row']['job_state']

        msg = get_status_message(found_job, False)

        if job_state != 2:
            raise Exception("Error: unable to download Job Id {0}. Invalid job state- job not ready for "
                            "download:\n{1}".format(job_id, msg))

        # Put this in the download queue
        submitted_jobs.append(job_id)
        download_queue.put(found_job)
        return submitted_jobs

    else:
        # not a valid job
        raise Exception("Error: Job Id {0} is not a valid job, has expired or is not owned by you.".format(job_id))


def mwa_client():
    usage = "\nmwa_client -c csvfile -d destdir           " \
            "Submit jobs in the csv file, monitor them, then download the files, then exit" \
            "\nmwa_client -c csvfile -s                   " \
            "Submit jobs in the csv file, then exit" \
            "\nmwa_client -d destdir -w JOBID             " \
            "Download the job id (assuming it is ready to download), then exit" \
            "\nmwa_client -d destdir -w 0                 " \
            "Download any ready to download jobs, then exit" \
            "\nmwa_client -l                              " \
            "List all of your jobs and their status, then exit" \
            "\n\nThe mwa_client is a command-line tool for submitting, monitoring and downloading jobs from the MWA " \
            "ASVO (https://asvo.mwatelescope.org). Please see README.md for csv file format and other details."

    parser = OptionParser(usage)
    parser.add_option("-c", "--csv", dest="csvfile",
                      help="csv job file", metavar="FILE")

    parser.add_option("-d", "--dir", dest="outdir",
                      help="download directory", metavar="DIR")

    parser.add_option("-s", "--submit-only", action="store_true", dest="submit_only",
                      help="submit job(s) from csv file then exit (-d is ignored)", default=False)

    parser.add_option("-l", "--list-only", action="store_true", dest="list_only",
                      help="List the user's active job(s) and exit immediately (-s, -c & -d are ignored)", default=False)

    parser.add_option("-w", "--download-only", action="store", dest="download_job_id", type="int",
                      help="Download the job id (-w DOWNLOAD_JOB_ID), if it is ready; or all downloadable jobs (-w 0)"
                           ", then exit (-s, -c & -l are ignored)", default=None)

    parser.add_option("-v", "--verbose", action="store_true", dest="verbose",
                      help="verbose output", default=False)

    (options, args) = parser.parse_args()

    # Figure out what mode we are running in, based on the command line args
    mode_submit_only = (options.submit_only is True)
    mode_list_only = (options.list_only is True)
    mode_download_only = not (options.download_job_id is None)

    # submit-only, list-only and download-only are mutually exclusive
    if (mode_submit_only and (mode_download_only or mode_list_only)) \
        or (mode_download_only and (mode_submit_only or mode_list_only)) \
        or (mode_list_only and (mode_submit_only or mode_download_only)):
        raise Exception("Error: --submit-only (-s), --list-only (-l) and --download-only (-w) cannot be used together.")

    # full mode is the default- submit, monitor, download
    mode_full = not (mode_submit_only or mode_list_only or mode_download_only)

    verbose = options.verbose

    # Check that we specify a csv file if need one
    if options.csvfile is None and (mode_submit_only or mode_full):
        raise Exception('Error: csvfile not specified')

    # Check the -d parameter is valid
    outdir = './'
    if options.outdir:
        outdir = options.outdir

        if not os.path.isdir(outdir):
            raise Exception("Error: Output directory {0} is invalid.".format(outdir))

    host = os.environ.get('ASVO_HOST', 'asvo.mwatelescope.org')
    if not host:
        raise Exception('ASVO_HOST env variable not defined')

    port = os.environ.get('ASVO_PORT', '8778')
    if not port:
        raise Exception('ASVO_PORT env variable not defined')

    user = os.environ.get('ASVO_USER', None)
    if not user:
        raise Exception('ASVO_USER env variable not defined')

    passwd = os.environ.get('ASVO_PASS', None)
    if not passwd:
        raise Exception('ASVO_PASS env variable not defined')

    ssl_verify = os.environ.get("SSL_VERIFY", "1")
    if ssl_verify == "1":
        sslopt = {'cert_reqs': ssl.CERT_REQUIRED}
    else:
        sslopt = {'cert_reqs': ssl.CERT_NONE}

    # Setup status thread. This will be used to update stdout with status info
    status_queue = Queue()
    status_thread = Thread(target=status_func, args=(status_queue,))
    status_thread.daemon = True
    status_thread.start()

    # Download queue keeps track of all in progress downloads
    download_queue = Queue()

    # Result queue keeps track of job completion
    result_queue = Queue()
    submit_lock = RLock()

    jobs_to_submit = []
    if mode_submit_only or mode_full:
        jobs_to_submit = parse_csv(options.csvfile)

        if len(jobs_to_submit) == 0:
            raise Exception("Error: No jobs to submit")

    params = (host,
              port,
              user,
              passwd)

    status_queue.put("Connecting to MWA ASVO...")
    session = Session.login(*params)
    status_queue.put("Connected to MWA ASVO")

    jobs_list = []

    # Take an action depending on command line options specified
    if mode_submit_only or mode_full:
        jobs_list = submit_jobs(session, jobs_to_submit, status_queue)

    elif mode_list_only:
        job_count = get_jobs_status(session, status_queue, verbose)
        if job_count == 0:
            print("You have no jobs.")

    elif mode_download_only:
        # JobID 0 is used to download ALL of the user's ready to download jobs
        if options.download_job_id == 0:
            jobs_list = enqueue_all_ready_to_download_jobs(session, download_queue, status_queue, verbose)

            if len(jobs_list) == 0:
                print("You have no jobs that are ready to download.")

                # exit gracefully
                status_queue.put(None)
                status_thread.join()
                return
        else:
            jobs_list = check_job_is_downloadable_and_enqueue(session, download_queue, options.download_job_id)

    if mode_submit_only or mode_list_only:
        # Exit- user opted to submit only or list only
        status_queue.put(None)
        status_thread.join()
        return

    if mode_full:
        # Initiate a notifier thread to get updates from the server
        status_queue.put("Connecting to MWA ASVO Notifier...")
        notify = Notify.login(*params, sslopt=sslopt)
        status_queue.put("Connected to MWA ASVO Notifier")

        notify_thread = Thread(target=notify_func, args=(notify,
                                                         submit_lock,
                                                         jobs_list,
                                                         download_queue,
                                                         result_queue,
                                                         status_queue,
                                                         verbose))

        notify_thread.daemon = True
        notify_thread.start()

    threads = []

    for i in range(4):
        # Launch a download thread
        t = Thread(target=download_func, args=(submit_lock,
                                               jobs_list,
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
            if len(jobs_list) == 0:
                break

        try:
            r = result_queue.get(timeout=1)
            if not r:
                raise Exception('Error: Control connection lost, exiting')
            results.append(r)
        except Empty:
            continue

    for _ in threads:
        download_queue.put(None)

    for t in threads:
        t.join()

    if mode_full:
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
        print('There were errors:')

    for r in results:
        print(r)

    if results_len > 0:
        sys.exit(4)


def main():
    init(autoreset=True)

    try:
        mwa_client()
    except ParseException as e:
        print('Error: %s, Line num: %s' %
              (str(e), e.line_num))
        sys.stdout.flush()
        sys.exit(3)
    except requests.exceptions.HTTPError as re:
        print(re.response.text)
        sys.stdout.flush()
        sys.exit(2)
    except KeyboardInterrupt:
        print("Interruped! Exiting...\n")
        sys.stdout.flush()
        sys.exit(1)
    except Exception as exp:
        print(exp)
        sys.stdout.flush()
        sys.exit(1)
    print("mwa_client finished successfully")

if __name__ == "__main__":
    main()
