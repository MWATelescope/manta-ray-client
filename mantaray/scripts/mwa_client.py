import os
import ssl
import csv
import sys
import requests
import json
from urllib.parse import urlparse

try:
    from queue import Queue, Empty
except:
    from Queue import Queue, Empty

from threading import Thread, RLock
import argparse
from colorama import init, Fore, Style
from mantaray.api import Notify, Session, get_pretty_version_string


# Constants for job states
JOB_STATE_QUEUED = 0
JOB_STATE_PROCESSING = 1
JOB_STATE_READY_FOR_DOWNLOAD = 2
JOB_STATE_ERROR = 3
JOB_STATE_EXPIRED = 4
JOB_STATE_CANCELLED = 5

# Constants descriptions for job types
JOB_TYPE_VALUES = {
    0: "conversion",
    1: "download visibilities",
    2: "download metadata",
    3: "download_voltage",  # not implemented
    4: "cancel job"
}

class Result(object):

    def __init__(self, result_job_id, result_obs_id, result_colour_message, result_no_colour_message):
        self._job_id = result_job_id
        self._obs_id = result_obs_id
        self._colour_message = str(result_colour_message)
        self._no_colour_message = "".join(str(result_no_colour_message))  # Remove any newlines

    @property
    def job_id(self):
        return self._job_id

    @property
    def obs_id(self):
        return self._obs_id

    @property
    def colour_message(self):
        return self._colour_message

    @property
    def no_colour_message(self):
        return self._no_colour_message


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
                elif val == 'v':
                    job_type = 'submit_voltage_job_direct'
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
        except requests.exceptions.HTTPError as re:
            response_dict = json.loads(re.response.text)
            error_code = response_dict.get('error_code')
            error_text = response_dict.get('error')

            if error_code == 0:
                status_queue.put("{0}Skipping:{1} {2}.".format(Fore.MAGENTA, Fore.RESET, error_text))
            if error_code == 2:
                status_queue.put("{0}Skipping:{1} {2} already queued, processing or complete.".format(Fore.MAGENTA, Fore.RESET, job[1]['obs_id']))
        except Exception:
            print("Error submitting job #{0} from csvfile. Details below:".format(job_number))
            raise
        else:
            new_job_id = job_response['job_id']
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


def uri_validator(product):
    try:
        result = urlparse(product)
        return all([result.scheme, result.netloc])
    except:
        return False


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
        obs_id = item['row']['job_params']['obs_id']
        products = item['row']['product']['files']

        for prod in products:
            try:
                product_name = prod[0]
                file_size = prod[1]
                server_sha1 = prod[2]

                if not uri_validator(product_name):
                    # Filename is not a downloadable URL. File must be on /astro
                    msg = '%sJob on astro:%s Job id: %s%s%s file: %s%s%s' % \
                            (Fore.GREEN, Fore.RESET, Fore.LIGHTWHITE_EX+Style.BRIGHT, job_id,
                            Fore.RESET, Fore.LIGHTWHITE_EX+Style.BRIGHT, product_name, Fore.RESET)
                    status_queue.put(msg)
                    continue

                url = urlparse(product_name)
                filename = os.path.basename(url.path)
                file_path = os.path.join(output_dir, filename)

                msg = '%sDownload complete:%s Job id: %s%s%s file: %s%s%s server-sha1: %s%s%s' % \
                      (Fore.GREEN, Fore.RESET, Fore.LIGHTWHITE_EX+Style.BRIGHT, job_id,
                       Fore.RESET, Fore.LIGHTWHITE_EX+Style.BRIGHT, file_path,
                       Fore.RESET, Fore.LIGHTWHITE_EX+Style.BRIGHT, server_sha1, Fore.RESET)

                if os.path.isfile(file_path):
                    if os.path.getsize(file_path) == file_size:
                        status_queue.put(msg)
                        continue

                status_queue.put('%sDownloading:%s Job id: %s%s%s file: %s%s%s size: %s%s%s bytes'
                                 % (Fore.MAGENTA, Fore.RESET, Fore.LIGHTWHITE_EX+Style.BRIGHT, job_id,
                                    Fore.RESET, Fore.LIGHTWHITE_EX+Style.BRIGHT, product_name,
                                    Fore.RESET, Fore.LIGHTWHITE_EX + Style.BRIGHT, file_size, Fore.RESET))
                session.download_file_product(job_id, product_name, file_path)
                status_queue.put(msg)

            except Exception as e:
                result_queue.put(Result(job_id, obs_id, e, e))
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
        obs_id = item['row']['job_params']['obs_id']
        job_state = item['row']['job_state']

        msg = get_status_message(item, verbose, True)
        no_color_msg = get_status_message(item, verbose, False)  # get uncolorised output for error file

        with submit_lock:
            if action == 'DELETE':
                status_queue.put(msg)

                _remove_submitted(submit_lock,
                                  submitted_jobs,
                                  job_id)
                continue

            if job_id in submitted_jobs:
                if job_state == JOB_STATE_QUEUED:
                    status_queue.put(msg)

                elif job_state == JOB_STATE_PROCESSING:
                    status_queue.put(msg)

                elif job_state == JOB_STATE_READY_FOR_DOWNLOAD:
                    status_queue.put(msg)

                    download_queue.put(item)

                elif job_state == JOB_STATE_ERROR:
                    result_queue.put(Result(job_id, obs_id, msg, no_color_msg))

                    _remove_submitted(submit_lock,
                                      submitted_jobs,
                                      job_id)

                elif job_state == JOB_STATE_EXPIRED:
                    result_queue.put(Result(job_id, obs_id, msg, no_color_msg))

                    _remove_submitted(submit_lock,
                                      submitted_jobs,
                                      job_id)

                elif job_state == JOB_STATE_CANCELLED:
                    # do not consider cancelled as an error
                    status_queue.put(msg)

                    _remove_submitted(submit_lock,
                                      submitted_jobs,
                                      job_id)


def get_job_summary(job_id, obs_id, job_type_desc, use_colour):
    if use_colour:
        return '%sJob id: %s%s %sObs id: %s%s%s type: %s%s%s' % (Fore.RESET, Fore.LIGHTWHITE_EX + Style.BRIGHT,
                                                                 job_id,
                                                                 Fore.RESET, Fore.LIGHTWHITE_EX + Style.BRIGHT,
                                                                 obs_id,
                                                                 Fore.RESET, Fore.LIGHTWHITE_EX + Style.BRIGHT,
                                                                 job_type_desc,
                                                                 Fore.RESET)
    else:
        return 'Job id: %s Obs id: %s type: %s' % (job_id, obs_id, job_type_desc)


def get_status_message(item, verbose, use_colour):
    # Format the status message
    action = item['action']
    job_id = int(item['row']['id'])
    job_state = item['row']['job_state']
    job_type = item['row']['job_type']
    job_params = item['row']['job_params']
    error_text = item['row']['error_text']

    job_type_desc = JOB_TYPE_VALUES.get(job_type)
    obs_id = job_params["obs_id"]

    msg = get_job_summary(job_id, obs_id, job_type_desc, use_colour)

    if verbose:
        if use_colour:
            msg = msg + '%s typeid: %s%s%s params: %s%s%s' % (Fore.RESET, Fore.LIGHTWHITE_EX + Style.BRIGHT,
                                                              job_type,
                                                              Fore.RESET, Fore.LIGHTWHITE_EX + Style.BRIGHT,
                                                              job_params,
                                                              Fore.RESET)
        else:
            msg = msg + ' typeid: %s params: %s' % (job_type, job_params)

    if action == 'DELETE':
        if use_colour:
            msg = "%s%s: %s" % (Fore.RED, 'Deleted', msg)
        else:
            msg = "%s: %s" % ('Deleted', msg)
    else:
        if job_state == JOB_STATE_QUEUED:
            if use_colour:
                msg = "%s%s: %s" % (Fore.MAGENTA, 'Queued', msg)
            else:
                msg = "%s: %s" % ('Queued', msg)

        elif job_state == JOB_STATE_PROCESSING:
            if use_colour:
                msg = "%s%s: %s" % (Fore.BLUE, 'Processing', msg)
            else:
                msg = "%s: %s" % ('Processing', msg)

        elif job_state == JOB_STATE_READY_FOR_DOWNLOAD:
            # Get the products and file sizes
            products = item['row']['product']['files']
            total_size = 0

            # loop through any products and get their size in bytes
            for prod in products:
                file_size = int(prod[1])
                total_size = total_size + file_size

            if products[0][2] == '': #No hash, must be astro job
                if use_colour:
                    msg = "%s%s: %s %spath: %s%s, size: %s bytes" % (Fore.GREEN, 'Ready on /astro', msg, Fore.RESET,
                                                        Fore.LIGHTWHITE_EX + Style.BRIGHT, products[0][0], products[0][1])
                else:
                    msg = "%s: path: %s, size: %s bytes" % ('Ready on /astro', products[0][0], products[0][1])
            else:
                if use_colour:
                    msg = "%s%s: %s %ssize: %s%s bytes" % (Fore.MAGENTA, 'Ready for Download', msg, Fore.RESET,
                                                        Fore.LIGHTWHITE_EX + Style.BRIGHT, total_size)
                else:
                    msg = "%s: size: %s bytes" % ('Ready for Download', total_size)

        elif job_state == JOB_STATE_ERROR:
            if use_colour:
                msg = "%s%s: %s %s" % (Fore.RED, 'Error', error_text, msg)
            else:
                msg = "%s: %s" % ('Error', error_text)

        elif job_state == JOB_STATE_EXPIRED:
            msg = "%s: %s" % ('Expired', msg)

        elif job_state == JOB_STATE_CANCELLED:
            if use_colour:
                msg = "%s%s: %s" % (Fore.RED, 'Cancelled', msg)
            else:
                msg = "%s: %s" % ('Cancelled', msg)

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
            msg = get_status_message(j, verbose, True)
            status_queue.put(msg)

    return len(jobs)


def enqueue_all_ready_to_download_jobs(session, download_queue, status_queue, verbose):
    submitted_jobs = []
    jobs = get_job_list(session)

    for j in jobs:
        job_state = j['row']['job_state']

        # Check is ready for download
        if job_state == JOB_STATE_READY_FOR_DOWNLOAD:
            job_id = j['row']['id']
            submitted_jobs.append(job_id)
            msg = get_status_message(j, verbose, True)
            status_queue.put(msg)
            download_queue.put(j)

    return submitted_jobs


def check_job_is_downloadable_and_enqueue(session, download_queue, result_queue, job_id):
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
        obs_id = found_job['row']['job_params']['obs_id']
        job_type_desc = JOB_TYPE_VALUES.get(found_job['row']['job_type'])

        if job_state != JOB_STATE_READY_FOR_DOWNLOAD:
            colour_msg = "{0}Error: Invalid job state- job not ready for download{1}; {2}".format(
                Fore.RED, Fore.RESET, get_job_summary(job_id, obs_id, job_type_desc, True))
            no_colour_msg = "Error: Invalid job state- job not ready for download"
            result_queue.put(Result(job_id, obs_id, colour_msg, no_colour_msg))

            return []
        else:
            # Put this in the download queue
            submitted_jobs.append(job_id)
            download_queue.put(found_job)
            return submitted_jobs

    else:
        # not a valid job
        colour_msg = "{0}Error: Job Id {1} is not a valid job, has expired or is not owned by you.{2}".format(
            Fore.RED, job_id, Fore.RESET)
        no_colour_msg = "Error: Job Id {0} is not a valid job, has expired or is not owned by you.".format(job_id)
        result_queue.put(Result(job_id, "N/A", colour_msg, no_colour_msg))

        return []


class ParseDownloadOnly(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        # Acceptable values are:
        # 0                == all jobs
        # Positive integer == specific job
        # all|ALL,etc      == all jobs, same as 0
        msg = "'{0}' is not valid for -w / --download-only. Try a Job Id, or 'all' for all jobs.".format(values)

        try:
            if int(values) >= 0:
                setattr(namespace, self.dest, int(values))
            else:
                parser.error(msg)
        except ValueError:
            # Not an int, so treat as string
            if str(values).lower() == "all":
                setattr(namespace, self.dest, 0)
            else:
                parser.error(msg)


def mwa_client():
    version_string = get_pretty_version_string()
    print(version_string)

    epi = "\nExamples: "\
          "\nmwa_client -c csvfile -d destdir           " \
          "Submit jobs in the csv file, monitor them, then download the files, then exit" \
          "\nmwa_client -c csvfile -s                   " \
          "Submit jobs in the csv file, then exit" \
          "\nmwa_client -d destdir -w JOBID             " \
          "Download the job id (assuming it is ready to download), then exit" \
          "\nmwa_client -d destdir -w all               " \
          "Download any ready to download jobs, then exit" \
          "\nmwa_client -d destdir -w all -e error_file " \
          "Download any ready to download jobs, then exit, writing any errors to error_file" \
          "\nmwa_client -l                              " \
          "List all of your jobs and their status, then exit" \

    desc = "{0}\n==============================\n\n" \
           "The mwa_client is a command-line tool for submitting, monitoring and \n" \
           "downloading jobs from the MWA ASVO (https://asvo.mwatelescope.org). \n" \
           "Please see README.md for csv file format and other details.".format(version_string)

    parser = argparse.ArgumentParser(description=desc, epilog=epi, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group()

    group.add_argument("-s", "--submit-only", action="store_true", dest="submit_only",
                       help="submit job(s) from csv file then exit (-d is ignored)", default=False)

    group.add_argument("-l", "--list-only", action="store_true", dest="list_only",
                       help="List the user's active job(s) and exit immediately (-s, -c & -d are ignored)",
                       default=False)

    group.add_argument("-w", "--download-only", action=ParseDownloadOnly, dest="download_job_id",
                       help="Download the job id (-w DOWNLOAD_JOB_ID), if it is ready; or all downloadable jobs "
                            "(-w all | -w 0), then exit (-s, -c & -l are ignored)")

    parser.add_argument("-c", "--csv", dest="csvfile",
                        help="csv job file", metavar="FILE")

    parser.add_argument("-d", "--dir", dest="outdir",
                        help="download directory", metavar="DIR")

    parser.add_argument("-e", "--error-file", "--errfile", dest="errfile",
                        help="Write errors in json format to an error file", default=None)

    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose",
                        help="verbose output", default=False)

    args = parser.parse_args()

    # Figure out what mode we are running in, based on the command line args
    mode_submit_only = (args.submit_only is True)
    mode_list_only = (args.list_only is True)
    mode_download_only = not (args.download_job_id is None)

    # full mode is the default- submit, monitor, download
    mode_full = not (mode_submit_only or mode_list_only or mode_download_only)

    verbose = args.verbose

    # Check that we specify a csv file if need one
    if args.csvfile is None and (mode_submit_only or mode_full):
        raise Exception('Error: csvfile not specified')

    # Check the -d parameter is valid
    outdir = './'
    if args.outdir:
        outdir = args.outdir

        if not os.path.isdir(outdir):
            raise Exception("Error: Output directory {0} is invalid.".format(outdir))

    host = os.environ.get('MWA_ASVO_HOST', 'asvo.mwatelescope.org')
    if not host:
        raise Exception('[ERROR] MWA_ASVO_HOST env variable not defined')

    port = os.environ.get('MWA_ASVO_PORT', '443')
    if not port:
        raise Exception('[ERROR] MWA_ASVO_PORT env variable not defined')

    https = os.environ.get('MWA_ASVO_HTTPS', '1')
    if not https:
        raise Exception('[ERROR] MWA_ASVO_SSL env variable not defined')

    user = os.environ.get('ASVO_USER', None)
    if user:
        print("[WARNING] ASVO_USER environment variable is no longer used by the mwa_client- "
              "you should remove it from your .profile/.bashrc/startup scripts.")

    passwd = os.environ.get('ASVO_PASS', None)
    if passwd:
        print("[WARNING] ASVO_PASS environment variable is no longer used by the mwa_client- "
              "you should remove it from your .profile/.bashrc/startup scripts.")

    api_key = os.environ.get('MWA_ASVO_API_KEY', None)
    if not api_key:
        raise Exception('[ERROR] MWA_ASVO_API_KEY env variable not defined. Log in to the MWA ASVO web site- '
                        'https://asvo.mwatelescope.org/settings to obtain your API KEY, then place the following '
                        'into your .profile/.bashrc/startup scripts (where xxx is your API key):\n'
                        '   export MWA_ASVO_API_KEY=xxx\n')

    ssl_verify = os.environ.get("SSL_VERIFY", "0")
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
        jobs_to_submit = parse_csv(args.csvfile)

        if len(jobs_to_submit) == 0:
            raise Exception("Error: No jobs to submit")

    params = (https,
              host,
              port,
              api_key)

    status_queue.put("Connecting to MWA ASVO ({0}:{1})...".format(host, port))
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
        if args.download_job_id == 0:
            jobs_list = enqueue_all_ready_to_download_jobs(session, download_queue, status_queue, verbose)

            if len(jobs_list) == 0:
                print("You have no jobs that are ready to download.")

                # exit gracefully
                status_queue.put(None)
                status_thread.join()
                return
        else:
            jobs_list = check_job_is_downloadable_and_enqueue(session, download_queue,
                                                              result_queue, args.download_job_id)

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

    # If we specified an error file, write to that too
    if args.errfile:
        # open the error file for overwrite, even if we have no errors, so we clear the file
        error_file = open(args.errfile, "w")

    if len(results) > 0:
        print('There were errors:')

        json_list = []

        for r in results:
            # Output errors to the screen
            print(r.colour_message)

            # Put results into a JSON object
            json_list.append({'job_id': r.job_id, 'obs_id': r.obs_id, 'result': r.no_colour_message})

        # If we specified an error file, write to that too
        if args.errfile:
            # open the error file for overwrite
            error_file = open(args.errfile, "w")

            json_output = json.dumps(json_list, indent=4)
            error_file.write(json_output)

            error_file.close()

        sys.exit(4)
    else:
        if args.errfile:
            error_file.close()

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
