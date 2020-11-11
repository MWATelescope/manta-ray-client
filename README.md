# Manta-ray Client (MWA ASVO Command Line Client)

## Description

Python API and helper script (mwa_client) to interact with the [MWA ASVO](https://asvo.mwatelescope.org).

For general help on using the MWA ASVO, please visit: [MWA ASVO wiki](https://wiki.mwatelescope.org/display/MP/Data+Access).

* Supported Python versions: 
  * Python 3.8
  * Python 3.7
  * Python 3.6
  * Python 2.7 works, however see note below:
> **_NOTE:_**  [Python2.x is now end of life](https://www.python.org/doc/sunset-python-2/), so we recommend making the switch to Python versions at or above Python 3.6 ASAP. At time of writing, manta-ray-client worked in Python2.7. Support for EOL versions of Python will be on a best effort basis where it is not a burden to do so, but will not go on indefinitely.

## mwa_client

mwa_client is a helper script which provides the following functions:
* Submit MWA ASVO jobs in bulk
* Monitor the status of your jobs
* Download your completed jobs

There are two types of MWA ASVO jobs: 
* Conversion: Average, convert and download a visibility data set ( and optionally apply calibration solutions).
* Download: Package and download a raw visibility data set. (This is recommended for advanced users, as the raw visibility files are in an MWA-specific format and require conversion and calibration).

## Installation Options

You must have an account on the [MWA ASVO website](https://asvo.mwatelescope.org)

Set your API key as an environment variables in linux (usually in your profile / .bashrc). You can get your API key from [Your Profile page](https://asvo.mwatelescope.org/settings) on the MWA ASVO website.
```
~$ export MWA_ASVO_API_KEY=<api key>
```

Then you may install natively on your computer OR install via Docker.

## Installation (Natively on your computer)

#### Clone the repository
```
~$ git clone https://github.com/ICRAR/manta-ray-client.git
```
#### Create a virtual environment
```
$ python3 -m venv env
```
or if you are *still* using python2.7 you will need to use virtualenv (See [Setting up Python, Pip, and Virtualenv (external link)](http://timsherratt.org/digital-heritage-handbook/docs/python-pip-virtualenv/) for information on installing virtualenv) 
```
~$ virtualenv -p /usr/bin/python2.7 env
```

#### Activate the virtual environment
```
~$ source env/bin/activate
(env)~$
```

#### Install mwa_client and all required packages
```
(env)~$ cd manta-ray-client
(env)~/manta-ray-client$ pip3 install -r requirements.txt
(env)~/manta-ray-client$ python3 setup.py install
```

## Installation (using Docker)
If you prefer, you can also run the manta-ray-client as a Docker container instead of installing it locally.
This assumes you have docker installed on your machine. If not please see the [Get Docker (external link)](https://docs.docker.com/get-docker/) page for instructions.

#### Clone the repository
```
~$ git clone https://github.com/ICRAR/manta-ray-client.git
```

#### Build the image
```
~$ cd manta-ray-client
~/manta-ray-client$ docker build --tag manta-ray-client:latest .
```

#### Use The Container
Once the image is built, you can run the mwa_client directly. The below command will:
* Create and launch and instances of the image (called a container), 
* Map '/your/host/data/path/' which should be a directory on your machine, to the container's /data directory
* Remove the container once it has finished the command
* Map your machine's MWA_ASVO_API_KEY environment variable into the container so it has your MWA ASVO API key
* Then 'mwa_client -w all -d /data' will run the mwa_client and download all 'Completed' jobs to the container's /data directory (which we mapped to '/your/host/data/path/' on your machine)
```
~$ docker run --name my_mwa_client --entrypoint="" --volume=/your/host/data/path/:/data --rm=true -e MWA_ASVO_API_KEY manta-ray-client:latest mwa_client -w all -d /data
```

Or you can open a shell within the container itself and then run as many mwa_client commands as you like, interactively, then exit to leave the container:
```
~$ docker run -it --name my_mwa_client --entrypoint="" --volume=/your/host/data/path/:/data --rm=true -e MWA_ASVO_API_KEY manta-ray-client:latest /bin/bash
root@c197566f86d9:/# mwa_client -l
...
root@c197566f86d9:/# exit
~$ 
```
You will get a prompt like the one above and from there you can run mwa_client commands as normal.

## Examples

```
mwa_client -c csvfile -d destdir           Submit jobs in the csv file, monitor them, then download the files, then exit
mwa_client -c csvfile -s                   Submit jobs in the csv file, then exit
mwa_client -d destdir -w JOBID             Download the job id (assuming it is ready to download), then exit
mwa_client -d destdir -w all               Download any ready to download jobs, then exit
mwa_client -d destdir -w all -e error_file Download any ready to download jobs, then exit, writing any errors to error_file
mwa_client -l                              List all of your jobs and their status, then exit
```

#### Help

```
optional arguments:
  -h, --help            show this help message and exit
  -s, --submit-only     submit job(s) from csv file then exit (-d is ignored)
  -l, --list-only       List the user's active job(s) and exit immediately
                        (-s, -c & -d are ignored)
  -w DOWNLOAD_JOB_ID, --download-only DOWNLOAD_JOB_ID
                        Download the job id (-w DOWNLOAD_JOB_ID), if it is ready; 
                        or all downloadable jobs (-w all | -w 0), then exit (-s, -c & -l are ignored)
  -c FILE, --csv FILE   csv job file
  -d DIR, --dir DIR     download directory
  -e ERRFILE, --error-file ERRFILE, --errfile ERRFILE
                        Write errors in json format to an error file
  -v, --verbose         verbose output

```

## Job States

Each job submitted will transition through the following states:

* Queued: Job has been submitted and is waiting to be processed. 
* Processing: Job is being processed.
* Ready for download: Job has completed- job product is ready for download.
* Downloading: Job product is being downloaded.
* Download Compete: Product download has been completed.
* Error: There was an error. 

## Submitting Jobs

Users can submit multiple jobs using a CSV file (see below for instructions).

## CSV Format

Each row is a single job and each CSV element must be a key=value pair. Whitespace (blank rows) and comments (lines beginning with #) are allowed. Please see the included [example.csv](example.csv) for several full working examples.

### Conversion Job Options

* obs_id: < integer >
* job_type: c
* timeres: < decimal >
  - Average N seconds of time steps together before writing output.
* freqres: < integer >
  - Average N kHz bandwidth of fine channels together before writing output.
* edgewidth: < integer >
  - Flag the given width (in kHz) of edge channels of each coarse channel.
* conversion:  < ms || uvfits >
  - ms: CASA measurement set. 
  - uvfits: uvfits output.

#### Optional options
To enable an option, set value to true e.g. norfi=true

Recommended defaults:
* allowmissing: Do not abort when not all GPU box files are available.
* flagdcchannels: Flag the centre channel of each sub-band.

Other options:
* calibrate: Apply a calibration solution to the dataset, if found. If not found, the job will fail- in this case you can resubmit the job without this option for uncalibrated raw visibilities. See: [Data Access/MWA ASVO Calibration Option ](https://wiki.mwatelescope.org/display/MP/MWA+ASVO+Calibration+Option) on the [MWA Telescope Wiki](https://wiki.mwatelescope.org/pages/viewpage.action?pageId=5963859) for more information.
* norfi: Disable RFI detection.
* nostats: Disable collecting statistics.
* nogeom: Disable geometric corrections.
* noantennapruning: Do not remove the flagged antennae.
* noflagautos: Do not flag auto-correlations.
* nosbgains: Do not correct for the digital gains.
* noflagmissings: Do not flag missing gpu box files (only makes sense with allowmissing).
* usepcentre: Centre on pointing centre.
* sbpassband: Apply unity passband (i.e. do not apply any corrections)

#### Example line in csv file

```
obs_id=1110103576, job_type=c, timeres=8, freqres=40, edgewidth=80, conversion=ms, calibrate=true, allowmissing=true, flagdcchannels=true
```

### Download Job Options

* obs_id: < integer >
* job_type: d
* download_type: < vis_meta || vis >
  - vis_meta: download visibility metadata only (metafits and RFI flags).
  - vis: download raw visibility data sets and metadata (raw visibility files, metafits and RFI flags).

#### Example lines in csv file

```
obs_id=1110103576, job_type=d, download_type=vis
obs_id=1110105120, job_type=d, download_type=vis_meta
```

### Understanding and using the error file output
You can get a machine readable error file in JSON format by specifying "-e" | "--error-file" | "--errfile" on the command line. This might be useful if you are trying to automate the download and processing of many observations and you don't want to try and parse the human readable standard output. 

An example of the format is below, with two jobs with errors:
```
[
    {
        "obs_id": "1216295963", 
        "job_id": 28979, 
        "result": "Error: an error message"
    },
    {
        "obs_id": "1216298341", 
        "job_id": 28980, 
        "result": "Error: some error message"
    }
]
```
Since this is JSON, in python you could simply use the below code to iterate through any errors by deserialising the JSON string:
```
import json

# Open the error file mwa_client produced when using -e
with open("error.txt", "r") as f:
    # Read the JSON from the file into a string
    json_string = f.read()

    # Deserialise the JSON into a python list of objects
    result_list = json.loads(json_string)

    # Iterate through all of the errors
    for r in result_list:        
        print("Job:{0} ObsId:{1} Result:{2}", r['job_id'], r['obs_id'], r['result'])
```
