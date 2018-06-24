# Manta-ray Client

## Description

Python API and helper script (mwa_client) to interact with the [MWA ASVO](https://asvo.mwatelescope.org).

## mwa_client

mwa_client is a helper script which provides the following functions:
* Submit MWA ASVO jobs in bulk
* Monitor the status of your jobs
* Download your completed jobs

There are two types of MWA ASVO jobs: 
* Conversion: Average, convert and download visibility data set.
* Download: Package and download a raw visibility data set. (This is recommended for advanced users, as the raw visibility files are in an MWA-specific format).

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

* obs_id=< integer >
* job_type=c
* timeres=< integer >
* freqres=< integer >
* edgewidth=< integer >
* conversion=< ms || uvfits >
  - ms: CASA measurement set. 
  - uvfits: uvfits set.

#### Optional options
To enable an option, set value to true e.g. norfi=true

Recommended defaults:
* allowmissing: Do not abort when not all GPU box files are available (default is to abort).
* flagdcchannels: Flag the centre channel of each sub-band.

Other options:
* norfi: Disable RFI detection.
* nostats: Disable collecting statistics.
* nogeom: Disable geometric corrections.
* noantennapruning: Do not remove the flagged antennae.
* noflagautos: Do not flag auto-correlations.
* nosbgains: Do not correct for the digital gains.
* noflagmissings: Do not flag missing gpu box files (only makes sense with allowmissing).
* usepcentre: Centre on pointing centre.

#### Example line in csv file

```
obs_id=1110103576, job_type=c, timeres=8, freqres=40, edgewidth=80, conversion=ms, allowmissing=true, flagdcchannels=true
```

### Download Job Options

* obs_id=< integer >
* job_type=d
* download_type=< vis_meta || vis >
  - vis_meta: download visibility metadata only (metafits and RFI flags).
  - vis: download raw visibility data sets and metadata (raw visibility files, metafits and RFI flags).

#### Example lines in csv file

```
obs_id=1110103576, job_type=d, download_type=vis
obs_id=1110105120, job_type=d, download_type=vis_meta
```

## Installation

You must have an account on the [MWA ASVO website](https://asvo.mwatelescope.org)

Set the credentials as environment variables in linux (usually in your profile)
```
export ASVO_USER=<username>
export ASVO_PASS=<password>
```

```
# Clone the repository
git clone https://github.com/ICRAR/manta-ray-client.git

# Create a virtual environment
virtualenv env
source env/bin/activate

# Install mwa_client and all required packages
cd manta-ray-client
python setup.py install
```

## Running

```
mwa_client -c csvfile -d destdir           Submit jobs in the csv file, monitor them, then download the files, then exit
mwa_client -c csvfile -s                   Submit jobs in the csv file, then exit
mwa_client -d destdir -w JOBID             Download the job id (assuming it is ready to download), then exit
mwa_client -d destdir -w 0                 Download any ready to download jobs, then exit
mwa_client -l                              List all of your jobs and their status, then exit
```

#### Help

```
Options:
  -h, --help            show this help message and exit
  -c FILE, --csv=FILE   csv job file
  -d DIR, --dir=DIR     download directory
  -s, --submit-only     submit job(s) from csv file then exit (-d is ignored)
  -l, --list-only       List the user's active job(s) and exit immediately (-s, -c & -d are ignored)
  -w DOWNLOAD_JOB_ID, --download-only=DOWNLOAD_JOB_ID
                        Download the job id (-w DOWNLOAD_JOB_ID), if it is ready; or all downloadable jobs (-w 0),
                        then exit (-s, -c & -l are ignored)
  -v, --verbose         verbose output
```

