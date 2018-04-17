# Manta-ray Client

## Description

Python API and helper scripts to interact with the [MWA ASVO](https://asvo.mwatelescope.org).

## Job Services

There are two types of MWA ASVO jobs: 
* Conversion: Average, convert and download visibility data set using cotter.
* Visibility Download: Package and download a visibility data set. 

## Submit Jobs

Users can submit multiple jobs using a CSV file and `mwa_client.py`. 
Once all the jobs have been processed the data will be automatically downloaded.
The script will block until all the jobs have been downloaded or there is an error.

## Job States

Each job submitted will transition through the following states:

* Queued: Job has been submitted and is waiting to be processed. 
* Processing: Job is being processed. 
* Downloading: Job product is being downloaded.
* Download Compete: Product download has been completed.
* Error: There was an error. 

## CSV Format

Each row is a single job and each CSV element must be a key=value pair. 

### Conversion Job Options

* obs_id=< integer >
* job_type=c
* timeres=< integer >
* freqres=< integer >
* edgewidth=< integer >
* conversion=< ms || uvfits >
  - ms: CASA measurement set. 
  - UVFits: uvfits set.

#### Optional option
To enable an option, set value to true i.e. norfi=true

* norfi: Disable RFI detection.
* nostats: Disable collecting statistics.
* nogeom: Disable geometric corrections.
* noantennapruning: Do not remove the flagged antennae.
* noflagautos: Do not flag auto-correlations.
* nosbgains: Do not correct for the digital gains.
* noflagmissings: Do not flag missing gpu box files (only makes sense with allowmissing).
* allowmissing: Do not abort when not all GPU box files are available (default is to abort).
* flagdcchannels: Flag the centre channel of each sub-band.
* usepcentre: Centre on pointing centre.

#### Example

```
obs_id=1191828584, job_type=c, timeres=20, freqres=20, edgewidth=80, conversion=ms, allowmissing=true, flagdcchannels=true
```

### Visibility Download Job Options

* obs_id=< integer >
* job_type=d
* download_type=< vis_meta || vis >
  - vis_meta: download visibility meta-data only. 
  - vis: download visibility data sets and meta-data. 

#### Example

```
obs_id=1191828584, job_type=d, download_type=vis
obs_id=1191828528, job_type=d, download_type=vis_meta
```

## Installation

You must have an account on the MWA ASVO portal.

Set the credentials as environment variables in linux
```
export ASVO_USER=<username>
export ASVO_PASS=<password
```

##### Code

```
virtualenv env
source env/bin/activate

git clone https://github.com/ICRAR/manta-ray-client.git
cd manta-ray-client
python setup.py install
```

## Running

```
mwa_client --csv=jobs.csv --dir=/tmp/
```

##### Help

```
Usage: mwa_client.py [options]

Options:
  -h, --help           show this help message and exit
  -c FILE, --csv=FILE  csv job file
  -d DIR, --dir=DIR    download directory
```

