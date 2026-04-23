# Manta-ray Client (MWA ASVO Command Line Client)

## Description

Python API and helper script (mwa_client) to interact with the [MWA ASVO](https://asvo.mwatelescope.org).

Giant Squid is the preferred CLI client for the MWA ASVO- check it out here [Giant Squid](https://github.com/MWATelescope/giant-squid)

For general help on using the MWA ASVO, please visit: [MWA ASVO wiki](https://mwatelescope.atlassian.net/wiki/spaces/MP/pages/24973129/Data+Access).

---

NOTE FOR HPC USERS

Please read [this wiki article](https://mwatelescope.atlassian.net/wiki/spaces/MP/pages/65405030/MWA+ASVO+Use+with+HPC+Systems) if you are running manta-ray-client on HPC systems.

---

- Supported Python versions:
  - Python 3.10+

## mwa_client

mwa_client is a helper script which provides the following functions:

- Submit MWA ASVO jobs in bulk
- Monitor the status of your jobs
- Download your completed jobs

There are five types of MWA ASVO jobs:

- Conversion: Average, convert and download a visibility data set (and optionally apply calibration solutions).
- Download: Package and download a raw visibility data set. (This is recommended for advanced users, as the raw visibility files are in an MWA-specific format and require conversion and calibration).
- Beamformer: Download beamformer data products from MWAX_BEAMFORMER or MWAX_CORR_BF observations.
- Voltage: Raw voltage data from VCS observations. This option is restricted to members of the mwavcs team who have a Pawsey account. If you are interested in getting access to VCS data, please [contact us](maito:asvo_support@mwatelescope.org)
- Imaging: Generate calibrated sky images from visibility data using wsclean. Can process raw visibility data (with preprocessing) or pre-converted measurement sets (coming soon).

## Installation Options

You must have an account on the [MWA ASVO website](https://asvo.mwatelescope.org)

Set your API key as an environment variables in linux (usually in your profile / .bashrc). You can get your API key from [Your Profile page](https://asvo.mwatelescope.org/settings) on the MWA ASVO website.

```bash
~$ export MWA_ASVO_API_KEY=<api key>
```

Then you may install natively on your computer OR install via Docker.

### Installation Natively on your computer

#### Clone the repository

```bash
~$ git clone https://github.com/ICRAR/manta-ray-client.git
```

#### Create a virtual environment

Pre-requisites:
- Python3.10+
- uv installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

```bash
# Sync the project and create a virtual environment
uv sync
```

#### Install mwa_client and all required packages

```bash
(env)~$ cd manta-ray-client

# Install the local package in 'editable' mode
# This registers the metadata so the client can find its own version
(env)~/manta-ray-client$ uv pip install -e .

# 3. Verify the installation
(env)~/manta-ray-client$ uv run mwa_client --help
```

### Installation using Docker

If you prefer, you can also run the manta-ray-client as a Docker container instead of installing it locally.
This assumes you have docker installed on your machine. If not please see the [Get Docker (external link)](https://docs.docker.com/get-docker/) page for instructions.

#### Clone the repository

```bash
~$ git clone https://github.com/mwatelescope/manta-ray-client.git
```

#### Build the image

```bash
~$ cd manta-ray-client
~/manta-ray-client$ docker build --tag manta-ray-client:latest .
```

#### Use The Container

Once the image is built, you can run the mwa_client directly. The below command will:

- Create and launch and instances of the image (called a container),
- Map '/your/host/data/path/' which should be a directory on your machine, to the container's /data directory
- Remove the container once it has finished the command
- Map your machine's MWA_ASVO_API_KEY environment variable into the container so it has your MWA ASVO API key
- Then 'mwa_client -w all -d /data' will run the mwa_client and download all 'Completed' jobs to the container's /data directory (which we mapped to '/your/host/data/path/' on your machine)

```bash
~$ docker run --name my_mwa_client --entrypoint="" --volume=/your/host/data/path/:/data --rm=true -e MWA_ASVO_API_KEY manta-ray-client:latest mwa_client -w all -d /data
```

Or you can open a shell within the container itself and then run as many mwa_client commands as you like, interactively, then exit to leave the container:

```bash
~$ docker run -it --name my_mwa_client --entrypoint="" --volume=/your/host/data/path/:/data --rm=true -e MWA_ASVO_API_KEY manta-ray-client:latest /bin/bash
root@c197566f86d9:/# mwa_client -l
...
root@c197566f86d9:/# exit
~$
```

You will get a prompt like the one above and from there you can run mwa_client commands as normal.

## Examples

```bash
mwa_client -c csvfile -d destdir           Submit jobs in the csv file, monitor them, then download the files, then exit
mwa_client -c csvfile -s                   Submit jobs in the csv file, then exit
mwa_client -d destdir -w JOBID             Download the job id (assuming it is ready to download), then exit
mwa_client -d destdir -w all               Download any ready to download jobs, then exit
mwa_client -d destdir -w all -e error_file Download any ready to download jobs, then exit, writing any errors to error_file
mwa_client -l                              List all of your jobs and their status, then exit
```

## Help

```bash
optional arguments:
  -h, --help            Show this help message and exit
  -s, --submit-only     Submit job(s) from csv file then exit (-d is ignored)
  -l, --list-only       List the user's active job(s) and exit immediately
                        (-s, -c & -d are ignored)
  -w DOWNLOAD_JOB_ID, --download-only DOWNLOAD_JOB_ID
                        Download the job id (-w DOWNLOAD_JOB_ID), if it is ready;
                        or all downloadable jobs (-w all | -w 0), then exit (-s, -c & -l are ignored)
  -c FILE, --csv FILE   csv job file
  -d DIR, --dir DIR     Download directory
  -e ERRFILE, --error-file ERRFILE, --errfile ERRFILE
                        Write errors in json format to an error file
  -v, --verbose         Verbose output
  -ar, --allow-resubmit Will allow a job with the same parameters and an existing job in your queue in Completed, Error or Cancelled status to be resubmitted. Default is to not allow resubmission if the new job matches the parameters of an existing job in your queue.

```

## Job States

Each job submitted will transition through the following states:

- Queued: Job has been submitted and is waiting to be processed.
- Waiting for calibration: New Calibration solution has been requested.
- Staging: Files are being staged.
- Staged: Files are staged from the Archive.
- Retrieving Files from Archive: Files are being downloaded to HPC from the archive.
- Preprocessing: Job is being processed by Birli.
- Imaging: image product is being created.
- Delivering: Files are being delivered to the destined location.
- Ready for download: Job has completed- job product is ready for download.
- Downloading: Job product is being downloaded.
- Download Compete: Product download has been completed.
- Error: There was an error.

## Submitting Jobs

Users can submit multiple jobs using a CSV file (see below for instructions).

## CSV Format

Each row is a single job and each CSV element must be a key=value pair. Whitespace (blank rows) and comments (lines beginning with #) are allowed. Please see the included [example.csv](example.csv) for several full working examples.

## Conversion Job Options

Please note that some options are only available depending on the choice of preprocessor (explained below).

- `obs_id: <integer>`
  - Observation ID
- `job_type: c`
  - Always 'c' for conversion jobs.
- `avg_time_res: <decimal>`
  - Time resolution: average N seconds of time steps together before writing output.
- `avg_freq_res: <integer>`
  - Average N kHz bandwidth of fine channels together before writing output.
- `flag_edge_width: <integer>`
  - Defaults to 80 kHz.
  - Flag the given width (in kHz) of edge channels of each coarse channel.
  - Set to 0 kHz to disable edge flagging.
- `output:  <ms || uvfits>`
  - Output format.
  - `ms`: CASA measurement set.
  - `uvfits`: uvfits output.
- `delivery:  <acacia || scratch || dug>`
  - Where you would like your data to be stored
  - `acacia (default)`: Data will be delivered to Pawsey's Acacia system and you will receive a link to download a zip file containing the data.
  - `scratch`: Data will be left on the /scratch file system at Pawsey in /scratch/\<pawsey_group>/asvo/<job_id>.
  - `dug`: Data will be transferred to the dug super computing fecility under /data/<dug_group>/asvo/<job_id>.
  - astro and scratch options are only available for Pawsey users who are in one of the mwa science groups (mwasci, mwavcs, mwaeor, mwaops). Please contact support if you would like to use this option.

### Flags / Optional Options

- In addition to the options specified above, a number of flags (or optional options) can also be passed with the job request.
- To enable an option, set value to true e.g. `no_rfi=true`
- If you omit an option it is equivalent to false. e.g. not specifying no_rfi is equivalent to `no_rfi=false`.

Birli currently supports the options below. For more info on the Birli preprocessor, please visit [the repository](https://github.com/mwatelescope/birli). Any other flags passed will be ignored.

- `no_rfi=true` Do not perform RFI detection.
- `no_geometric_delay=true` Disable geometric corrections.
- `no_cable_delay=true` Disable cable length corrections.
- `no_digital_gains=true` Do not correct for the digital gains.
- `no_passband_gains=true` Apply unity passband (i.e. do not apply any passband corrections).
- `no_flag_dc=true` Do not flag the centre/DC channel of each coarse channel.

#### Calibration

- `apply_di_cal=true` Apply a rough calibration solution to the dataset, if found. If not found, the job will fail- in this case you can resubmit the job without this option for uncalibrated raw visibilities. See: [Data Access/MWA ASVO Calibration Option](https://mwatelescope.atlassian.net/wiki/spaces/MP/pages/24972245/MWA+ASVO+Calibration+Option) on the [MWA Telescope Wiki](https://mwatelescope.atlassian.net/wiki/spaces/MP/overview?mode=global) for more information.

#### Pointing options

If the `centre` options is omitted, the job will default to using the observations phase centre.

- `centre=phase || pointing || custom`
  - `phase` Centre on the observations phase centre
  - `pointing` Centre on the observations pointing centre
  - `custom` Centre on a custom phase centre. If this option is specified, two additional parameters must be passed:
    - `phase_centre_ra: <ra formatted as: 0.0 deg>` ICRS (J2000.0). Centre on a custom phase centre with this decimal right ascension (must include phase_centre_dec).
    - `phase_centre_dec: <dec formatted as: +00.0 deg>` ICRS (J2000.0). Centre on a custom phase centre with this decimal declination (must include phase_centre_ra).
    - e.g. `centre=custom,phase_centre_ra=123.23,phase_centre_dec=-20.1`

#### Example line in csv file

```csv
obs_id=1110103576, job_type=c, avg_time_res=8, avg_freq_res=40, flag_edge_width=80, output=ms, apply_di_cal=true, no_rfi=true
```

### Download Job Options

- `obs_id: <integer>`
  - Observation ID
- `job_type: d`
  - Always 'd' for download jobs.
- `download_type: <vis_meta || vis>`
  - `vis_meta`: download visibility metadata only (metafits and RFI flags).
  - `vis`: download raw visibility data sets and metadata (raw visibility files, metafits and RFI flags).
- `delivery: <acacia || scratch || dug>`
  - `acacia`: Data will be delivered to Pawsey's Acacia system and you will receive a link to download a zip file containing the data.
  - `scratch`: Data will be left on the /scratch file system at Pawsey in /scratch/\<group>/asvo/<job_id>.
  - `dug`: Data will be transferred to the dug super computing fecility under /data/<dug_group>/asvo/<job_id>.
  - astro and scratch options are only available for Pawsey users who are in one of the mwa science groups (mwasci, mwavcs, mwaeor, mwaops). Please contact support if you would like to use this option.

#### Example lines in csv file

```csv
obs_id=1110103576, job_type=d, download_type=vis, delivery=acacia
obs_id=1110105120, job_type=d, download_type=vis_meta, delivery=scratch
obs_id=1110105120, job_type=d, download_type=vis_meta, delivery=dug
```

### Beamformer Job Options

- `obs_id: <integer>`
  - Observation ID
- `job_type: b`
  - Always 'b' for beamformer jobs.
- `delivery: <acacia || scratch || dug>`
  - `acacia` (default): Data will be delivered to Pawsey's Acacia system and you will receive a link to download a zip file containing the data.
  - `scratch`: Data will be left on the /scratch file system at Pawsey in /scratch/\<pawsey_group\>/asvo/\<job_id\>.
  - `dug`: Data will be transferred to the DUG super computing facility under /data/\<dug_group\>/asvo/\<job_id\>.
  - scratch and dug options are only available for Pawsey users who are in one of the mwa science groups (mwasci, mwavcs, mwaeor, mwaops). Please contact support if you would like to use this option.
- `delivery_format: <files || tar>` [optional]
  - `files` (default): Download individual beamformer files.
  - `tar`: Package all files into a tar archive.
  - Only applicable for scratch and dug delivery types.

#### Example lines in csv file

```csv
obs_id=1234567890, job_type=b, delivery=acacia
obs_id=1234567891, job_type=b, delivery=scratch, delivery_format=tar
obs_id=1234567892, job_type=b, delivery=dug, delivery_format=files
```

### Voltage Job Options

Note that voltage jobs will always be left on /astro or /scratch, and you will therefore need a Pawsey account to submit them. Please get in contact if you're interested in accessing VCS data.

- `obs_id: <integer>`
  - Observation ID
- `job_type: v`
  - Always 'v' for voltage jobs.
- `offset: <integer>`
  - Number of seconds from the beginning of the observation for which you would like data
- `duration: <integer>`
  - Number of seconds of voltage data to be included in the job.

#### Example lines in csv file

```csv
obs_id=1323776840, job_type=v, offset=0, duration=1200
```


### Imaging Job Options

Imaging jobs generate calibrated sky images from MWA visibility data using the wsclean imager. The workflow includes:
1. **Staging**: Raw visibility files are retrieved from the archive
2. **Preprocessing** (if `input_mode=raw`): Calibration and averaging using Birli
3. **Imaging**: wsclean generates sky images with CLEAN deconvolution
4. **Delivery**: FITS images (and optionally all intermediate products) are delivered

#### Required Parameters

- `obs_id: <integer>`
  - Observation ID
- `job_type: i`
  - Always 'i' for imaging jobs.
- `delivery: <acacia || scratch || dug>`
  - `acacia` (default): Data will be delivered to Pawsey's Acacia system and you will receive a link to download the image files.
  - `scratch`: Data will be left on the /scratch file system at Pawsey in /scratch/\<pawsey_group\>/asvo/\<job_id\>.
  - `dug`: Data will be transferred to the DUG super computing facility under /data/\<dug_group\>/asvo/\<job_id\>.
  - scratch and dug options are only available for Pawsey users who are in one of the MWA science groups (mwasci, mwavcs, mwaeor, mwaops). Please contact support if you would like to use this option.

#### Image Parameters

- `image_size: <512 || 1024 || 2048 || 3072 || 4096 || 8192>` [optional, default: 3072]
  - Image dimensions in pixels (square images). Powers of 2 optimize FFT performance.
  - Typical: 3072 pixels (covers ~51° × 51° at 20" pixels, ~60 arcmin at default MWA resolution)
- `pixel_scale: <10.0 - 120.0>` [optional, default: 20.0]
  - Pixel size in arcseconds. MWA resolution at 150 MHz is ~2 arcminutes.
  - Smaller values = higher resolution but slower imaging and larger memory requirements.
  - Typical range: 15-30 arcseconds.
- `weighting: <natural || uniform || briggs>` [optional, default: briggs]
  - Visibility weighting scheme:
    - `natural`: Maximum sensitivity, lower resolution
    - `uniform`: Maximum resolution, lower sensitivity
    - `briggs`: Compromise controlled by robust parameter (recommended)
- `robust: <-2.0 to 2.0>` [optional, default: -0.5]
  - Briggs robust parameter (only used if `weighting=briggs`).
  - -2 ≈ uniform (high resolution), +2 ≈ natural (high sensitivity)
  - Typical: -0.5 to 0.0

#### CLEAN Deconvolution Parameters

- `clean_iterations: <0 - 1000000>` [optional, default: 100000]
  - Maximum number of CLEAN iterations.
  - 0 = dirty image only (no deconvolution)
  - Typical: 10,000 - 500,000 depending on field complexity
- `clean_threshold: <0.0 - 10.0>` [optional, default: 0.001]
  - Flux threshold in Jy below which CLEAN stops.
  - Typical: 0.0001 - 0.01 Jy for faint source imaging
- `auto_threshold: <0.1 - 5.0>` [optional, default: 0.5]
  - Auto-threshold factor in units of RMS noise.
  - CLEAN stops when residuals reach `auto_threshold × RMS`.
  - Typical: 0.5 (stop at 0.5× noise level)
- `nwlayers: <32 - 512>` [optional, default: 128]
  - Number of w-layers for w-stacking (wide-field imaging correction).
  - More layers = better accuracy but slower imaging.
  - Typical: 128. Increase for very wide fields or high frequencies.
- `multiscale: <true || false>` [optional, default: true]
  - Enable multiscale CLEAN for extended source structure.
  - Recommended: true for most science cases.

#### Input/Output Options

- `input_mode: <raw || processed>` [optional, default: raw]
  - `raw`: Process raw visibility files (gpubox files) through Birli preprocessing, then image.
  - `processed`: Image pre-converted measurement sets (requires prior conversion job).
- `output_mode: <fits || all_fits || all_files>` [optional, default: fits]
  - `fits`: Deliver only the final restored FITS image.
  - `all_fits`: Deliver all FITS products (restored, residual, model, PSF).
  - `all_files`: Deliver all files including measurement set and wsclean working files.
- `delivery_format: <files || tar>` [optional, default: tar]
  - `files`: Individual files
  - `tar`: Packaged tar archive (recommended for imaging jobs)

#### Preprocessing Parameters (for input_mode=raw)

When `input_mode=raw`, these conversion parameters apply before imaging:

- `avg_time_res: <decimal>` [optional]
  - Average N seconds of time steps together before imaging.
  - Must be ≥ observation time resolution and an integer multiple of it.
- `avg_freq_res: <integer>` [optional]
  - Average N kHz of fine channels together before imaging.
  - Must be ≥ observation frequency resolution and an integer multiple of it.
  - Maximum: 1280 kHz.
- `flag_edge_width: <integer>` [optional, default: 80]
  - Flag N kHz at edges of each coarse channel.
  - Set to 0 to disable edge flagging.
  - Must be integer multiple of observation frequency resolution.
- `apply_di_cal: <true || false>` [optional, default: true]
  - Apply direction-independent calibration solution if available.
  - Recommended: true for imaging jobs.
  - See: [MWA ASVO Calibration Option](https://mwatelescope.atlassian.net/wiki/spaces/MP/pages/24972245/MWA+ASVO+Calibration+Option)

#### Preprocessing Flags (for input_mode=raw)

Birli preprocessing flags (set to `true` to enable, omit or set to `false` to disable):

- `no_rfi=true` Do not perform RFI detection.
- `no_geometric_delay=true` Disable geometric corrections.
- `no_cable_delay=true` Disable cable length corrections.
- `no_digital_gains=true` Do not correct for digital gains.
- `no_passband_gains=true` Apply unity passband (no passband corrections).
- `no_flag_dc=true` Do not flag the DC channel of each coarse channel.

#### Phase Center Options

- `phase_center: <phase || custom>` [optional, default: phase]
  - `phase`: Use observation's phase center (recommended)
  - `custom`: Specify custom RA/Dec (requires `custom_ra` and `custom_dec`)
- `custom_ra: <0.0 - 359.999999>` [required if phase_center=custom]
  - Right ascension in decimal degrees (ICRS J2000.0)
- `custom_dec: <-90.0 - 90.0>` [required if phase_center=custom]
  - Declination in decimal degrees (ICRS J2000.0)

#### Advanced Options

- `apply_primary_beam: <true || false>` [optional, default: true]
  - Apply MWA primary beam correction.
  - Recommended: true for accurate flux measurements.
- `input: <ms || uvfits>` [optional, default: ms]
  - Input visibility format for preprocessing.
  - `ms`: CASA measurement set (recommended)
  - `uvfits`: UVFITS format

#### Example lines in csv file

**Basic imaging (raw data with defaults):**
```csv
obs_id=1234567890, job_type=i, delivery=acacia
```

**High-resolution imaging with custom parameters:**
```csv
obs_id=1234567890, job_type=i, image_size=4096, pixel_scale=15, weighting=uniform, clean_iterations=500000, clean_threshold=0.0005, nwlayers=256, delivery=scratch
```

**Imaging with preprocessing and calibration:**
```csv
obs_id=1234567890, job_type=i, input_mode=raw, avg_time_res=8, avg_freq_res=80, apply_di_cal=true, image_size=3072, pixel_scale=20, weighting=briggs, robust=-0.5, delivery=acacia, output_mode=all_fits
```

**Custom phase center:**
```csv
obs_id=1234567890, job_type=i, phase_center=custom, custom_ra=123.45, custom_dec=-20.5, image_size=2048, pixel_scale=25, delivery=acacia
```

**Imaging pre-converted data:** *(coming soon)*
```csv
obs_id=1234567890, job_type=i, input_mode=processed, image_size=3072, pixel_scale=20, weighting=briggs, robust=0.0, multiscale=true, delivery=dug, delivery_format=tar
```

#### Resource Estimates

Imaging jobs automatically estimate resource requirements:
- **Memory**: Scales with image size² and data volume. 3072px images typically require 100-200 GB.
- **Walltime**: Varies from 2-12 hours depending on data size, image size, and CLEAN iterations.
- Jobs exceeding 350 GB memory or 24 hours walltime will be rejected with an error message.

#### Observation Requirements

Imaging jobs require correlator mode observations. The following modes are **not** supported:
- VOLTAGE_START, VOLTAGE_BUFFER, MWAX_VCS (voltage capture modes)
- MWAX_BEAMFORMER, MWAX_CORR_BF (beamformer modes)

For these observations, use the appropriate job type (voltage or beamformer).

### Understanding and using the error file output

You can get a machine readable error file in JSON format by specifying "-e" | "--error-file" | "--errfile" on the command line. This might be useful if you are trying to automate the download and processing of many observations and you don't want to try and parse the human readable standard output.

An example of the format is below, with two jobs with errors:

```json
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

```python
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

### Common issues & solutions

During the migration to `uv`, we identified a few legacy hurdles. If you encounter errors, check these solutions:

1. `ModuleNotFoundError: No module named 'pkg_resources'`

The MWA client relies on an older part of `setuptools` called `pkg_resources`. Modern Python environments (especially 3.12+) do not include this by default.

- **Fix**: Ensure `setuptools` is in your `dependencies` list. Note that setuptools versions **70.0.0 and above** removed certain legacy features. We recommend pinning:

```bash
uv add "setuptools<70.0"
```

2. `The 'manta-ray-client' distribution was not found`

This happens if the code is present but hasn't been "installed" as a package. The client uses `pkg_resources` to look up its own metadata at runtime.

Fix: Run `uv pip install -e .` This creates the necessary `.egg-info` or `.dist-info` folders that allow the code to identify itself.

3. `mwa_client: command not found`
uv installs scripts into a local .venv/bin folder rather than your global path.

Fix: Always prefix your commands with `uv run`:

```bash
uv run mwa_client -j <JOB_ID>
```
Alternatively, run source `.venv/bin/activate` to use the command directly.
