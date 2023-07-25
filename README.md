# superfamily-pipeline

## Recipe

Create a directory to use for the project and `cd` into it and run `misc/create_project.pl`.

Alternatively, run `misc/create_project.pl` from anywhere with the `--project-dir PATH` argument (and optionally
`--output-version`, `--load-version`, and/or `--cytoscape-config` path).

Then edit `local_app_conf.sh` and `local_conf.sh` as appropriate.

## OLD Recipe

Create a directory `$DIR` that contains all of the intermediate and target data.

Copy `misc/run_collect.sh` to `$DIR`.

Copy `misc/local_conf.sh` to `$DIR` and set the version numbers, in MMDD format and data version X.X format.

    OUT_VER=0601
    LOAD_VER=4.0

Copy `misc/local_app_conf.sh.example` to `$DIR/local_app_conf.sh` and update paths to the apps as necessary.
An example for IGB/EFI is included below.

Create the directories for intermediate and output directory:

    mkdir $DIR/data_$OUT_VER
    mkdir $DIR/load-$LOAD_VER

Copy a master spreadsheet to `$DIR/master_$OUT_VER.txt`.

The `$DIR/run_collect.sh` script can be run.  It is fine to add this script to cron.  Suggested periodicity
is 10 minutes.

## Parameters for EFI/IGB for `local_app_conf.sh`

    export EFI_TOOLS_HOME="/home/n-z/noberg/dev/EFITools"
    export EFI_PERL_ENV="/home/groups/efi/apps/perl_env.sh"
    export EFI_PIPELINE_HOME="/home/n-z/noberg/dev/superfamily-pipeline"
    export EFI_INPUT_JOB_DIR="/private_stores/gerlt/jobs/dev/est"
    export QUEUE="efi"
    
    source "/etc/profile"
    module load Perl
    module load efishared/devlocal
    module load efiest/devlocal
    module load efignt/devlocal
    module load singularity
    source $EFI_PERL_ENV


