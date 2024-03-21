#!/bin/bash

module purge
bin_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

site_dir=$1
data_dir=$2
temp_dir=$3

if [[ "$site_dir" == "" ]]; then
    echo "Invalid site directory (parameter #1) provided"
    exit 1
fi
if [[ ! -d $data_dir ]]; then
    echo "Invalid data dir (parameter #2) provided"
    exit 1
fi
if [[ ! -d $temp_dir ]]; then
    echo "Invalid temp dir (parameter #3) provded"
    exit 1
fi


mkdir -p $site_dir
cd $site_dir

git clone -b devel https://github.com/EnzymeFunctionInitiative/superfamily.git .

echo "-----------------------------> INSTALLING DEPDENCIES USING COMPOSER <-------------------------------"
/bin/bash bin/setup-composer.sh
php composer.phar update

echo "-------------------------------> EDITING SITE CONFIGURATION FILE <----------------------------------"
project_name=$( basename -- "$( cd "$data_dir/.." &> /dev/null && pwd )" )
site_data_dir=$( cd "$site_dir/.." &> /dev/null && pwd )
site_data_dir="$site_data_dir/data"
mkdir -p $site_data_dir
ln -s $data_dir "$site_data_dir/$project_name-1.0"
cat "1.0" > $site_data_dir/versions.txt

echo "-------------------------------> EDITING SITE CONFIGURATION FILE <----------------------------------"
cp conf/settings.inc.php.dist conf/settings.inc.php
perl $bin_dir/edit_site_config.pl conf/settings.inc.php $site_dir $site_data_dir $project_name $temp_dir

echo "---------------------------> SETTING UP PHP FILES FOR INTERACTIVITY <-------------------------------"
#TODO: copy/set up .php files


