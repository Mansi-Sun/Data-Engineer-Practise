set -e
service_type=$1
year=$2

url_prefix='https://github.com/DataTalksClub/nyc-tlc-data/releases/download'

for month in {1..12};do  
    fmonth=`printf "%02d" $month`
    url="${url_prefix}/${service_type}/${service_type}_tripdata_${year}-${fmonth}.csv.gz"

    local_prefix="data/raw/${service_type}/${year}/${fmonth}"
    local_file="${service_type}_tripdata_${year}-${fmonth}.csv.gz"
    local_path="${local_prefix}/${local_file}"
    echo "Downloading ${url} to ${local_path}"
    mkdir -p ${local_prefix}
    wget ${url} -O ${local_path}
done