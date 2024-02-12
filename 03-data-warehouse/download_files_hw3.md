`for i in {1..12}
do
    wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-$(printf "%02d" $i).parquet
done`
