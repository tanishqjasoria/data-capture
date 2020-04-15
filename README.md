# data-capture

-> binance_data.py - responsible for fetching data from the Binance server using RESTful API<br/>
-> check_data_quality.py - is a periodic test run to check the quality of data stored<br/>
->crypto_store.py - get 1M ticker data using binance_data and calculate 5M, 15M, 30M 1H ticker. Store the values in the database. Also, runs periodic quality test using check_data_quality<br/>
<br/>

InfluxDB is used to store the data. CRYPTO_1M, CRYPTO_5M, CRYPTO_15M, CRYPTO_30M, CRYPTO_1H measurements are used to store corresponding ticker data.

<br/>
To run this on you machine
<br/>
git clone https://github.com/tanishqjasoria/data-capture.git<br/>
cd data_capture<br/>
./setup.sh<br/>
python3 crypto_store.py -M run
