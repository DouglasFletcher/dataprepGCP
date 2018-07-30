# dataprepGCP
Running the file dataprocessing_run.sh (CD: bash dataprocessing_run.sh) will execute the process:

1. create a Dataset in BigQuery (DATAPROCESS) 
2. Data extraction is executed in BigQuery from the example data sets, and saved to a temporary BigQuery datatable (in dataset: DATAPROCESS). 
3. Data is transferred to Google Cloud storage in tmp/ directories
4. Dataset is deleted (decrease storage costs)