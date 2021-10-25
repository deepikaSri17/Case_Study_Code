
Code is available in 2 forms : dbc file and python file. Please use any one as suitable. SAME CODE IS PRESENT IN BOTH FILES.

Recommended: dbc file can be imported directly to Databricks and run from there after connecting to cluster.

1. PLEASE CHANGE THE PATH OF INPUT FILE (ol_cdump.json) IN THE CASE STUDY CODE BEFORE EXECUTION:
 
   Go to Command 3 : df_jsondataraw = spark.read.schema(dataloadschema).json("/mnt/datalkds1791/processed/ol_cdump.json")

  Currently it is pointing to DataLake path "/mnt/datalkds1791/processed/ol_cdump.json".
 Please change it to the path where file is located.



In case of Failures on execution: Please refer below : 

. Magic commands are used to install pandas-profiling and ipywidgets

  If magic commands dont work and pandas-profiling and ipywidgets already installed please comment or ignore the below commands.

%sh
pip install pandas-profiling
pip install ipywidgets

prof = pandas_profiling.ProfileReport(df_jsondataraw_pandas.sample(n=10000))