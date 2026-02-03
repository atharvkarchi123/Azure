#!/bin/bash

# Defining the target configuration file
TARGET_FILE="/databricks/spark/conf/spark-env.sh"

#Defining the env variables 
echo "export raw_container='abfss://raw@atharvkarchiadls.dfs.core.windows.net'" >> $TARGET_FILE

echo "export processed_container='abfss://processed@atharvkarchiadls.dfs.core.windows.net'" >> $TARGET_FILE

echo "export presentation_container='abfss://presentation@atharvkarchiadls.dfs.core.windows.net'" >> $TARGET_FILE

echo "export demo_container='abfss://demo@atharvkarchiadls.dfs.core.windows.net'" >> $TARGET_FILE