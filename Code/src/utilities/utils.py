import yaml

#Function to Read CSV Data and Return Dataframe
def load_data(spark, input_file_path):

    return spark.read.option("inferSchema", "true").csv(input_file_path, header=True)

# Read Config File and Return a Dictionary with Configuration Details
def read_yaml(input_file_path):
    
    with open(input_file_path, "r") as f:
        return yaml.safe_load(f)

#Write dataframe to parquet format
def write_output(df, output_file_path, output_file_format):
    
    df.write.format(output_file_format).mode('overwrite').option("header", "true").save(output_file_path)