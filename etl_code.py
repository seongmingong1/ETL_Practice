import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

## Task 1: Extraction 

# extract csv files
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

# extract Json files
def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines = True)
    #lines =True to enable the function to read the file as a JSON
    return dataframe

# extract XML files
def extract_from_xml(file_to_process): 
    dataframe = pd.DataFrame(columns=["name", "height", "weight"]) 
    tree = ET.parse(file_to_process) 
    root = tree.getroot() 
    for person in root: 
        name = person.find("name").text 
        height = float(person.find("height").text) 
        weight = float(person.find("weight").text) 
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True) 
    return dataframe 

# extract all files 

def extract():
    extracted_data = pd.DataFrame(columns=['name', 'height','weight'])
    #creat an empty data frame to hold extract data

    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extracted_data

# Task 2: Transformation 

## height should be changed from inches to cm
## weight should be changed from pounds to kg

def transform(data):
    #1 inch = 0.0254 meters
    data['height'] = round(data.height * 0.0254,2)
    #1 pind = 0.45339237 kg
    data['weight'] = round(data.weight * 0.45339237,2 )

    return data

# Task 3: Loading and Logging 

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S" # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() #get current timestamp 
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ','+message + '\n')

# TEST ETL Op, Log progress

log_progress("ETL Job Started")

### Extract ###
log_progress("Extract phase Started") 
extracted_data = extract() 

log_progress("Extrect phase Ended")

### Transform ###
log_progress("Transform phase started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

log_progress("Transform phase Ended")

### Load ###
log_progress("Load phase Started")
load_data(target_file, transformed_data)

log_progress("Load finished")

### End ###
log_progress("ETL Job Ended")
