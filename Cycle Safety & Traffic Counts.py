
# coding: utf-8

# # Cycle Safety Project

# ## Imports

# In[12]:

import numpy as np
import pandas as pd
import pyproj
import warnings
import zipfile
from scipy.spatial import cKDTree
from pyproj import Proj, transform


# ## Options

# In[13]:

from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))       # Changing the cell widths

warnings.filterwarnings('ignore', category=DeprecationWarning, message='.*use @default decorator instead.*')
                                                                            # Getting rid of annoying warnings

pd.options.display.max_rows = 30                                            # Setting the max number of rows
pd.options.display.max_columns = 40                                         # Setting the max number of columns


# ## Data

# This notebook uses the following data sources:
# 
# AADF Data: https://data.gov.uk/dataset/gb-road-traffic-counts/datapackage.zip<br>
# AADF Metadata: http://data.dft.gov.uk/gb-traffic-matrix/aadf-majorroads-metadata.pdf<br>
# Casualty Data: https://data.gov.uk/dataset/road-accidents-safety-data/datapackage.zip<br>
# Casualty Variable Decodes: http://data.dft.gov.uk/road-accidents-safety-data/Road-Accident-Safety-Data-Guide.xls<br>
# 
# You will need to download the two data files above and save the zip files into the path directory specified below

# ## Variables

# In[14]:

year = 2015

path = 'data/'
out = 'out/'
counts_file = 'gb-road-traffic-counts.zip'
casualties_file = 'road-accidents-safety-data.zip'


# ## Traffic Count Data

# ### Importing the data

# In[15]:

files = set(['AADF-data-major-roads','AADF-data-minor-roads'])               # List of relevent files to import

tc_dict = {}                                                                 # Blank Dictionary to store the casualties dataframes

for file in files:
    # Zip Files
    tc_files = zipfile.ZipFile(path + counts_file, mode='r')                 # Level 1 location
    tc_ext = tc_files.extract('data/{}.zip'.format(file))                    # Level 1 extraction
    ind_file = zipfile.ZipFile(tc_ext, mode='r')                             # Level 2 location
    ind_ext = ind_file.extract('{}.csv'.format(file))                        # Level 2 extraction
    
    # Dataframe
    df = pd.read_csv(ind_ext,low_memory=False)                               # Creating the dataframe
    df = df[(df['AADFYear'] == year)]                                        # Limiting to the specified year
    tc_dict['df_' + file.split('-')[2].lower()] = df                         # Appending the dataframe into the df_dict


# In[16]:

df_tc_raw = pd.concat([tc_dict['df_major'],tc_dict['df_minor']])
df_tc_raw = df_tc_raw.reset_index().drop(['index'],axis=1)


# ### Converting E / N to Lat / Lon

# In[17]:

# Some Basic Cleaning

df_tc = df_tc_raw.drop(['ONS GOR Name', 'ONS LA Name','AADFYear','A-Junction','B-Junction','LenNet','LenNet_miles','RCat'],axis=1)    # Keeps only the latest year & drops unwanted variables

# Creating a master Goods Vehicle Variable

df_tc['FdAll_GV'] = sum([df_tc['FdHGV'], df_tc['FdHGVA3'],df_tc['FdHGVA5'], df_tc['FdHGVA6'], df_tc['FdHGVR2'], df_tc['FdHGVR3'], df_tc['FdHGVR4'], df_tc['FdLGV']])
df_tc = df_tc.drop(['FdHGV', 'FdHGVA3','FdHGVA5', 'FdHGVA6', 'FdHGVR2', 'FdHGVR3', 'FdHGVR4', 'FdLGV'],axis=1)

# Setting the Projections

bng = Proj("+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy +datum=OSGB36 +units=m +no_defs towgs84='446.448,-125.157,542.060,0.1502,0.2470,0.8421,-20.4894'")
wgs84 = Proj('+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs')

# Converting columns to lists to pass to pyproj:

Easting = df_tc['S Ref E'].tolist()                                     # Convert Easting to List
Northing = df_tc['S Ref N'].tolist()                                    # Convert Northing to List
Lon_S,Lat_S = pyproj.transform(bng,wgs84,Easting,Northing)              # Performing the conversion:
LL = pd.DataFrame(Lat_S,Lon_S)                                          # Creating a DataFram
LL.reset_index(inplace='True')                                          # Reset the Index
LL.rename(columns={'index':'Lon_S',0:'Lat_S'}, inplace='True')          # Rename the columns
rep = [-7.557159842082696,49.76680723189604]                            # Lat / Lon co-ords for where the data is missing
LL = LL.replace(rep,np.nan)                                             # Replacing missing lat / lon values with nan

df_tc = df_tc.merge(LL,left_index='True',right_index='True',how='outer')             .drop(['S Ref E','S Ref N'],axis=1)                        # Merging on to the master dataset & Dropping the Eastings / Northings column


# ### Creating Dataframes to merge with Casualties Data

# In[18]:

df_tc['1st_Road_Class'] = df_tc['Road'].str[0:1]                                                                 # Creating the 1st Road Class variable
road_list = df_tc['Road'].unique().tolist()                                                                      # List of road names
road_list.remove('C')                                                                                            # Removing C Roads
road_list.remove('U')                                                                                            # Removing U Roads
type_list = ['A','B','M','C','U']                                                                                # List of road types
 
name_counts = {'{}'.format(road_name): df_tc[(df_tc['Road'] == road_name)] for road_name in road_list}           # Dictionary containing traffic count dataframes for each road name
type_counts = {'{}'.format(road_type): df_tc[(df_tc['1st_Road_Class'] == road_type)] for road_type in type_list} # Dictionary containing traffic count dataframes for each road type

for road_name in road_list:
    name_counts[road_name] = name_counts[road_name].reset_index().drop(['index'],axis=1)

for road_type in type_list:
    type_counts[road_type] = type_counts[road_type].reset_index().drop(['index'],axis=1)

# Creating Sets out of the road and type lists to improve performance
    
road_set = set(road_list)
type_set = set(type_list)


# ## Casualties Data

# ### Importing the Data

# In[19]:

files = set(['DfTRoadSafety_Vehicles_' + str(year),
             'DfTRoadSafety_Accidents_' + str(year),
             'DfTRoadSafety_Casualties_' + str(year)])                        # List of relevent files to import

cas_dict = {}                                                                 # Blank Dictionary to store the casualties dataframes

for file in files:
    # Zip Files
    cas_files = zipfile.ZipFile(path + casualties_file, mode='r')             # Level 1 location
    cas_ext = cas_files.extract('data/{}.zip'.format(file))                   # Level 1 extraction
    ind_file = zipfile.ZipFile(cas_ext, mode='r')                             # Level 2 location
    ind_ext = ind_file.extract('{}.csv'.format(file))                         # Level 2 extraction

    # Dataframe
    df = pd.read_csv(ind_ext,low_memory=False)                                # Creating the dataframe
    df.rename(columns={'﻿Accident_Index':'Accident_Index'},inplace=True)      # Renaming the Accident Index variable due to a wierd character
    cas_dict['df_' + file.split('_')[1].lower()] = df                         # Appending the dataframe into the df_dict


# ### Dataframe reference variables

# In[20]:

df_v = cas_dict['df_vehicles']  # Vehicles data not required (yet)

df_c = cas_dict['df_casualties']
df_c_cols = ['Accident_Index','Casualty_Class','Sex_of_Casualty','Age_of_Casualty','Casualty_Severity','Casualty_Type']

df_a = cas_dict['df_accidents']
df_a_cols = ['Accident_Index','Police_Force','Longitude','Latitude','Junction_Detail','Junction_Control','Number_of_Vehicles','Number_of_Casualties','Date','Day_of_Week','Time','1st_Road_Class','1st_Road_Number',
             'Road_Type','Speed_limit','Light_Conditions','Weather_Conditions','Road_Surface_Conditions','Urban_or_Rural_Area']


# ### Merging the Accidents & Casualties Dataframes together

# In[22]:

df_a = df_a[df_a_cols].drop_duplicates()          
df_c = df_c[df_c_cols].drop_duplicates()
cas_dict['df_cas'] = df_c.merge(df_a,left_on='Accident_Index',right_on='Accident_Index',how='inner').drop_duplicates()
df_cas = cas_dict['df_cas']

df_cas = df_cas.reset_index().drop(['index'],axis=1)


# ### Defining functions to decode variable values

# In[23]:

def casualty_class(row):
    if row["Casualty_Class"] == 1:
        return "Driver or rider"
    elif row["Casualty_Class"] == 2:
        return "Passenger"
    elif row["Casualty_Class"] == 3:
        return "Pedestrian"
    else:
        return "Unknown"
    
def sex_of_casualty(row):
    if row["Sex_of_Casualty"] == 1:
        return "Male"
    elif row["Sex_of_Casualty"] == 2:
        return "Female"
    else:
        return "Unknown"  
    
def casualty_severity(row):
    if row["Casualty_Severity"] == 1:
        return "Fatal"
    elif row["Casualty_Severity"] == 2:
        return "Serious"
    elif row["Casualty_Severity"] == 3:
        return "Slight"
    else:
        return "Unknown"
    
def casualty_type(row):
    if row["Casualty_Type"] == 0:
        return "Pedestrian"   
    elif row["Casualty_Type"] == 1:
        return "Cyclist"  
    elif row["Casualty_Type"] == 2:
        return "Motorcycle 50cc and under rider or passenger" 
    elif row["Casualty_Type"] == 3:
        return "Motorcycle 125cc and under rider or passenger"     
    elif row["Casualty_Type"] == 4:
        return "Motorcycle over 125cc and up to 500cc rider or  passenger" 
    elif row["Casualty_Type"] == 5:
        return "Motorcycle over 500cc rider or passenger"     
    elif row["Casualty_Type"] == 8:
        return "Taxi/Private hire car occupant"  
    elif row["Casualty_Type"] == 9:
        return "Car occupant"  
    elif row["Casualty_Type"] == 10:
        return "Minibus (8 - 16 passenger seats) occupant"  
    elif row["Casualty_Type"] == 11:
        return "Bus or coach occupant (17 or more pass seats)"      
    elif row["Casualty_Type"] == 16:
        return "Horse rider"   
    elif row["Casualty_Type"] == 17:
        return "Agricultural vehicle occupant"   
    elif row["Casualty_Type"] == 18:
        return "Tram occupant"   
    elif row["Casualty_Type"] == 19:
        return "Van / Goods vehicle (3.5 tonnes mgw or under) occupant"   
    elif row["Casualty_Type"] == 20:
        return "Goods vehicle (over 3.5t. and under 7.5t.) occupant"   
    elif row["Casualty_Type"] == 21:
        return "Goods vehicle (7.5 tonnes mgw and over) occupant"  
    elif row["Casualty_Type"] == 22:
        return "Mobility scooter rider"  
    elif row["Casualty_Type"] == 23:
        return "Electric motorcycle rider or passenger"  
    elif row["Casualty_Type"] == 90:
        return "Other vehicle occupant"  
    elif row["Casualty_Type"] == 97:
        return "Motorcycle - unknown cc rider or passenger"  
    elif row["Casualty_Type"] == 98:
        return "Goods vehicle (unknown weight) occupant"     
    
def police_force(row):
    if row["Police_Force"] == 1:
        return "Metropolitan Police"
    elif row["Police_Force"] == 3:
        return "Cumbria"
    elif row["Police_Force"] == 4:
        return "Lancashire"
    elif row["Police_Force"] == 5:
        return "Merseyside"
    elif row["Police_Force"] == 6:
        return "Greater Manchester"
    elif row["Police_Force"] == 7:
        return "Cheshire"
    elif row["Police_Force"] == 10:
        return "Northumbria"
    elif row["Police_Force"] == 11:
        return "Durham"
    elif row["Police_Force"] == 12:
        return "North Yorkshire"
    elif row["Police_Force"] == 13:
        return "West Yorkshire"
    elif row["Police_Force"] == 14:
        return "South Yorkshire"
    elif row["Police_Force"] == 16:
        return "Humberside"
    elif row["Police_Force"] == 17:
        return "Cleveland"
    elif row["Police_Force"] == 20:
        return "West Midlands"
    elif row["Police_Force"] == 21:
        return "Staffordshire"
    elif row["Police_Force"] == 22:
        return "West Mercia"
    elif row["Police_Force"] == 23:
        return "Warwickshire"
    elif row["Police_Force"] == 30:
        return "Derbyshire"
    elif row["Police_Force"] == 31:
        return "Nottinghamshire"
    elif row["Police_Force"] == 32:
        return "Lincolnshire"
    elif row["Police_Force"] == 33:
        return "Leicestershire"
    elif row["Police_Force"] == 34:
        return "Northamptonshire"
    elif row["Police_Force"] == 35:
        return "Cambridgeshire"
    elif row["Police_Force"] == 36:
        return "Norfolk"
    elif row["Police_Force"] == 37:
        return "Suffolk"
    elif row["Police_Force"] == 40:
        return "Bedfordshire"
    elif row["Police_Force"] == 41:
        return "Hertfordshire"
    elif row["Police_Force"] == 42:
        return "Essex"
    elif row["Police_Force"] == 43:
        return "Thames Valley"
    elif row["Police_Force"] == 44:
        return "Hampshire"
    elif row["Police_Force"] == 45:
        return "Surrey"
    elif row["Police_Force"] == 46:
        return "Kent"
    elif row["Police_Force"] == 47:
        return "Sussex"
    elif row["Police_Force"] == 48:
        return "City of London"
    elif row["Police_Force"] == 50:
        return "Devon and Cornwall"
    elif row["Police_Force"] == 52:
        return "Avon and Somerset"
    elif row["Police_Force"] == 53:
        return "Gloucestershire"
    elif row["Police_Force"] == 54:
        return "Wiltshire"
    elif row["Police_Force"] == 55:
        return "Dorset"
    elif row["Police_Force"] == 60:
        return "North Wales"
    elif row["Police_Force"] == 61:
        return "Gwent"
    elif row["Police_Force"] == 62:
        return "South Wales"
    elif row["Police_Force"] == 63:
        return "Dyfed-Powys"
    elif row["Police_Force"] == 91:
        return "Northern"
    elif row["Police_Force"] == 92:
        return "Grampian"
    elif row["Police_Force"] == 93:
        return "Tayside"
    elif row["Police_Force"] == 94:
        return "Fife"
    elif row["Police_Force"] == 95:
        return "Lothian and Borders"
    elif row["Police_Force"] == 96:
        return "Central"
    elif row["Police_Force"] == 97:
        return "Strathclyde"
    elif row["Police_Force"] == 98:
        return "Dumfries and Galloway"
    else:
        return "Unknown"

def junction_detail(row):
    if row["Junction_Detail"] == 0:
        return "Not at junction or within 20 metres"
    elif row["Junction_Detail"] == 1:
        return "Roundabout"
    elif row["Junction_Detail"] == 2:
        return "Mini-roundabout"   
    elif row["Junction_Detail"] == 3:
        return "T or staggered junction"   
    elif row["Junction_Detail"] == 5:
        return "Slip road"   
    elif row["Junction_Detail"] == 6:
        return "Crossroads"   
    elif row["Junction_Detail"] == 7:
        return "More than 4 arms (not roundabout)"   
    elif row["Junction_Detail"] == 8:
        return "Private drive or entrance"   
    elif row["Junction_Detail"] == 9:
        return "Other junction"   
    else:
        return "Unknown"   

def junction_control(row):
    if row["Junction_Control"] == 0:
        return "Not at junction or within 20 metres"
    elif row["Junction_Control"] == 1:
        return "Authorised person"    
    elif row["Junction_Control"] == 2:
        return "Auto traffic signal"
    elif row["Junction_Control"] == 3:
        return "Stop sign"
    elif row["Junction_Control"] == 4:
        return "Give way or uncontrolled"   
    else:
        return "Unknown"
    
def is_junction(row):
    if row['Junction_Detail'] in ['T or staggered junction','Crossroads','Roundabout','Mini-roundabout','Slip road','Junction - more than 4 arms (not roundabout)','Other junction']:
        return "Junction"
    else:
        return "Not a Junction"
    
def day_of_week(row):
    if row["Day_of_Week"] == 1:
        return "Sunday"
    elif row["Day_of_Week"] == 2:
        return "Monday"
    elif row["Day_of_Week"] == 3:
        return "Tuesday"    
    elif row["Day_of_Week"] == 4:
        return "Wednesday"    
    elif row["Day_of_Week"] == 5:
        return "Thursday"  
    elif row["Day_of_Week"] == 6:
        return "Friday"    
    elif row["Day_of_Week"] == 7:
        return "Saturday"
    else:
        return "Unknown"
    
def day_type(row):
    if row['Day_of_Week'] in ['Saturday','Sunday']:
        return 'Weekend'
    elif row['Day_of_Week'] in ['Monday','Tuesday','Wednesday','Thursday','Friday']:
        return 'Weekday'
    else:
        return 'Unknown'
      
def first_road_class(row):
    if row["1st_Road_Class"] == 1:
        return "Motorway"
    if row["1st_Road_Class"] == 2:
        return "A"    
    if row["1st_Road_Class"] == 3:
        return "A"   
    if row["1st_Road_Class"] == 4:
        return "B"      
    if row["1st_Road_Class"] == 5:
        return "C"      
    else:
        return "U"

def road_name(row):
    if row["1st_Road_Class"] in ['A','B','M']:
        return row['1st_Road_Class'] + str(row['1st_Road_Number'])
    else:
        return row['1st_Road_Class']
    
def road_type(row):
    if row["Road_Type"] == 1:
        return "Roundabout"
    elif row["Road_Type"] == 2:
        return "One way street"
    elif row["Road_Type"] == 3:
        return "Dual carriageway"
    elif row["Road_Type"] == 6:
        return "Single carriageway"    
    elif row["Road_Type"] == 7:
        return "Slip road"    
    elif row["Road_Type"] == 9:
        return "Unknown"
    elif row["Road_Type"] == 12:
        return "One way street/Slip road"
    else: 
        return "Unknown"
    
def light_conditions(row):
    if row["Light_Conditions"] == 1:
        return "Daylight"
    elif row["Light_Conditions"] == 4:
        return "Darkness - lights lit" 
    elif row["Light_Conditions"] == 5:
        return "Darkness - lights unlit"     
    elif row["Light_Conditions"] == 6:
        return "no lighting"       
    elif row["Light_Conditions"] == 7:
        return "lighting unknown"     
    else:
        return "Unknown"

def weather_conditions(row):
    if row["Weather_Conditions"] == 1:
        return "Fine no high winds"
    elif row["Weather_Conditions"] == 2:
        return "Raining no high winds"
    elif row["Weather_Conditions"] == 3:
        return "Snowing no high winds"    
    elif row["Weather_Conditions"] == 4:
        return "Fine + high winds"    
    elif row["Weather_Conditions"] == 5:
        return "Raining + high winds"
    elif row["Weather_Conditions"] == 6:
        return "Snowing + high winds"    
    elif row["Weather_Conditions"] == 7:
        return "Fog or mist"    
    elif row["Weather_Conditions"] == 8:
        return "Other"
    else:
        return "Unknown"
    
def road_surface_conditions(row):
    if row["Road_Surface_Conditions"] == 1: 
        return "Dry"
    elif row["Road_Surface_Conditions"] == 2: 
        return "Wet or damp"
    elif row["Road_Surface_Conditions"] == 3: 
        return "Snow"
    elif row["Road_Surface_Conditions"] == 4: 
        return "Frost or ice" 
    elif row["Road_Surface_Conditions"] == 5: 
        return "Flood over 3cm. deep"
    elif row["Road_Surface_Conditions"] == 6: 
        return "Oil or diesel"
    elif row["Road_Surface_Conditions"] == 7: 
        return "Mud"
    else:
        return "Unknown"     
    
def urban_or_rural_area(row):
    if row["Urban_or_Rural_Area"] == 1:
        return "Urban"
    elif row["Urban_or_Rural_Area"] == 2:
        return "Rural"
    else:
        return "Unknown"


# ### Calling functions to decode variable values & creating more meaningful columns

# In[24]:

df_cas['Police_Force'] = df_cas.apply(police_force,axis=1)
df_cas['Casualty_Class'] = df_cas.apply(casualty_class,axis=1)
df_cas['Sex_of_Casualty'] = df_cas.apply(sex_of_casualty,axis=1)
df_cas['Casualty_Severity'] = df_cas.apply(casualty_severity,axis=1)
df_cas['Casualty_Type'] = df_cas.apply(casualty_type,axis=1)
df_cas['geo'] = df_cas['Longitude'].apply(str) + ',' + df_cas['Latitude'].apply(str)
df_cas['Junction_Detail'] = df_cas.apply(junction_detail,axis=1)
df_cas['Junction_Control'] = df_cas.apply(junction_control,axis=1)
df_cas['Junction'] = df_cas.apply(is_junction,axis=1)
df_cas['Day_of_Week'] = df_cas.apply(day_of_week,axis=1)
df_cas['Day_Type'] = df_cas.apply(day_type,axis=1)
df_cas['1st_Road_Class'] = df_cas.apply(first_road_class,axis=1)
df_cas['Road_Name'] = df_cas.apply(road_name,axis=1)
df_cas['Road_Type'] = df_cas.apply(road_type,axis=1)
df_cas['Light_Conditions'] = df_cas.apply(light_conditions,axis=1)
df_cas['Weather_Conditions'] = df_cas.apply(weather_conditions,axis=1)
df_cas['Road_Surface_Conditions'] = df_cas.apply(road_surface_conditions,axis=1)
df_cas['Urban_or_Rural_Area'] = df_cas.apply(urban_or_rural_area,axis=1)


# ## Applying Traffic Count Values to the Casualty Locations

# ### Creating the K nearest neighbours (Knn) algorithm to merge the casualty and traffic counts data

# In[25]:

def Knn_func(row):
    
    '''K Nearest Neighbours Machine Learning algortihm to match the Casualty Point with the two most appropriate Traffic Count Points. 
    The Algorithm firstly tries to match the specific road name (e.g. A315) and if no match can be made, it matches on Road Type (e.g. ('A' Road)). 
    It returns the following variables:
    * Accident Index
    * Road Name
    * Road Type
    * Distance to relevent Count Point 1
    * Distance to relevent Count Point 2
    * Index of Relevent Count Point 1
    * Index of Relevent Count Point 2'''
    
    # Creating base variables to aid readibility
    
    road = row['Road_Name']
    accid = row['Accident_Index']
    rtype = row['1st_Road_Class']
    cas_pt = row[['Longitude','Latitude']].values
    
    # If the Roadname is known then match with traffic counts based upon that:
    
    if road in road_set: 
        tree = cKDTree(name_counts[road][['Lon_S','Lat_S']])                 # Creating the tree from the Traffic Count data
        dists, indexes = tree.query(cas_pt, k=2)                             # Querying the tree with the Casualty Point
        assign = 'Road Name'
        return accid, assign, road, rtype, dists[0], dists[1], int(indexes[0]),int(indexes[1]) # Returning the data
    
    # Else match based upon the Road Class:
    
    elif rtype in type_set:
        tree = cKDTree(type_counts[rtype][['Lon_S','Lat_S']])                 # Creating the tree from the Traffic Count data
        dists, indexes = tree.query(cas_pt, k=2)                              # Querying the tree with the Casualty Point
        assign = 'Road Type'
        return accid, assign, road, rtype, dists[0], dists[1], int(indexes[0]),int(indexes[1])  # Returning the data
        
    # Else return blank values:
    
    else:
        assign = 'None'
        return accid, assign, 'None', 'None', np.nan, np.nan, np.nan, np.nan


# ### Applying the Knn algorithm and cleaning / formatting the data

# In[26]:

df_knn_func = df_cas[['Road_Name','Accident_Index','1st_Road_Class','Longitude','Latitude']].drop_duplicates()
knn = df_knn_func.apply(Knn_func,axis=1).to_dict() 

df_knn = pd.DataFrame.from_dict(knn, orient='index') # Converting the output dictionary to a dataframe
df_knn.replace([np.inf, -np.inf], np.nan)            # Replacing infinite values with nan's
knn_cols = ['Accident_Index','Assign_Type','Road_Name','1st_Road_Class','Distance_1','Distance_2','CP_Index_1','CP_Index_2'] 
df_knn.columns = knn_cols                            # Column naming
df_knn = df_knn.drop_duplicates()                    # Removing duplicates caused by multiple casualties per Accident Index


# ### Creating Dicts of Road Names and Types

# In[27]:

name1 = {
    'CP':'CP_1',
    'Lon_Lat':'Lon_Lat_1',
    'Road':'Road_1',
    'Lon_S':'Lon_S_1',
    'Lat_S': 'Lat_S_1',
    'FdAll_MV':'FdAll_MV_1', 
    'FdPC':'FdPC_1',
    'Fd2WMV':'Fd2WMV_1',
    'FdCar':'FdCar_1', 
    'FdBUS':'FdBUS_1', 
    'FdAll_GV':'FdAll_GV_1',
    '1st_Road_Class_x':'1st_Road_Class'
}

name2 = {
    'CP':'CP_2',
    'Lon_Lat':'Lon_Lat_2',
    'Road':'Road_2',
    'Lon_S':'Lon_S_2',
    'Lat_S': 'Lat_S_2',
    'FdAll_MV':'FdAll_MV_2', 
    'FdPC':'FdPC_2',
    'Fd2WMV':'Fd2WMV_2',
    'FdCar':'FdCar_2', 
    'FdBUS':'FdBUS_2', 
    'FdAll_GV':'FdAll_GV_2',
    '1st_Road_Class_x':'1st_Road_Class'
}

# Creates dicts for Road Name and Type containing individual dataframes of Knn data: 

names_knn = {}
types_knn = {}

for road_name in road_list:
    names_knn[road_name] = df_knn[(df_knn['Assign_Type'] == 'Road Name') & (df_knn['Road_Name'] == road_name)]
    
for road_type in type_list:
    types_knn[road_type] = df_knn[(df_knn['Assign_Type'] == 'Road Type') & (df_knn['1st_Road_Class'] == road_type)]


# ### Merging the Casualty data with the traffic count data using the appropriate index variable

# In[28]:

df_roadname_1 = {'{}'.format(road_name): pd.merge(names_knn[road_name],name_counts[road_name],left_on='CP_Index_1',right_index=True,how='left') for road_name in road_list}

for road_name in road_list:
    df_roadname_1[road_name].rename(columns=name1,inplace='True')

df_roadname_2 = {'{}'.format(road_name): pd.merge(df_roadname_1[road_name],name_counts[road_name],left_on='CP_Index_2',right_index=True,how='left') for road_name in road_list}

for road_name in road_list:
    df_roadname_2[road_name].rename(columns=name2, inplace='True')
    df_roadname_2[road_name].drop(['1st_Road_Class_y'],axis=1,inplace='True')

# Road type data:

df_roadtype_1 = {'{}'.format(road_type): pd.merge(types_knn[road_type],type_counts[road_type],left_on='CP_Index_1',right_index=True,how='left') for road_type in type_list}

for road_type in type_list:
    df_roadtype_1[road_type].rename(columns=name1,inplace='True')
    
df_roadtype_2 = {'{}'.format(road_type): pd.merge(df_roadtype_1[road_type],type_counts[road_type],left_on='CP_Index_2',right_index=True,how='left') for road_type in type_list}   
    
for road_type in type_list:
    df_roadtype_2[road_type].rename(columns=name2,inplace='True')
    df_roadtype_2[road_type].drop(['1st_Road_Class_y'],axis=1,inplace='True')


# ### Putting all the dicts together

# In[29]:

cols =  df_roadname_1['A1'].columns
df_out = pd.DataFrame(columns=cols)
    
for road_name in road_list:
    df_out = pd.concat([df_out,df_roadname_2[road_name]])
    
for road_type in type_list: 
    df_out = pd.concat([df_out,df_roadtype_2[road_type]])

df_out.sort_index(inplace='True')
df_out = df_out[['1st_Road_Class','Road_Name', 'Accident_Index', 'Assign_Type','CP_1', 'CP_2', 'CP_Index_1', 'CP_Index_2', 'Distance_1', 'Distance_2',
        'Fd2WMV_1', 'Fd2WMV_2', 'FdAll_GV_1', 'FdAll_GV_2', 'FdAll_MV_1','FdAll_MV_2', 'FdBUS_1', 'FdBUS_2', 'FdCar_1', 'FdCar_2', 'FdPC_1',
        'FdPC_2', 'Lat_S_1', 'Lat_S_2', 'Lon_S_1', 'Lon_S_2', ]]


# ### Check to see if everything's worked correctly (Should return 0)

# In[30]:

len(df_knn) - len(df_out) - len(df_knn[(df_knn['Assign_Type'] == 'None')])


# ### Assigning Count Points based upon Distance

# In[31]:

# Formula to assign weight to each of the 3 CP counts and create an estimated traffic count for All Motor Vehicles (All_MV), Cycles (PC) and All Traffic (AT)

def missing(row):
    if pd.isnull(row['CP_2']) == True:     # isnull() function
        return 'Missing'
    elif pd.notnull(row['CP_2']) == True:  # notnull() function
        return 'Not Missing'
    
df_out['Missing'] = df_out.apply(missing,axis=1)

df_out_1 = df_out[(df_out['Missing'] == 'Not Missing')]
df_out_2 = df_out[(df_out['Missing'] == 'Missing')]

df_out_1['Total_Distance'] = df_out_1['Distance_1'] + df_out_1['Distance_2']
df_out_1['Distance_1_Rel'] = df_out_1['Total_Distance'] - df_out_1['Distance_1']
df_out_1['Distance_2_Rel'] = df_out_1['Total_Distance'] - df_out_1['Distance_2']
df_out_1['Total_Rel'] = df_out_1['Distance_1_Rel'] + df_out_1['Distance_2_Rel']
df_out_1['CP_1_%'] =  df_out_1['Distance_1_Rel'] / df_out_1['Total_Rel']
df_out_1['CP_2_%'] =  df_out_1['Distance_2_Rel'] / df_out_1['Total_Rel']

df_out_1['CP_1_MV_Val'] = df_out_1['FdAll_MV_1'] * df_out_1['CP_1_%']
df_out_1['CP_1_PC_Val'] = df_out_1['FdPC_1']     * df_out_1['CP_1_%']
df_out_1['CP_1_GV_Val'] = df_out_1['FdAll_GV_1'] * df_out_1['CP_1_%']
df_out_1['CP_1_BUS_Val'] = df_out_1['FdBUS_1'] * df_out_1['CP_1_%']
df_out_1['CP_1_CAR_Val'] = df_out_1['FdCar_1'] * df_out_1['CP_1_%']
df_out_1['CP_1_2WMV_Val'] = df_out_1['Fd2WMV_1'] * df_out_1['CP_1_%']

df_out_1['CP_2_MV_Val'] = df_out_1['FdAll_MV_2'] * df_out_1['CP_2_%']
df_out_1['CP_2_PC_Val'] = df_out_1['FdPC_2']     * df_out_1['CP_2_%']
df_out_1['CP_2_GV_Val'] = df_out_1['FdAll_GV_2'] * df_out_1['CP_2_%']
df_out_1['CP_2_BUS_Val'] = df_out_1['FdBUS_2'] * df_out_1['CP_2_%']
df_out_1['CP_2_CAR_Val'] = df_out_1['FdCar_2'] * df_out_1['CP_2_%']
df_out_1['CP_2_2WMV_Val'] = df_out_1['Fd2WMV_2'] * df_out_1['CP_2_%']

df_out_1['FdAll_MV'] = df_out_1['CP_1_MV_Val'] + df_out_1['CP_2_MV_Val'] 
df_out_1['FdPC'] = df_out_1['CP_1_PC_Val'] + df_out_1['CP_2_PC_Val']
df_out_1['FdAll_GV'] = df_out_1['CP_1_GV_Val'] + df_out_1['CP_2_GV_Val']
df_out_1['FdBUS'] = df_out_1['CP_1_BUS_Val'] + df_out_1['CP_2_BUS_Val']
df_out_1['FdCar'] = df_out_1['CP_1_CAR_Val'] + df_out_1['CP_2_CAR_Val']
df_out_1['Fd2WMV'] = df_out_1['CP_1_2WMV_Val'] + df_out_1['CP_2_2WMV_Val']

df_out_2['FdAll_MV'] = df_out_2['FdAll_MV_1'] 
df_out_2['FdPC'] = df_out_2['FdPC_1']
df_out_2['FdAll_GV'] = df_out_2['FdAll_GV_1'] 
df_out_2['FdBUS'] = df_out_2['FdBUS_1']
df_out_2['FdCar'] = df_out_2['FdCar_1']
df_out_2['Fd2WMV'] = df_out_2['Fd2WMV_1']

df_out = pd.concat([df_out_1,df_out_2])


# ### Output Files

# In[32]:

df_cas_out = pd.merge(df_cas,df_out,left_on='Accident_Index',right_on='Accident_Index',how='left')
df_cas_out.rename(columns={'Casualty_Lon':'S_Lon','Casualty_Lat':'S_Lat'},inplace='True')


# In[33]:

df_cas_out[(df_cas_out['Police_Force'].isin(['Metropolitan Police','City of London']))].to_csv(out + 'Casualties.csv')


# In[34]:

len(df_cas_out)


# In[ ]:



