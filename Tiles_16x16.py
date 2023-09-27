#python Tiles_16x16.py --folder_data C:\Users\borja\Downloads\ano_2022\dia_001 --tile [7,6] --out C:\Users\borja\Downloads\V2 --mongoDB mongodb://localhost:27017/
import numpy as np
import pandas as pd
import argparse
from skimage import io
import os
import time
import pymongo
import h5py    
import re


from concurrent.futures import ThreadPoolExecutor



#Input of the script
parser = argparse.ArgumentParser()
parser.add_argument('--folder_data',  type=str,required=True,  help='Folder where the H5 files of the downloaded data of the VIIRS are located')
parser.add_argument('--tile',  type=str,  help='Tile pisition. Format example: [1,2]')
parser.add_argument('--mongoDB',  type=str,  help='If saving data in MongoDB. DB url where you want to save the data.')#"mongodb://{}:{}/".format('localhost','27017')
parser.add_argument('--output', '--out',  type=str,  help='If saving data in CSV dataset. Filename where you want to save the data')
parser.add_argument('--parallelization',  type=str,  help='Number workers, 0 for not parallelization')
args = parser.parse_args()

folder = args.folder_data
tile_arg=args.tile
mongoDB=args.mongoDB
output = args.output
parallelization =args.parallelization

if parallelization==None:
    parallelization=0
else:
    parallelization=int(parallelization)



if mongoDB==None:
    mongo=False
else:
    mongo=True
if output==None:
    dataset_save=False
else: 
    dataset_save=True
if  (mongoDB==None) and (output==None):
    print('ERROR not save input')
    exit() 

if tile_arg:
    tile=np.array(tile_arg[1:-1].split(',')).astype('int')
    x_tile = tile[0]
    y_tile = tile[1]

zoom=4

def irradiance_to_mag(ir):
    a=np.round(np.log10(ir)*-0.95+20.93,2) #We use the https://www.mdpi.com/2072-4292/15/17/4189 results
    a[ir==0]=22
    a[a>22]=22
    return a 

def degree_to_rad(alfa):
    return alfa*2*np.pi/360

def rad_to_degree(alfa):
    return alfa*360/(2*np.pi)

def equirectangular_to_mercator(longitude,latitude,zoom):
    longitude=degree_to_rad(longitude)
    latitude=degree_to_rad(latitude)

    x=256*2**zoom*(np.pi+longitude)/(2*np.pi)
    y=256*2**zoom*(np.pi-np.log(np.tan(np.pi/4+latitude/2)))/(2*np.pi)
    return x,y

def mercator_to_equirectangular(x,y,zoom):
    longitude=2*np.pi*x/(256*2**zoom)-np.pi
    latitude=2*np.arctan(np.exp(np.pi-2*np.pi*y/(256*2**zoom)))-np.pi/2

    return rad_to_degree(longitude),rad_to_degree(latitude)

def get_tile(V): 
    lon=V[0]
    lat=V[1]
    v=np.floor((90-lat)/10)
    h=np.floor((lon+180)/10)
    return int(v),int(h)

def get_left_upper_corner(V):
    lat=90-V[1]*10
    lon=V[0]*10-180
    return lon,lat

def add_zero(a):
    if a<10:
        return ('0'+str(a))
    else:
        return str(a)
    
def get_necesary_equirectangular_tiles(x,y,zoom): #Listado de teselas equireactangular necesarias para una tesela mercator
    left_upper_corner=get_tile(mercator_to_equirectangular(x*256,y*256,zoom))
    right_lower_corner=get_tile(mercator_to_equirectangular(256*(x+1),256*(y+1),zoom))
    V=[]
    for i in range(left_upper_corner[0],right_lower_corner[0]+1):
        for ii in range(left_upper_corner[1],right_lower_corner[1]+1):
            V=V+['h'+add_zero(ii)+'v'+add_zero(i)]
    return V

def name_to_tile(name):
    return int(name[1:3]),int(name[4:])

def degree_decimal_to_degree_hexadecimal(degree_decimal): #Borja
            degree=np.floor(degree_decimal).astype('int') #Borja
            minute_decimal=(degree_decimal-degree)*60 #Borja
            minute=np.floor(minute_decimal).astype('int') #Borja
            second=np.floor((minute_decimal-minute)*60).astype('int') #Borja
            return (degree,minute,second) #Borja

def create_tile(t1,t2,zoom,u):
    print('Trying: '+'t'+str(t1)+'_'+str(t2))
    DF=pd.DataFrame()
    files=os.listdir(folder)
    files2=np.array([i.split('.')[2] for i in files])

    for i in get_necesary_equirectangular_tiles(t1,t2,zoom):
        p=np.where(files2==i)[0]
        try:
            Data=pd.DataFrame()
            h5file = h5py.File(folder+"\\"+files[p[0]],"r")
            var1=np.array(h5file['HDFEOS']['GRIDS']['VIIRS_Grid_DNB_2d']['Data Fields']['AllAngle_Composite_Snow_Free'])
            var2=np.array(h5file['HDFEOS']['GRIDS']['VIIRS_Grid_DNB_2d']['Data Fields']['lat'])
            var3=np.array(h5file['HDFEOS']['GRIDS']['VIIRS_Grid_DNB_2d']['Data Fields']['lon'])

            Data['AllAngle_Composite_Snow_Free']=var1.reshape(1,-1)[0]
            Data['lat']=list(var2.reshape(1,-1)[0])*len(var3)
            Data['lon']=np.array([[i]*len(var2) for i in var3.reshape(1,-1)[0]]).reshape(1,-1)[0]
            Data['AllAngle_Composite_Snow_Free']=Data['AllAngle_Composite_Snow_Free'].replace({65535:np.nan})*0.1 #Cambio nulos
            Data=Data.sort_values(['lat','lon'],ascending=[False,True])
        except:
            Data=pd.DataFrame()
            Data['AllAngle_Composite_Snow_Free']=[np.nan]*5760000


        left_upper_corner=get_left_upper_corner(name_to_tile(i))
        LON=np.linspace(left_upper_corner[0],left_upper_corner[0]+10,2401)[:-1]
        LAT=np.linspace(left_upper_corner[1],left_upper_corner[1]-10,2401)[:-1]
        LON=np.tile(LON, (1, 2400))[0]
        LAT=np.tile(LAT, (1, 2400))[0]
        LON=np.transpose(LON.reshape(2400,2400)).reshape([-1])
        Data['lat']=LAT
        Data['lon']=LON
        (X,Y)=equirectangular_to_mercator(Data['lon'].values,Data['lat'].values,zoom)
        Data['X']=X
        Data['Y']=Y
        Data=Data[(Data['X']>=t1*256) & (Data['Y']>=t2*256)]
        Data=Data[(Data['X']<(t1+1)*256) & (Data['Y']<(t2+1)*256)]
        Data=Data[(Data['Y']>=0)]
          
        DF=pd.concat([DF,Data])

    DF['mag']=irradiance_to_mag(DF['AllAngle_Composite_Snow_Free'])

    if dataset_save and (u==1 or u==0):
        out=output+"\map"+"\\CSV\\"
        os.makedirs(out, exist_ok=True)
        DF[['mag','X','Y']].to_csv(out+"\h"+add_zero(t1)+"v"+add_zero(t2)+".csv", sep=';',index=False)
    if dataset_save and (u==1 or u==0):
        print("h"+add_zero(t1)+"v"+add_zero(t2)+".csv")

    if mongo and (u==2 or u==0):

        DF2=DF[DF['mag']>0][['mag','lon','lat']]

        Degree_hex_LON=degree_decimal_to_degree_hexadecimal(DF2['lon'])
        Degree_hex_LAT=degree_decimal_to_degree_hexadecimal(DF2['lat'])
        DF2['grad_lon']=Degree_hex_LON[0]
        DF2['min_lon']=Degree_hex_LON[1]
        DF2['sec_lon']=(np.round(Degree_hex_LON[2]/15)*15).astype('int')
        DF2['grad_lat']=Degree_hex_LAT[0]
        DF2['min_lat']=Degree_hex_LAT[1]
        DF2['sec_lat']=(np.round(Degree_hex_LAT[2]/15)*15).astype('int')
        DF2['min_lon']=DF2['min_lon']+(DF2['sec_lon']==60).astype('int')
        DF2['sec_lon']=DF2['sec_lon']*(DF2['sec_lon']!=60).astype('int')
        DF2['grad_lon']=DF2['grad_lon']+(DF2['min_lon']==60).astype('int')
        DF2['min_lon']=DF2['min_lon']*(DF2['min_lon']!=60).astype('int')
        DF2['min_lat']=DF2['min_lat']+(DF2['sec_lat']==60).astype('int')
        DF2['sec_lat']=DF2['sec_lat']*(DF2['sec_lat']!=60).astype('int')
        DF2['grad_lat']=DF2['grad_lat']+(DF2['min_lat']==60).astype('int')
        DF2['min_lat']=DF2['min_lat']*(DF2['min_lat']!=60).astype('int')
        DF2=DF2[['mag','grad_lon','min_lon','sec_lon','grad_lat','min_lat','sec_lat']]

        dic=DF2.to_dict(orient = 'records')
        mongo_client=pymongo.MongoClient(mongoDB)
        mongo_colection=mongo_client['map_values']['t'+str(t1)+'_'+str(t2)]
        mongo_colection.drop() 
        try:
            mongo_colection.insert_many(dic)
            mongo_colection.create_index([("grad_lat", pymongo.DESCENDING),("grad_lon", pymongo.DESCENDING),("min_lat", pymongo.DESCENDING),("min_lon", pymongo.DESCENDING),("sec_lat", pymongo.DESCENDING),("sec_lon", pymongo.DESCENDING)], unique=True)
        except:
            mongo_colection.insert_one({})
        print("Mongo: "+'t'+str(t1)+'_'+str(t2))

def create_tile_V(V):
    create_tile(V[0],V[1],V[2],V[3])


# init = time.time()
if tile_arg:
    create_tile(x_tile,y_tile,zoom,0)
else:
    ALL_NAME=[]
    for i in range(0,2**zoom):
        for ii in range(0,2**zoom):
            ALL_NAME=ALL_NAME+[str(i)+'_'+str(ii)]
    if mongo:
        mongo_client=pymongo.MongoClient(mongoDB)
        mongo_set=set(mongo_client['map_values'].list_collection_names())
        incompletes=[]
        for k in range(0,16):
            for kk in range(0,16):
                mongo_colection=mongo_client['map_values'][str(k)+'_'+str(kk)] #4,12
                index=mongo_colection.index_information()
                try:
                    index['grad_lat_-1_grad_lon_-1_min_lat_-1_min_lon_-1_sec_lat_-1_sec_lon_-1']
                    have_index=True
                except:
                    have_index=False
                try:
                    mongo_colection.find_one()['mag'] 
                except:
                    have_index=True
                if not(have_index):
                    incompletes=incompletes+[str(k)+'_'+str(kk)]
        mongo_list=list(mongo_set-set(incompletes))
    else: 
        mongo_list=ALL_NAME

    if dataset_save:
        CSVs= os.listdir(output+"\map"+"\\CSV")
        CVSs_list=[str(int(re.split('v|h|_|.c', i)[1]))+'_'+str(int(re.split('v|h|_|.c', i)[2])) for i in CSVs]
    else:
        CVSs_list=ALL_NAME

    content=[]
    for i in range(0,2**zoom):
        for ii in range(0,2**zoom):
            name=str(i)+'_'+str(ii)
            if not (name in mongo_list) and not (name in CVSs_list):
                content=content+[(i,ii,zoom,0)]
            elif (name in mongo_list) and not (name in CVSs_list):
                content=content+[(i,ii,zoom,1)]
            elif not (name in mongo_list) and (name in CVSs_list):
                content=content+[(i,ii,zoom,2)]

    if parallelization>0:
        with ThreadPoolExecutor(max_workers=parallelization) as executor1:
            executor1.map(create_tile_V,content)
    else:
        for i in content:
            create_tile(i[0],i[1],i[2],i[3])

# end = time.time()
# print('Time:')
# print(end-init)


