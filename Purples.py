# python Purples.py --folder_data C:\Users\borja\Downloads\mapa\CSV --tile [7,6] --mongoDB mongodb://localhost:27017/ --parallelization 0

import numpy as np
import numpy as np
import pandas as pd
import argparse
from skimage import io
import time
import pymongo

from concurrent.futures import ThreadPoolExecutor


#Input of the script
parser = argparse.ArgumentParser()
parser.add_argument('--folder_data',  type=str, required=True,  help='Folder where the CSV files of the tiles 16x16 are located')
parser.add_argument('--mongoDB',  type=str, required=True,  help='DB url where you want to save the data.')
parser.add_argument('--tile',  type=str,  help='Tile pisition. Format example: [1,2]')
parser.add_argument('--parallelization',  type=str,  help='Number workers, 0 for not parallelization')

args = parser.parse_args()

folder = args.folder_data
tile_arg=args.tile
mongoDB=args.mongoDB
parallelization =args.parallelization

if tile_arg:
    tile=np.array(tile_arg[1:-1].split(',')).astype('int')
    x_tile = tile[0]
    y_tile = tile[1]

if parallelization==None:
    parallelization=0
else:
    parallelization=int(parallelization)



def Add_zero(a):
    if a<10:
        return ('0'+str(a))
    else:
        return str(a)
    
def Find_purples(A,zoom,t1,t2):
        B=A.copy()
        B['X']=np.floor(2**zoom*B['X']/2**4/256).astype('int')
        B['Y']=np.floor(2**zoom*B['Y']/2**4/256).astype('int')
        D=B.groupby(['X','Y']).mean().reset_index()
        C=B.groupby(['X','Y']).std().reset_index()
        G=B[np.isnan(B['mag'])].groupby(['X','Y']).mean().reset_index()
        G['mag']=True
        DD=D.merge(C,left_on=['X','Y'],right_on=['X','Y'],how='left')
        DDD=DD.merge(G,left_on=['X','Y'],right_on=['X','Y'],how='left')
        DDD.columns=['X','Y','mean','std','null']

        Purples=DDD[(DDD['mean']==22) & (DDD['null']!=True) & (DDD['std']==0 | np.isnan(DDD['std']))]
        Purples=Purples[['X','Y']]
        Purples['zoom']=zoom
        Purples.columns=['h','v','zoom']
        dic=Purples.to_dict(orient = 'records')
        if dic!=[]:
            mongo_client=pymongo.MongoClient(mongoDB)
            mongo_colection=mongo_client['map_purple_tiles']['t'+str(t1)+'_'+str(t2)]
            mongo_colection.insert_many(dic)
            mongo_colection.create_index([("zoom", pymongo.DESCENDING),("h", pymongo.DESCENDING),("v", pymongo.DESCENDING)], unique=True)
        

def Find_purples_all_zoom(t1,t2):
    print('Trying: '+'t'+str(t1)+'_'+str(t2))
    TEXTO=folder+'\h'+Add_zero(t1)+'v'+Add_zero(t2)+'.csv'
    A=pd.read_csv(TEXTO,sep=';')
    mongo_client=pymongo.MongoClient(mongoDB)
    zooms=[]
    for iii in range(5,15):
        mongo_find=mongo_client['map_purple_tiles']['t'+str(t1)+'_'+str(t2)].find_one({"zoom":iii}, {"_id": 0,})
        if mongo_find:
            zooms=zooms+[mongo_find['zoom']]
    if zooms==[]:
        max_zoom=4
    else:
        max_zoom=max(zooms)
    for iii in range(max_zoom+1,15):
        Find_purples(A,iii,t1,t2)
    mongo_client['map_purple_tiles']['hist'].insert_one({'hist':'t'+str(t1)+'_'+str(t2)})
    print("Mongo: "+'t'+str(t1)+'_'+str(t2))
    

def Find_purples_all_zoom_V(V):
    Find_purples_all_zoom(V[0],V[1])
    

#init = time.time()
if tile_arg:
    Find_purples_all_zoom(x_tile,y_tile)
else:
    mongo_hist=list(pymongo.MongoClient(mongoDB)['map_purple_tiles']['hist'].find({},{'_id':0}))
    mongo_list=[i['hist'] for i in mongo_hist]
    content=[]
    for i in range(0,16):
        for ii in range(0,16):
            name=str(i)+'_'+str(ii)
            if not (name in mongo_list):
                content=content+[(i,ii)]
    if parallelization>0:
        with ThreadPoolExecutor(max_workers=parallelization) as executor1:
            executor1.map(Find_purples_all_zoom_V,content)
    else:
        for i in content:
            Find_purples_all_zoom(i[0],i[1])

# end = time.time()
# print('Time:')
# print(end-init)




