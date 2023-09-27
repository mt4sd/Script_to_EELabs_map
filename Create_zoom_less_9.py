#python Create_zoom_less_9.py --folder_data C:\Users\borja\Downloads\V3 --mongoDB mongodb://localhost:27017/
import os
import numpy as np
from PIL import Image
import argparse
import time

from concurrent.futures import ThreadPoolExecutor

import pymongo

#Input of the script
parser = argparse.ArgumentParser()
parser.add_argument('--folder_data',  type=str,required=True,  help='Folder where the images files of the zoom=9 are located')
parser.add_argument('--mongoDB',  type=str,required=True,  help='DB url where you want to save the data.') 
args = parser.parse_args()

mongoDB=args.mongoDB
folder = args.folder_data
folder=folder+'\\map\\tiles'
 
#a=time.time()

PURPLE_img=Image.new("RGBA", (256, 256), (93, 42, 144, 255))
PURPLE_img.save(folder+'\\'+'purple.png')

def Add_zero(a):
    if a<10:
        return ('0'+str(a))
    else:
        return str(a)
    
#ZOOM <9


def Get_purples(zoom):
    purples=[]
    for i in range(0,16):
        for ii in range(0,16):
            mongo_client=pymongo.MongoClient(mongoDB)
            query = {"zoom":zoom+1} 
            mongo_colection=mongo_client['map_purple_tiles']['t'+str(i)+'_'+str(ii)]
            purples=purples+list(mongo_colection.find(query,{"_id":0,"zoom":0}))
    return purples

def Open_img(i,ii,purples,zoom,read):
    tile='h'+Add_zero(i)+'v'+Add_zero(ii)+'.png'
    purple=0
    empty=0
    z=zoom+1
    read=read[:-2]
    try:
        I=Image.open(read+'\\'+str(z)+'\\'+tile)
        I=I.resize((128,128)) 
    except:
        if {'h':i,'v':ii} in purples:
            I=Image.new("RGBA", (128, 128), (93, 42, 144, 255))
            purple=1
        else:
            I=Image.new("RGBA", (128, 128), (0, 0, 0, 0))
            empty=1
    return I,purple,empty

def Join(i,ii,purples,zoom,out,read):
    I=Image.new("RGBA", (256, 256), (0, 0, 0, 0))
    img1=Open_img(i,ii,purples,zoom,read)
    img2=Open_img(i+1,ii,purples,zoom,read)
    img3=Open_img(i,ii+1,purples,zoom,read)
    img4=Open_img(i+1,ii+1,purples,zoom,read)
    I.paste(img1[0],(0,0))
    I.paste(img2[0],(128,0))
    I.paste(img3[0],(0,128))
    I.paste(img4[0],(128,128))
    if (img1[1]+img2[1]+img3[1]+img4[1]<4) and (img1[2]+img2[2]+img3[2]+img4[2]<4):
        I.save(out+'\\'+'h'+Add_zero(int(i/2))+'v'+Add_zero(int(ii/2))+'.png')
def Join_V(V):
    Join(V[0],V[1],V[2],V[3],V[4],V[5])

def Join_parallelization(zoom,folder,tile16x16=None):
    purples=Get_purples(zoom)
    if zoom>4:
        out=folder+'\\'+str(tile16x16[0])+'_'+str(tile16x16[1])+'\\'+str(zoom)
        read=out
    elif zoom==4:
        out=folder+'\\'+str(zoom)
        read=folder+'\\'+str(tile16x16[0])+'_'+str(tile16x16[1])+'\\'+str(zoom)
    else:
        out=folder+'\\'+str(zoom)
        read=out
    try:
        os.mkdir(out)
    except:
        pass
    inputs=[]
    if tile16x16:
        r=2**(zoom+1)/2**4
        I=np.array(range(int(tile16x16[0]*r),int(tile16x16[0]*r+r)))
        II=np.array(range(int(tile16x16[1]*r),int(tile16x16[1]*r+r)))
    else:
        I=np.array(range(0,2**(zoom+1)))[::2]
        II=I
    for i in I[::2]:
        for ii in II[::2]:
            inputs=inputs+[[i,ii,purples,zoom,out,read]]
    with ThreadPoolExecutor() as executor1:
        executor1.map(Join_V,inputs)

def Join_parallelization_2(zoom,folder):
    purples=Get_purples(zoom)
    out=folder+'\\'+str(zoom)
    read=out
    try:
        os.mkdir(out)
    except:
        pass
    inputs=[]
    for i in np.array(range(0,2**(zoom+1)))[::2]:
        for ii in np.array(range(0,2**(zoom+1)))[::2]:
            inputs=inputs+[[i,ii,purples,zoom,out,read]]
    with ThreadPoolExecutor() as executor1:
        executor1.map(Join_V,inputs)


def Create_zoom_less_9(tile16x16,folder):
    for zoom in np.array(range(4,9))[::-1]:
        Join_parallelization(int(zoom),folder,tile16x16)
        print('zoom:')
        print(zoom)


def Add(text):
    f = open (folder+'\\'+'hist2.txt','r')
    new_text=f.read()+','+text
    f = open (folder+'\\'+'hist2.txt','w')
    f.write(new_text)
    f.close()

try:
    f = open (folder+'\\'+'hist2.txt','r')
    hist=f.read().split(',')
    f.close()
except:
    hist=[]
for i in range(0,16): #0,16
    for ii in range(0,16): #0,16
        if not ('t'+str(i)+'_'+str(ii) in hist):
            print('Trying: '+'t'+str(i)+'_'+str(ii))
            Create_zoom_less_9([i,ii],folder)
            try:
                Add('t'+str(i)+'_'+str(ii))
            except:
                f = open (folder+'\\'+'hist2.txt','w')
                f.write('t'+str(i)+'_'+str(ii))
                f.close()
            print('Finished: '+'t'+str(i)+'_'+str(ii))      

for zoom in np.array(range(0,4))[::-1]:
    Join_parallelization_2(int(zoom),folder)
    print('zoom:')
    print(zoom)


#print(time.time()-a)