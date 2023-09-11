# python Crear_zoom_9.py --folder_data C:\Users\borja\Downloads\mapa\CSV --output C:\Users\borja\Downloads\V2
import numpy as np
import pandas as pd
import argparse
from PIL import Image
import os
import branca
import time

from concurrent.futures import ThreadPoolExecutor


#Input of the script
parser = argparse.ArgumentParser()
parser.add_argument('--folder_data',  type=str,required=True,  help='Folder where the CSV files of the tiles 16x16 are located')
parser.add_argument('--output', '--out',  type=str,required=True,  help='Filename where you want to save the data')
args = parser.parse_args()

folder = args.folder_data

zoom=9
output = args.output

def Add_zero(a):
    if a<10:
        return ('0'+str(a))
    else:
        return str(a)

def Add_pixels(DF2):
    fx=np.array(list(set(range(DF2['X'].min(),DF2['X'].min()+256))-set(DF2['X'].values)))-DF2['X'].min()
    fy=np.array(list(set(range(DF2['Y'].min(),DF2['Y'].min()+256))-set(DF2['Y'].values)))-DF2['Y'].min()
    fx.sort()
    fy.sort()
    mag=DF2['mag'].values.reshape([len(set(DF2['X'])),len(set(DF2['Y']))])
    for i in fy:
        mag=np.insert(mag, i, mag[:,i-1], 1)
    for i in fx:
        mag=np.insert(mag, i, mag[i-1,:], 0)
    X=np.transpose(np.tile(np.array(range(DF2['X'].min(),DF2['X'].min()+256)),(256,1)))
    Y=np.tile(np.array(range(DF2['Y'].min(),DF2['Y'].min()+256)),(256,1))
    return pd.DataFrame({'X':X.reshape([-1]),'Y':Y.reshape([-1]),'mag':mag.reshape([-1])})

Colors=__mag_colors = [
    "#FFFFFF",  # 14
    "#FFFDFF",
    "#FFFBFF",
    "#FFF9FF",
    "#FFF7FF",
    "#FFF5FF",
    "#FFF3FF",
    "#FFF1FF",
    "#FFEFFF",
    "#FFEDFF",
    "#FFEBFF",
    "#FFE9FF",
    "#FFE7FF",
    "#FFE5FF",
    "#FFE2FF",
    "#FFE0FE",
    "#FFDFFC",
    "#FFDEFA",
    "#FFDDF8",
    "#FFDCF6",
    "#FDDBF4",  # 15
    "#FDDBEE",
    "#FDDBF1",
    "#FDDCEC",
    "#FDDDEA",
    "#FDDEE8",
    "#FDE0E6",
    "#FDE2E5",
    "#FDE4E4",
    "#FDE6E3",
    "#FDE8E3",
    "#FDEAE1",
    "#FDECE0",
    "#FDEEDF",
    "#FDF0DE",
    "#FDF2DD",
    "#FDF4DC",
    "#FDF5DB",
    "#FDF6DA",
    "#FCF6D9",
    "#FAF6D8",  # 16
    "#F8F7D8",
    "#F6F8D8",
    "#F4F9D7",
    "#F2FAD7",
    "#F0FBD6",
    "#EEFCD6",
    "#ECFDD6",
    "#EAFED6",
    "#E8FFD6",
    "#E6FFD8",
    "#E4FFDA",
    "#E2FFDC",
    "#E0FFDE",
    "#DFFFE0",
    "#DEFFE2",
    "#DDFFE4",
    "#DCFFE6",
    "#DBFFE8",
    "#DAFFEA",
    "#D9FFEC",  # 17
    "#D9FEEE",
    "#D9FDF0",
    "#D9FCF2",
    "#D9FBF4",
    "#D9FAF6",
    "#D9F9F8",
    "#D9F8FA",
    "#D9F7FC",
    "#D9F6FE",
    "#D8F4FE",
    "#D7F2FE",
    "#D6F0FD",
    "#D5EDFC",
    "#D4EAFB",
    "#D3E7FA",
    "#D2E4F9",
    "#D1E1F8",
    "#D0DEF7",
    "#CFDAF7",
    "#CFD4F8",  # 18
    "#CFC8F6",
    "#D0BAF4",
    "#D1AAF2",
    "#D39AEE",
    "#D58AE9",
    "#D77CE0",
    "#D970D6",
    "#DB68CB",
    "#DD61BB",
    "#DF5AAC",
    "#E0539D",
    "#E04B8C",
    "#DE437B",
    "#DC3B6A",
    "#DA335D",
    "#D82C50",
    "#D62644",
    "#D42238",
    "#D21E31",
    "#D01A2B",  # 19
    "#CE222A",
    "#CC2B29",
    "#CB3428",
    "#CB4327",
    "#CD4E26",
    "#D25A25",
    "#D86824",
    "#DE7823",
    "#E48A22",
    "#E89E21",
    "#EEAB20",
    "#F2B71F",
    "#F6C31E",
    "#FECD1E",
    "#F8D11F",
    "#EBCE20",
    "#DEC921",
    "#CCC521",
    "#BAC221",
    "#A8BE21",  # 20
    "#94BC21",
    "#7FBA23",
    "#6CB928",
    "#58B92D",
    "#4CBA36",
    "#42BC40",
    "#3ABE4C",
    "#35C05B",
    "#32C26A",
    "#2FC578",
    "#2CC886",
    "#28C994",
    "#24C8A3",
    "#21C5B0",
    "#1EC1BD",
    "#1ABCC9",
    "#17B2D1",
    "#14A6D6",
    "#119ADA",
    "#118EDC",  # 21
    "#1382DC",
    "#1576DC",
    "#176ADC",
    "#195EDB",
    "#1B52D9",
    "#1D46D7",
    "#1F3AD5",
    "#212ED3",
    "#2324D1",
    "#261CCE",
    "#2D19CB",
    "#341AC8",
    "#3B1BC4",
    "#421DBF",
    "#491FBA",
    "#5021B4",
    "#5523AB",
    "#5825A2",
    "#5B2799",
    "#5D2A90",  # 22
    "#5C2E87",
    "#5A337E",
    "#583875",
    "#553B6C",
    "#523E63",
    "#4F3F5A",
    "#4C3F51",
    "#493C49",
    "#463842",
    "#44333C",
    "#422E36",
    "#402A30",
    "#3E262B",
    "#3C2427",
    "#3A2224",
    "#382021",
    "#361E1F",
    "#341C1D",
    "#321A1B",
    "#2E1919",  # 23
    "#2A1717",
    "#261515",
    "#221313",
    "#1E1111",
    "#1A0F0F",
    "#160D0D",
    "#120C0C",
    "#0E0B0B",
    "#0A0A0A",
    "#090909",
    "#080808",
    "#070707",
    "#060606",
    "#050505",
    "#040404",
    "#030303",
    "#020202",
    "#010101",
    "#000000",
]
Min=14.0 
Max=24
Step=len(Colors)*10
cm=branca.colormap.LinearColormap(Colors,vmin=Min, vmax=Max).to_step(Step)
inter=round((Max-Min)/Step,10)

escale=[round(Min+i*inter,10) for i in range(0,Step+1)]
Colors_RGB=np.array(cm.colors)*255

def Paint_color(Z3):
    Z2=Z3.copy()
    Z2[Z2>-min(escale)]=-min(escale) 
    CZ=np.round(np.trunc(-Z2*1/inter)*inter,4) 
    PZ=(CZ-Min)/inter 
 
    NOT_NULLS=PZ.copy()
    NOT_NULLS[np.isnan(NOT_NULLS)] = -max(escale)+inter 
    RED=NOT_NULLS.copy().astype(int) 
    GREEN=NOT_NULLS.copy().astype(int)
    BLUE=NOT_NULLS.copy().astype(int)

    TRANS=PZ.copy()
    TRANS[TRANS>-100000000]=255
    TRANS[np.isnan(TRANS)] =0
    TRANS=TRANS.astype(int)

    for i in range(0,len(Colors_RGB)):
        RED[RED==i] =Colors_RGB[i][0]
        GREEN[GREEN==i] = Colors_RGB[i][1]
        BLUE[BLUE==i] = Colors_RGB[i][2]

    REDL=list(RED.reshape(-1,1))
    GREENL=list(GREEN.reshape(-1,1))
    BLUEL=list(BLUE.reshape(-1,1))
    TRANSL=list(TRANS.reshape(-1,1))
    Col_zeros=np.zeros(len(REDL)).reshape(-1,1) 

    RED_M=np.append(np.append(np.append(REDL, Col_zeros, axis = 1), Col_zeros, axis = 1),Col_zeros, axis = 1)
    GREEN_M=np.append(np.append(np.append(Col_zeros, GREENL, axis = 1), Col_zeros, axis = 1),Col_zeros, axis = 1)
    BLUE_M=np.append(np.append(np.append(Col_zeros,Col_zeros, axis = 1), BLUEL, axis = 1),Col_zeros, axis = 1)
    TRANS_M=np.append(np.append(np.append(Col_zeros,Col_zeros, axis = 1), Col_zeros, axis = 1),TRANSL, axis = 1)

    Z_COLOR=RED_M+GREEN_M+BLUE_M+TRANS_M
    if len(RED.shape)>1:
        Z_COLOR=Z_COLOR.reshape(RED.shape[0],RED.shape[1],4)
    else:
        Z_COLOR=Z_COLOR
    Z_end=Z_COLOR.astype(int) 
    return Z_end

def Create_tile(t1,t2,DFF):
             
    df=DFF[(DFF['X']>=t1*256) & (DFF['Y']>=t2*256)]
    df=df[(df['X']<(t1+1)*256) & (df['Y']<(t2+1)*256)]

    F=Add_pixels(df)
    F_C=-np.transpose(F['mag'].values.reshape([256,256]))
    F2=Paint_color(F_C)
    F2=Image.fromarray(F2.astype("uint8"))
    return F2
def Create_tile_V(V):
    F=Create_tile(V[0],V[1],V[2])
    F.save(V[3]+"\h"+Add_zero(V[0])+"v"+Add_zero(V[1])+".png")
    print('Created tile: '+str(V[0])+'_'+str(V[1]))
    return F

a=time.time()
cont=0
inicio = time.time()

k=int(2**zoom/16)

def Add(text):
    f = open (output+"\map"+"\\tiles\\"+'hist.txt','r')
    new_text=f.read()+','+text
    f = open (output+"\map"+"\\tiles\\"+'hist.txt','w')
    f.write(new_text)
    f.close()

try:
    f = open (output+"\map"+"\\tiles\\"+'hist.txt','r')
    hist=f.read().split(',')
    f.close()
except:
    hist=[]

for i in range(0,16):
    for ii in range(0,16):
        
        if not (str(i)+'_'+str(ii) in hist):
            out=output+"\map"+"\\tiles\\"+str(i)+"_"+str(ii)+"\\"+str(zoom)
            os.makedirs(out, exist_ok=True)
            print('Trying: '+str(i)+'_'+str(ii))
            TEXT=folder+'\h'+Add_zero(i)+'v'+Add_zero(ii)+'.csv'
            A=pd.read_csv(TEXT,sep=';')
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
            tiles=DDD[~((DDD['mean']==22) & (DDD['null']!=True) & (DDD['std']==0 | np.isnan(DDD['std']))|np.isnan(DDD['mean']))]
            tiles=tiles[['X','Y']]

            DF=A.copy()
            DF['X']=np.floor(2**zoom*DF['X']/2**4).astype('int')
            DF['Y']=np.floor(2**zoom*DF['Y']/2**4).astype('int')
            DFF=DF.groupby(['X','Y']).mean().reset_index()
            array_tiles=[]
            for j in tiles.values:
                name='h'+Add_zero(j[0])+'v'+Add_zero(j[1])+'.png'
                if not (name in os.listdir(out)):
                    array_tiles=array_tiles+[tuple(list(j)+[DFF]+[out])]
        

            with ThreadPoolExecutor() as executor1:
                executor1.map(Create_tile_V,array_tiles)
 
            try:
                Add(str(i)+'_'+str(ii))
            except:
                f = open (output+"\map"+"\\tiles\\"+'hist.txt','w')
                f.write(str(i)+'_'+str(ii))
                f.close()
            print('Finished: '+str(i)+'_'+str(ii))             



