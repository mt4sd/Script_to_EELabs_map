import pandas as pd
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('--out','--output', required=True, type=str, help='Output filename')
parser.add_argument('--year',  required=True, type=int, help='Year')
parser.add_argument('--token', required=True, type=str, help='NASA EARTHDATA token. Please visit the link https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A4/2021/001/. If necessary, register an account. It is important to have an active user account to proceed. Click on -See wget Download command- to obtain the token. If there is not a token, download a file and click on it again. The token expires every 4-6 months.')
parser.add_argument('--OS',required=True,help='Operating system: linux ot windows')

args = parser.parse_args()

output=args.out
year=args.year
token=args.token
OS=args.OS


#BEGINING NASA FUNCTIONS
#from __future__ import (division, print_function, absolute_import, unicode_literals)

import argparse
import os
import os.path
import shutil
import sys
from io import StringIO 

USERAGENT = 'tis/download.py_1.0--' + sys.version.replace('\n','').replace('\r','')


def geturl(url, token=None, out=None):
    headers = { 'user-agent' : USERAGENT }
    if not token is None:
        headers['Authorization'] = 'Bearer ' + token
    try:
        import ssl
        CTX = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        if sys.version_info.major == 2:
            import urllib2
            try:
                fh = urllib2.urlopen(urllib2.Request(url, headers=headers), context=CTX)
                if out is None:
                    return fh.read()
                else:
                    shutil.copyfileobj(fh, out)
            except urllib2.HTTPError as e:
                print('HTTP GET error code: %d' % e.code(), file=sys.stderr)
                print('HTTP GET error message: %s' % e.message, file=sys.stderr)
            except urllib2.URLError as e:
                print('Failed to make request: %s' % e.reason, file=sys.stderr)
            return None

        else:
            from urllib.request import urlopen, Request, URLError, HTTPError
            try:
                fh = urlopen(Request(url, headers=headers), context=CTX)
                print(fh)
                if out is None:
                    return fh.read().decode('utf-8')
                else:
                    shutil.copyfileobj(fh, out)
            except HTTPError as e:
                print('HTTP GET error code: %d' % e.code(), file=sys.stderr)
                print('HTTP GET error message: %s' % e.message, file=sys.stderr)
            except URLError as e:
                print('Failed to make request: %s' % e.reason, file=sys.stderr)
            return None

    except AttributeError:
        # OS X Python 2 and 3 don't support tlsv1.1+ therefore... curl
        import subprocess
        try:
            args = ['curl', '--fail', '-sS', '-L', '--get', url]
            for (k,v) in headers.items():
                args.extend(['-H', ': '.join([k, v])])
            if out is None:
                # python3's subprocess.check_output returns stdout as a byte string
                result = subprocess.check_output(args)
                return result.decode('utf-8') if isinstance(result, bytes) else result
            else:
                subprocess.call(args, stdout=out)
        except subprocess.CalledProcessError as e:
            print('curl GET error message: %' + (e.message if hasattr(e, 'message') else e.output), file=sys.stderr)
        return None

def sync(src, dest, tok,A):
    '''synchronize src url with dest directory'''
    try:
        import csv
        files = [ f for f in csv.DictReader(StringIO(geturl('%s.csv' % src, tok)), skipinitialspace=True) ]
    except ImportError:
        import json
        files = json.loads(geturl(src + '.json', tok))

    # use os.path since python 2/3 both support it while pathlib is 3.4+
    for f in files:
        print(f['name'])
        if f['name'] in A:
          # currently we use filesize of 0 to indicate directory
          filesize = int(f['size'])
          path = os.path.join(dest, f['name'])
          url = src + '/' + f['name']
          if filesize == 0:
              try:
                  print('creating dir:', path)
                  os.mkdir(path)
                  sync(src + '/' + f['name'], path, tok)
              except IOError as e:
                  print("mkdir `%s': %s" % (e.filename, e.strerror), file=sys.stderr)
                  sys.exit(-1)
          else:
              try:
                  if not os.path.exists(path):
                      print('downloading: ' , path)
                      with open(path, 'w+b') as fh:
                          geturl(url, tok, fh)
                  else:
                      print('skipping: ', path)
              except IOError as e:
                  print("open `%s': %s" % (e.filename, e.strerror), file=sys.stderr)
                  sys.exit(-1)
    return 0

#END NASA FUNCTIONS

def add_zero(a):
    if a<10:
        return ('0'+str(a))
    else:
        return str(a)
    
#Formato de los días con los 1->001, 23->023 0 124->124
def day_format(day):
    if day<10:
        return '00'+str(day)
    elif day<100:
        return '0'+str(day)
    else:
        return str(day)
    
tiles=[]
for i in range(0,18):
    for ii in range(0,36):
        tiles=tiles+['h'+add_zero(ii)+'v'+add_zero(i)]    


def Name(year,day,product):
    day=day_format(day)
    csv=pd.read_csv('https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/'+product+'/'+str(year)+'/'+day+'.csv')
    files=[(None if i==[] else i[0]) for i in [[i for i in csv['name'] if ii in i] for ii in tiles]]
    return files


def Download(year,day,product):
    A=set(Name(year,day,product))
    url='https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/'+product+'/'+str(year)+'/'+day_format(day) 

    if OS=='linux':
        out=output+'/year_'+str(year)+'/day_'+day_format(day)
    elif OS=='windows':
        out=output+'\\year_'+str(year)+'\\day_'+day_format(day)
    else:
        print('ERROR in OS input')
    os.makedirs(out, exist_ok=True)
    sync(url,out,token,A)
    
Download(year,1,'VNP46A4')




