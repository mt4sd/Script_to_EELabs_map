These scripts allow for the generation the EELabs map. First, the script '**Download_VIIRS_year.py**' is used to download VIIRS data. Subsequently, '**Tiles_16x16.py**' is employed to change the projection from Equirectangular to Mercator and save the data in CSV format (required for image generation) and in MongoDB for use in the cursor value. Next, you should run '**Purples.py**', which calculates all the completely purple tiles to avoid having to ignore them in later calculations. Then, '**Create_zoom_9.py**' is used to generate images for zoom=9, which contains the highest level of detailed information. Finally, '**Create_zoom_less_9.py**' is used to calculate all zoom levels below 9, while zoom levels above 9 are obtained directly from the website.

Run order: **Download_VIIRS_year.py**, **Tiles_16x16.py**, **Purples.py**, **Create_zoom_9.py** and **Create_zoom_less_9.py**.

These scripts use Windows for the folders and paths, except Download_VIIRS_year.py that use Windows or Linux. They need EELabs_map enviroment too.

**DOWNLOAD_VIIRS_YEAR.PY DESCRIPTION**

Download_VIIRS_year.py Is a script to download data from VIIRS satellite in a year, NASA product VNP46A4. IT IS NOT TO UPDATE DOWNLOADED DATA. 

Inputs:

--out, --output Folder name and ubication where you want to save the data. Example: C:\Users\borja\Downloads\Folder REQUIRED INPUT

--year Year of the data to download REQUIRED INPUT

--token NASA EARTHDATA token. Please visit the link https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/5000/VNP46A4/2021/001/. If necessary, register an account. It is important to have an active user account to proceed. Click on 'See wget Download command' to obtain the token. If there is not a token, download a file and click on it again. The token expires every 4-6 months. REQUIRED INPUT

--OS Operating system where it is run: linux ot windows. REQUIRED INPUT

IMPORTANT: In case of interrupting the program execution, it can be relaunched to continue the download from where it left off. Check the downloaded files. Empty files have been downloaded incorrectly; they should be deleted and the script should be relaunched.

**TILES_16X16.PY DESCRIPTION**

Tiles_16x16.py is employed to change the projection from Equirectangular to Mercator and save the data in CSV format (required for image generation) and in MongoDB for use in the cursor value. CSV files corresponding to the 16x16 tiles of zoom 4. In MongoDB, they are grouped into collections based on the 16x16 tiles of zoom 4.

Inputs:

--out, --output Folder name and ubication where you want to save the data in CSV format. Example: C:\Users\borja\Downloads\Folder

--mongoDB mongoDB url where you want to save the data

--folder_data Folder where the H5 files of the downloaded data of the VIIRS are located. Example: C:\Users\borja\Downloads\Folder REQUIRED INPUT

--tile If you want to generate a single 16x16 tile.  Format example: [1,2]

--parallelization f you want to parallelize, enter the number of workers. 0 for not parallelization.

