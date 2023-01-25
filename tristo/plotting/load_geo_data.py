import io
import zipfile

import geopandas as gpd

from ..paths import PATH_SUPP
from ..utils import download_file

lau_file = 'ref-lau-2020-01m.shp.zip'
lau_url = f'https://gisco-services.ec.europa.eu/distribution/v2/lau/download/{lau_file}'

LAU_version = 'LAU_RG_01M_2020_4326'
if not (PATH_SUPP / lau_file).exists():
    file_name = download_file(lau_url, 'LAU-SHP-Data')
if not (PATH_SUPP / LAU_version).exists():
    with zipfile.ZipFile(PATH_SUPP / lau_file) as arch:
        file = arch.read(f'{LAU_version}.shp.zip')
        with zipfile.ZipFile(io.BytesIO(file)) as sub_arch:
            sub_arch.extractall(path=PATH_SUPP / LAU_version) 

path_shp_DE = PATH_SUPP /f'{LAU_version}_DE'
if not path_shp_DE.exists():
    path_shp_DE.mkdir()
if not (path_shp_DE/'DE.shp').exists():
    shp_df = gpd.read_file(PATH_SUPP / LAU_version /f'{LAU_version}.shp').query('CNTR_CODE == "DE"')
    shp_df['state'] = shp_df.LAU_ID.str.slice(0,2)
    shp_df['LAU_ID'] = shp_df['LAU_ID'].astype(int)
    shp_df.to_file(path_shp_DE /'DE.shp')

cities_file = 'ref-urau-2021-100k.shp.zip'
cities_url = f'https://gisco-services.ec.europa.eu/distribution/v2/urau/download/{cities_file}'
cities_dataset = 'URAU_LB_2021_4326_CITIES'


if not (PATH_SUPP / cities_file).exists():
    file_name = download_file(cities_url, 'Cities-Data')

if not (PATH_SUPP / cities_dataset).exists():
    with zipfile.ZipFile(PATH_SUPP /cities_file) as arch:
        file = arch.read(f'{cities_dataset}.shp.zip')
        with zipfile.ZipFile(io.BytesIO(file)) as sub_arch:
            sub_arch.extractall(path=PATH_SUPP / cities_dataset) 

path_cities_DE = PATH_SUPP / f'{cities_dataset}_DE'
if not path_cities_DE.exists():
    path_cities_DE.mkdir()
if not (path_cities_DE / 'cities_DE.shp').exists():
    cities_df = gpd.read_file(PATH_SUPP / cities_dataset / f'{cities_dataset}.shp').query('CNTR_CODE == "DE"')
    cities_df['AREA_SQM']= cities_df['AREA_SQM'].astype(int)
    cities_df.to_crs('EPSG:4326').to_file(path_cities_DE / 'cities_DE.shp')

shapes_DE = gpd.read_file(path_shp_DE /'DE.shp')
states = shapes_DE.dissolve(by='state')
country = shapes_DE.dissolve(by='CNTR_CODE')
cities = gpd.read_file(path_cities_DE / 'cities_DE.shp')
