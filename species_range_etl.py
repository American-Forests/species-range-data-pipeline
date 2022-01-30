import pandas as pd
from bs4 import BeautifulSoup
import requests
from zipfile import ZipFile, BadZipFile
from osgeo import gdal
import rasterio as rio
from rasterio.features import shapes
import geopandas as gpd
import concurrent.futures
import time
# from tqdm import tqdm
import os
import io
import re
# import fileinput
# import concurrent.futures


class SpeciesPipeline():
    def __init__(self):
        self.data_dir = os.path.join(os.getcwd(), "data")
        self.base_url = "http://charcoal.cnre.vt.edu"
        self.species_index_url = f"{self.base_url}/climate/species/speciesDist/"
        self.species_list_url = f"{self.species_index_url}/speciesList.txt"
    
    def _get_species_list(self):
        """
        this scrapes the list of species from the research page
        and returns a list of species to be scraped
        """
        raw_list = requests.get(self.species_list_url).text
        raw_io = io.StringIO(raw_list)
        species_list_df = pd.read_csv(raw_io, sep="\t", header = None)
        species_list_df.columns = ['hyphenated_name', 'regular name', 'scientific name', 'source???']
        species_list = list(species_list_df.hyphenated_name)
        return species_list
    
    def _download_and_process_species_data(self, species_list):
        start = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            executor.map(self._download_species_data_helper, species_list)
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            executor.map(self._process_species_data_helper, species_list)
    
        end = time.time()
        print(f"processing took {end - start}")
    def _process_species_data_helper(self, species):
        species_ascii_path = os.path.join(self.data_dir, "ascii", species)
        species_tif_path = os.path.join(self.data_dir, "tif", species)
        species_shapes_path = os.path.join(self.data_dir, "shapes", species)
        ascii_data = []
        tif_data = []
        shape_data = []
        for ascii_file in os.listdir(species_ascii_path):
            ascii_data.append(os.path.join(species_ascii_path, ascii_file))
            tif_data.append(os.path.join(species_tif_path, re.sub("txt", "tif", ascii_file)))
            shape_data.append(os.path.join(species_shapes_path, re.sub("txt", "shp", ascii_file)))

        for a_file, t_file, s_data in zip(ascii_data, tif_data, shape_data):
            drv = gdal.GetDriverByName('GTiff')
            ds_in = gdal.Open(a_file)
            drv.CreateCopy(t_file, ds_in)
            # mask = None
            # with rio.open(t_file) as src:
            #     image = src.read(1) 
            #     image[image > 1] = 1
            #     results = ({'properties': {'raster_val': v}, 'geometry': s} for s, v in shapes(image, mask=mask, transform=src.transform))
            #     geoms = list(results)
            #     gpd_polygonized_raster  = gpd.GeoDataFrame.from_features(geoms)
            #     gpd_polygonized_raster.to_file(s_data)

    def _download_species_data_helper(self, species):
        try:
            os.makedirs(os.path.join(self.data_dir, "zipfiles", species))
            os.makedirs(os.path.join(self.data_dir, "ascii", species))
            os.makedirs(os.path.join(self.data_dir, "tif", species))
            os.makedirs(os.path.join(self.data_dir, "shapes", species))
        except FileExistsError:
            pass
        species_url = f"{self.species_index_url}/{species}"
        species_page = requests.get(species_url)
        species_soup = BeautifulSoup(species_page.content, 'html.parser')
        species_scenarios = species_soup.find_all(class_ = 'thumbnail-file-group')
        errors = []
        for scenario in species_scenarios:
            scenario_name = scenario.find('h4').text
            if 'Image not available' in scenario.text: 
                errors.append(['image not available', scenario_name])
                continue
            scenario_files = scenario.find(class_ = 'thumbnail-file-group-02').find_all('li')
            scenario_zip_file = f"{self.base_url}/{scenario_files[1].find('a')['href']}"
            scenario_zip = requests.get(scenario_zip_file)
            scenario_zip_file_path = f"{self.data_dir}/zipfiles/{species}/{scenario_name}.zip"
            with open(scenario_zip_file_path, 'wb') as output_file:
                output_file.write(scenario_zip.content)
            try:
                with ZipFile(scenario_zip_file_path, 'r') as zf:
                    for f in zf.infolist():
                        if f.filename.startswith(species):
                            zf.extract(f, path = os.path.join(self.data_dir, "ascii"))
                        else:
                            zf.extract(f, path = os.path.join(self.data_dir, "ascii", species))
            except BadZipFile:
                errors.append(['bad zip file', scenario_zip_file_path])

    def scrape_data(self):
        """
        this is a harness function for scraping all of the data from the VT site
        """
        print("generating_species_list")
        species_list = self._get_species_list()
        self._download_and_process_species_data(species_list)

if __name__=="__main__":
    print("creating pipeline")
    pipe = SpeciesPipeline()
    pipe.scrape_data()
