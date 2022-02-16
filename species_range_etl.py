import pandas as pd
from bs4 import BeautifulSoup
import requests
from zipfile import ZipFile, BadZipFile
from osgeo import gdal
import geopandas as gpd
import concurrent.futures
from multiprocessing import Pool
import os
import io
import re
import arcpy
from sqlalchemy import create_engine
import logging
from dotenv import load_dotenv
from datetime import datetime


class SpeciesPipeline:
    def __init__(self):
        """
        Initializing the directories and logger for the species data pipeline
        """
        self.data_dir = os.path.join("Y:", "AmericanReLeaf", "SpeciesRange")
        self.base_url = "http://charcoal.cnre.vt.edu"
        self.species_index_url = f"{self.base_url}/climate/species/speciesDist/"
        self.species_list_url = f"{self.species_index_url}/speciesList.txt"
        logging.basicConfig(filename="logs/data-pipeline.log", level=logging.DEBUG)
        self.logger = logging.getLogger()

    def _get_species_list(self):
        """
        this scrapes the list of species from the research page
        and returns a list of species to be scraped
        """
        raw_list = requests.get(self.species_list_url).text
        raw_io = io.StringIO(raw_list)
        species_list_df = pd.read_csv(raw_io, sep="\t", header=None)
        species_list_df.columns = [
            "hyphenated_name",
            "regular name",
            "scientific name",
            "source???",
        ]
        species_list = list(species_list_df.hyphenated_name)
        return species_list

    def _generate_species_folders(self, species):
        """
        setting up the folder structure for the data downloads
        """
        folder_categories =  [ "tif","shapes","zipfiles","ascii" ]
        for category in folder_categories:
            try:
                os.makedirs(os.path.join(self.data_dir, category, species))
            except FileExistsError:
                self.logger.debug(f"{category} folder already exists for {species}")

    def _convert_to_ASCII_helper(self, species):
        """
        convert the raw txt files to ascii
        """
        species_ascii_path = os.path.join(self.data_dir, "ascii", species)
        for ascii_file in os.listdir(species_ascii_path):
            if ascii_file.endswith("asc"):
                self.logger.debug(
                    f"ascii file already exists for {species} {ascii_file}"
                )
                continue
            ascii_path = os.path.join(species_ascii_path, ascii_file)
            ascii_path_fixed = os.path.join(
                species_ascii_path, re.sub("txt", "asc", ascii_file)
            )
            os.rename(ascii_path, ascii_path_fixed)

    def _convert_to_tif_helper(self, species):
        """
        take ascii files, produce tif files, we need tif files to use arcpy libraries
        """
        species_ascii_path = os.path.join(self.data_dir, "ascii", species)
        species_tif_path = os.path.join(self.data_dir, "tif", species)
        ascii_data = [file for file in os.listdir(species_ascii_path)]
        tif_data = [
            re.sub("asc", "tif", file) for file in os.listdir(species_ascii_path)
        ]

        for a_file, t_file in zip(ascii_data, tif_data):
            drv = gdal.GetDriverByName("GTiff")
            asc_raster = os.path.join(species_ascii_path, a_file)
            tif_raster = os.path.join(species_tif_path, t_file)
            ds_in = gdal.Open(asc_raster)
            drv.CreateCopy(tif_raster, ds_in)

    def _convert_to_shape_helper(self, species):
        """
        take the tif files and split into the various thresholds
        convert the rasters to shapefiles
        """
        species_tif_path = os.path.join(self.data_dir, "tif", species)
        species_shapes_path = os.path.join(self.data_dir, "shapes", species)
        cut_off_thresholds = [0.25, 0.5, 0.75]

        for tif_raster in [i for i in os.listdir(species_tif_path) if i.endswith(".tif")]:
            tif_raster_path = os.path.join(species_tif_path, tif_raster)
            raster_in = arcpy.sa.Raster(tif_raster_path)

            for threshold in cut_off_thresholds:
                # generate subsetted raster
                shape_file = re.sub(".tif", ".shp", tif_raster)
                raster_out_file = os.path.join(
                    species_tif_path, f"clean_{int(threshold*100)}_{tif_raster}"
                )
                s_data_path = os.path.join(
                    species_shapes_path, f"{int(threshold * 100)}_{shape_file}"
                )
                if os.path.exists(s_data_path):
                    print("skipping", s_data_path)
                    continue

                outCon = arcpy.sa.Con(raster_in >= threshold, 0)
                outCon.save(raster_out_file)
                # convert raster to polygon
                arcpy.RasterToPolygon_conversion(raster_out_file, s_data_path)

                try:
                    # set the right CRS
                    temp = gpd.read_file(s_data_path)
                    temp = temp.set_crs(epsg=4326)
                    temp.to_file(s_data_path)
                except:
                    self.logger.debug("failed to fix crs of", s_data_path)

    def _download_species_data_helper(self, species):
        """
        download function that downloads all data for
        a given species
        """
        species_url = f"{self.species_index_url}/{species}"
        species_page = requests.get(species_url)
        species_soup = BeautifulSoup(species_page.content, "html.parser")
        species_scenarios = species_soup.find_all(class_="thumbnail-file-group")
        for scenario in species_scenarios:
            scenario_name = scenario.find("h4").text
            if "Image not available" in scenario.text:
                self.logger.debug(f"image not available {species} {scenario_name}")
                continue
            scenario_files = scenario.find(class_="thumbnail-file-group-02").find_all(
                "li"
            )
            scenario_zip_file = f"{self.base_url}/{scenario_files[1].find('a')['href']}"
            scenario_zip = requests.get(scenario_zip_file)
            scenario_zip_file_path = os.path.join(
                self.data_dir, "zipfiles", species, f"{scenario_name}.zip"
            )
            with open(scenario_zip_file_path, "wb") as output_file:
                output_file.write(scenario_zip.content)
            try:
                with ZipFile(scenario_zip_file_path, "r") as zf:
                    for f in zf.infolist():
                        if f.filename.startswith(species):
                            zf.extract(f, path=os.path.join(self.data_dir, "ascii"))
                        else:
                            zf.extract(
                                f, path=os.path.join(self.data_dir, "ascii", species)
                            )
            except BadZipFile:
                self.logger.debug(f"bad zip file {species} {scenario_zip_file_path}")
        zip_path = os.path.join(self.data_dir, "zipfiles", species)
        os.rmdir(zip_path)

    def _load_species_data_helper(self, species):
        """
        completes final data cleaning steps for the postgis database
        """
        species_shape_folder = os.path.join(self.data_dir, "shapes", species)
        species_shapes = [
            i for i in os.listdir(species_shape_folder) if i.endswith(".shp")
        ]
        if not len(species_shapes):
            self.logger.debug(f"no shape files for {species}")
            return None

        data_results = []
        for s_file in species_shapes:
            details = s_file[:-4].split("_")
            threshold = details[0]
            if details[1] == "current":
                source = "vtech"
                scenario = "current"
                year = "2020"
            else:
                source = details[1]
                scenario = details[2]
                year = details[3][1:]
            data = gpd.read_file(os.path.join(species_shape_folder, s_file))
            data["threshold"] = threshold
            data["source"] = source
            data["year"] = year
            data["scenario"] = scenario
            data["species"] = species
            data = data.dissolve(by="species")
            data_results.append(data)

        data_results = gpd.GeoDataFrame(
            pd.concat(data_results), crs=data_results[0].crs
        ).rename(columns={"Id": "species_id"})
        return data_results

    def _load_species_data(self):
        """
        a wrapper function to parallelize the data loading
        """
        self.logger.info("transforming the data into a dataframe")
        with Pool(4) as process_pool:
            result = process_pool.map(self._load_species_data_helper, self.species_list)

        self.logger.info("updating column names")
        result = [i for i in result if i is not None]
        all_data = gpd.GeoDataFrame(pd.concat(result), crs=result[0].crs).rename(
            columns={"Id": "species_id"}
        )
        # all_data["year"] = pd.to_datetime(all_data["year"], format="%Y")
        all_data["area"] = all_data.geometry.area
        all_data = all_data.reset_index()

        self.logger.info("loading into postgres")
        engine = create_engine(
            f"postgresql://{os.getenv('USER')}:{os.getenv('PASS')}@{os.getenv('HOST')}:{os.getenv('PORT')}/{os.getenv('DB')}"
        )
        # all_data.to_file(os.path.join(self.data_dir, 'all_species.shp'))
        all_data.to_postgis("speciesdata", engine, if_exists="replace", index=True, index_label="sid", chunksize=5)


    def setup(self):
        """
        set up function to get the species list and data folders
        """
        self.logger.info("pulling down the species list")
        self.species_list = self._get_species_list()
        self.logger.info("generating the data folders")
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            executor.map(self._generate_species_folders, self.species_list)

    def extract(self):
        """
        the extract step of the data pipeline
        """
        self.logger.info("downloading the data")
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            executor.map(self._download_species_data_helper, self.species_list)

    def transform(self):
        """
        transformation step of the data pipeline
        """
        self.logger.info("converting the data from txt to ascii")
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            executor.map(self._convert_to_ASCII_helper, self.species_list)
        self.logger.info("converting the data from ascii to tif")
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            executor.map(self._convert_to_tif_helper, self.species_list)
        self.logger.info("converting the data from tif to shp")
        with Pool(4) as process_pool:
            process_pool.map(self._convert_to_shape_helper, self.species_list)

    def load(self):
        """
        loading the data into the database
        """
        self.logger.info("loading the data")
        self._load_species_data()


if __name__ == "__main__":
    arcpy.env.overwriteOutput = True
    load_dotenv(dotenv_path=".env")
    pipe = SpeciesPipeline()
    pipe.setup()
    pipe.extract()
    pipe.transform()
    pipe.load()
