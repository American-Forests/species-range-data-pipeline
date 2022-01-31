# Species Range Data Pipeline

This is a pipeline that reads in species range data from researchers at Virginia Tech and converts it into shape files to be used for an upcoming American Forests data tool!

# To use
1. Set up a `.env.` file with the following variables adjusted for your database:
```
USER = ""
PASS = ""
HOST = ""
PORT = 0
DB = ""
```
2. Set up your logs folder: `logs`.
3. Set where you want your data to be written to (we use an external drive) in the class constructor.
4. Install packages with `conda env create -f environment.yml` (to update use: `conda env update --file environment.yml  --prune`)
5. Run the pipeline with `python species_range_etl.py`