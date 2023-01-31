## combining asos-mirror and asos-parse

# %%  TODO: don't use dask for this
from ftplib import FTP, error_perm, error_temp
from pathlib import Path
from itertools import product


from tqdm import tqdm
import s3fs
from cloudside.asos import MetarParser
from dask import bag
from distributed import Client
from coiled import Cluster

# %% Create a cluster and client with coiled
cluster = Cluster(name="asos-mirror", n_workers=5, package_sync=True)
client = Client(cluster)
client


# %% set up the data we're going to download
fs = s3fs.S3FileSystem()
email = "paul+faa@coiled.io"
stations = [
    "KATL", "KPDX",
    "KBHM", "KLGA", "KSTL", "KSEA",
    "KDFW", "KORD", "KLAX", "KSFO",
]

# %%
def get_all_files(station, year):
    with FTP("ftp.ncei.noaa.gov") as ftp:
        ftp.login(passwd=email)
        try:
            dat_files = ftp.nlst(f"/pub/data/asos-fivemin/6401-{year}/*{station}*")
        except error_perm as e:
            print(e)
    return dat_files


def precip_process(df):
    from cloudside.asos import FIVEMIN, _find_reset_time, _process_precip
    data = df.groupby("datetime").last().sort_index().resample(FIVEMIN).asfreq()

    rt = _find_reset_time(data["raw_precipitation"])
    precip = _process_precip(data, rt, "raw_precipitation")
    return data.assign(precip=precip)


def transfer(dat_in, dat_out_uri):
    if not fs.exists(dat_out_uri) or (fs.size(dat_out_uri) == 0):
        with fs.open(dat_out_uri, "w") as dat_out, \
             FTP("ftp.ncei.noaa.gov") as ftp:
            ftp.login(passwd=email)
            try:
                ftp.retrlines(
                    f"RETR {dat_in}", lambda x: dat_out.write(x + "\n")
                )
                status = "transferred"
                file_size = fs.size(dat_out_uri)
            except Exception as e:
                file_size = None
                status = str(e)
    else:
        file_size = fs.size(dat_out_uri)
        status = "exists already"

    return {
        "source": dat_in,
        "file": dat_out_uri,
        "status": status,
        "size": file_size
    }


def parser(x):
    try:
        x = MetarParser(x, strict=False).asos_dict(null_sky_as=None)
    except:
        x = {}
    return x


# %%
years = list(range(2000, 2022))

station_years = product(stations, years)
all_files = (
    bag.from_sequence(station_years)
       .map(lambda station_year: get_all_files(*station_year))
       .compute()
)
cluster.scale(20)

# %%
flattened = []
for station_set in all_files:
    flattened.extend(station_set)

# %%
# status = bag.from_sequence(flattened).map(transfer_files).compute()
# flat_status = [s[0] for s in status]
s3_base = "oss-shared-scratch/paul-scratch/faa-scratch/raw"
in_out = [
    {"dat_in": f, "dat_out_uri": f"{s3_base}/{Path(f).name}"}
    for f in flattened
]

# %%
status_futures = client.map(lambda x: transfer(**x), in_out, retries=5)
statuses = client.gather(status_futures)

# file_bag = bag.from_sequence(all_files, npartitions=len(stations))
# %%
missing = list(filter(lambda s: not s["size"], statuses))
uploaded = list(filter(lambda s: s["status"] == "transferred", statuses))


# %%
parsed = (
    bag.read_text("s3://oss-shared-scratch/paul-scratch/faa-scratch/raw/*.dat")
        .map(lambda x: MetarParser(x, strict=False).asos_dict(null_sky_as=None))
        .to_dataframe()
        .map_partitions(precip_process)
)

parsed.to_parquet("s3://oss-shared-scratch/paul-scratch/faa-scratch/df03.parquet")




# %%


# %%
client.close()
cluster.close()
