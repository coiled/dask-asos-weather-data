from ftplib import FTP, error_perm, error_temp
from pathlib import Path
from itertools import product

import pandas
from tqdm import tqdm
from cloudside.asos import MetarParser, _find_reset_time, _process_precip


FIVEMIN = pandas.offsets.Minute(5)
EMAIL = "paul+faa@coiled.io"


def get_all_files(station, year):
    with FTP("ftp.ncei.noaa.gov") as ftp:
        ftp.login(passwd=EMAIL)
        try:
            dat_files = ftp.nlst(f"/pub/data/asos-fivemin/6401-{year}/*{station}*")
        except error_perm as e:
            print(e)
    return dat_files


def transfer_locally(dat_in, dat_out):
    p_in = Path(dat_in)
    p_out = Path(dat_out)
    if not p_out.exists() or (p_out.stat().st_size == 0):
        try:
            with FTP("ftp.ncei.noaa.gov") as ftp, p_out.open("w") as outfile:
                ftp.login(passwd=EMAIL)
                ftp.retrlines(
                    f"RETR {dat_in}",
                    lambda x: outfile.write(x + "\n")
                )
            file_size = p_out.stat().st_size
            status = "transferred"

        except Exception as e:
            file_size = None
            status = str(e)
    else:
        file_size = p_out.stat().st_size
        status = "exists already"

    return {
        "source": p_in,
        "file": p_out,
        "status": status,
        "size": file_size
    }


def parse_local(dat_local):
    p = Path(dat_local)
    with p.open("r") as datafile:
        data = [
            MetarParser(line, strict=False).asos_dict(null_sky_as=None)
            for line in datafile
        ]

    df = (
        pandas.DataFrame(data)
            .groupby("datetime").last()
            .sort_index()
            .resample(FIVEMIN).asfreq()
    )
    rt = _find_reset_time(df["raw_precipitation"])
    precip = _process_precip(df, rt, "raw_precipitation")
    return data.assign(precip=precip)


if __name__ == "__main__":
    stations = ["KATL", "KPDX"]
    years = list(range(2020, 2022))

    files = []
    pbar = tqdm(list(product(stations, years)))
    for station, year in pbar:
        pbar.set_description(f"finding {year} files for {station}: ")
        files.extend(get_all_files(station, year))

    status = []
    pbar = tqdm(files)
    for dat_in in pbar:
        pbar.set_description(f"transferring {dat_in}")
        name = Path(dat_in).name
        dat_out = Path("data", "serial", name)
        status.append(transfer_locally(dat_in, dat_out))

    pbar = tqdm(status)
    list_of_dataframes = []
    for s in pbar:
        if s["size"]:
            pbar.set_description(f"parsing {s['file']}")
            list_of_dataframes.append(parse_local(s["file"]))

    data = pandas.concat(list_of_dataframes, ignore_index=True)
