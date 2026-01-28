
import polars as pl 
import xarray as xr
import datetime as dt
import numpy as np

from pyproj import Proj
from pathlib import Path
from wztools import fetch_nearest_point


class AelmoExtractor:
    def __init__(self, aelmo_path: Path|str, cro2d_path: Path|str) -> None:
        """
        >>> aermo_extractor = AelmoExtractor(aelmo_path, cro2d_path)
        >>> # 提取某点数据
        >>> df = aermo_extractor.extract_data_by_lonlat(lon, lat)
        >>>
        >>> # 提起网格数据
        >>> df_grid = aermo_extractor.extract_data()
        """
        self.aelmo_path = Path(aelmo_path)
        self.cro2d_path = Path(cro2d_path)

    def extract_data(self) -> pl.DataFrame:
        ds = xr.open_dataset(self.aelmo_path, engine="netcdf4")

        # 加载地理信息文件
        ds_grid = xr.open_dataset(self.cro2d_path)
        # 获取经纬度 (通常是二维数组)
        lons = ds_grid['LON'].squeeze().values
        lats = ds_grid['LAT'].squeeze().values

        # 2. 从属性获取时间元数据
        sdate = int(ds.attrs['SDATE'])  # YYYYDDD
        stime = int(ds.attrs['STIME'])  # HHMMSS
        tstep = int(ds.attrs['TSTEP'])  # HHMMSS

        # 3. 解析起始时间
        start_dt = dt.datetime.strptime(str(sdate), '%Y%j')
        # 补全 6 位时间格式 (例如 51802 -> 051802)
        s_str = str(stime).zfill(6)
        start_dt += dt.timedelta(hours=int(s_str[:2]), minutes=int(s_str[2:4]), seconds=int(s_str[4:6]))

        # 4. 解析步长 (TSTEP)
        ts_str = str(tstep).zfill(6)
        tstep_delta = dt.timedelta(hours=int(ts_str[:2]), minutes=int(ts_str[2:4]), seconds=int(ts_str[4:6]))

        # 5. 生成时间序列（修复 FutureWarning）
        # 使用 ds.sizes 代替 ds.dims
        num_steps = ds.sizes['TSTEP'] 
        time_values = [start_dt + i * tstep_delta for i in range(num_steps)]
        time_values = [t + dt.timedelta(hours=8) for t in time_values]

        # 6. 重新定义坐标
        ds = ds.assign_coords(time=('TSTEP', time_values)).swap_dims({'TSTEP': 'time'})

        O3_data = ds.variables["O3"][:] * 1963
        NO2_data = ds.variables["NO2"][:] * 1881
        NO_data = ds.variables["NO"][:] * 1227
        PM10_data = ds.variables["PM10"][:]
        PM25_data = ds.variables["PM25"][:]
        CO_data = ds.variables["CO"][:] * 1.144
        SO2_data = ds.variables["SO2"][:] * 2619.6

        # lons = lons.values
        # lats = lats.values

        dfs = []
        nx = ds.sizes['COL']
        ny = ds.sizes['ROW']

        for index, t in enumerate(time_values):
            _O3_data = O3_data[index][0].values
            _PM10_data = PM10_data[index][0].values
            _PM25_data = PM25_data[index][0].values
            _NO_data = NO_data[index][0].values
            _NO2_data = NO2_data[index][0].values
            _CO_data = CO_data[index][0].values
            _SO2_data = SO2_data[index][0].values

            df_t = pl.DataFrame({
                "time": [t] * nx * ny,
                "lon": lons.flatten(), 
                "lat": lats.flatten(), 
                "O3": _O3_data.flatten(), 
                "PM10": _PM10_data.flatten(), 
                "PM25": _PM25_data.flatten(), 
                "NO2": _NO2_data.flatten(), 
                "CO": _CO_data.flatten(), 
                "NO": _NO_data.flatten(), 
                "SO2": _SO2_data.flatten(), 
            })
            dfs.append(df_t)      
        # print(O3_data.shape)
        # print(len(time_values))
        df = pl.concat(dfs)
        df = df.with_columns(
            *[pl.col(col).cast(pl.Float64).round(4) for col in df.columns if col not in ["time"]]
        )

        return df

    def extract_data_by_lonlat(self, lon, lat) -> pl.DataFrame:
        df = self.extract_data()

        nerest_lon, nearest_lat = fetch_nearest_point(df, tar_lon=lon, tar_lat=lat)
        df = df.filter(
            (pl.col("lon") == nerest_lon)
            &
            (pl.col("lat") == nearest_lat)
        ).sort("time")

        df = df.rename({"time": "datetime"})
        return df



if __name__ == "__main__":
    file_path = Path(r"C:\Users\Administrator\Desktop\tmp\CCTM_AELMO_d01_yixing2.nc")
    cro2d_path = Path(r"C:\Users\Administrator\Desktop\tmp\GRIDCRO2D_d01_yixing2.nc")

    lon = 114
    lat = 38

    aermo_extractor = AelmoExtractor(file_path, cro2d_path)

    aermo_extractor.extract_data()
    df = aermo_extractor.extract_data_by_lonlat(lon, lat)
    print(df)
