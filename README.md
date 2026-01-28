# CAMQ Tools

一个用于从 AELMO NetCDF 文件中提取和处理空气质量模型数据的 Python 库。

## 安装

```bash
pip install camq-tools
```

## 使用方法

```python
from camq_tools import AelmoExtractor
from pathlib import Path

# 初始化提取器
extractor = AelmoExtractor(
    aelmo_path=Path("path/to/CCTM_AELMO_d01.nc"),
    cro2d_path=Path("path/to/GRIDCRO2D_d01.nc")
)

# 提取所有数据
df = extractor.extract_data()
print(df)

# 根据经纬度提取特定位置的数据
df_point = extractor.extract_data_by_lonlat(lon=114.0, lat=38.0)
print(df_point)
```

## 支持的污染物

- O3 (臭氧)
- PM2.5 (细颗粒物)
- PM10 (可吸入颗粒物)
- NO2 (二氧化氮)
- CO (一氧化碳)
- NO (一氧化氮)
- SO2 (二氧化硫)

## 依赖

- polars
- xarray
- netcdf4
- pyproj
- wztools
