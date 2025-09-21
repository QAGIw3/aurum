from __future__ import annotations

"""Raster zonal statistics utilities."""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional

import logging

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.mask import mask as raster_mask
from shapely.geometry import mapping

logger = logging.getLogger(__name__)


@dataclass
class ZonalStatsConfig:
    resampling: Resampling = Resampling.bilinear
    nodata_threshold: float = 0.6
    minimum_coverage: float = 0.2


class ZonalStatisticsEngine:
    """Computes area-weighted statistics for raster assets over reference geographies."""

    def __init__(self, config: Optional[ZonalStatsConfig] = None) -> None:
        self.config = config or ZonalStatsConfig()

    def compute(
        self,
        raster_path: Path,
        geographies: gpd.GeoDataFrame,
        value_scale: float = 1.0,
    ) -> List[Dict[str, object]]:
        """Compute weighted means for each geometry in the dataframe.

        Parameters
        ----------
        raster_path:
            Path to the GeoTIFF raster to evaluate.
        geographies:
            GeoDataFrame with `region_type`, `region_id`, and geometry columns.
        value_scale:
            Optional scale factor to apply to raster values (e.g., convert mm to inches).
        """

        if geographies.crs is None:
            raise ValueError("Geographies GeoDataFrame must declare a CRS")

        with rasterio.open(raster_path) as dataset:
            raster_crs = dataset.crs
            if raster_crs is None:
                raise ValueError(f"Raster {raster_path} missing CRS metadata")

            logger.debug("Computing zonal stats", extra={"raster": str(raster_path), "crs": raster_crs})
            geometries = geographies.to_crs(raster_crs)
            nodata = dataset.nodata

            results: List[Dict[str, object]] = []
            for _, row in geometries.iterrows():
                geom = mapping(row.geometry)
                data, transform = raster_mask(dataset, [geom], crop=True, nodata=nodata, all_touched=True)
                band = (data[0] if data.ndim == 3 else data).astype('float64') * value_scale

                if nodata is not None:
                    valid_mask = ~np.isclose(band, nodata)
                else:
                    valid_mask = ~np.isnan(band)

                total_pixels = band.size
                valid_pixels = int(np.count_nonzero(valid_mask))
                if total_pixels == 0 or (valid_pixels / total_pixels) < self.config.minimum_coverage:
                    logger.debug(
                        "Skipping geometry due to insufficient coverage",
                        extra={"region_id": row.get("region_id"), "coverage": valid_pixels / max(total_pixels, 1)},
                    )
                    continue

                values = band[valid_mask]
                if values.size == 0:
                    continue

                area_weights = _pixel_area_weights(valid_mask, transform)
                weighted_value = float(np.average(values, weights=area_weights))

                results.append(
                    {
                        "region_type": row.get("region_type"),
                        "region_id": row.get("region_id"),
                        "value": weighted_value,
                        "valid_fraction": valid_pixels / total_pixels,
                    }
                )

        return results


def _pixel_area_weights(valid_mask: np.ndarray, transform) -> np.ndarray:
    """Compute proportional area weighting for valid raster pixels."""

    pixel_area = abs(transform.a * transform.e)
    weights = np.full(valid_mask.shape, pixel_area, dtype="float64")
    return weights[valid_mask.ravel()].ravel()


__all__ = ["ZonalStatisticsEngine", "ZonalStatsConfig"]
