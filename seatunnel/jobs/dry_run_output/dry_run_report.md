# SeaTunnel Template Dry-Run Report
Generated: 38 templates processed

## Summary
- Successful renders: 29
- Failed renders: 9
- Templates changed: 0
- Valid configurations: 0
- Configurations with validation issues: 29

## Failed Templates
- **cpi_series_to_kafka.conf**: Failed to render cpi_series_to_kafka.conf.tmpl: Template for job 'cpi_series_to_kafka.conf' references variables that were not provided: FRED_API_KEY. Export them or provide defaults.
- **eia_fuel_curve_to_kafka.conf**: Failed to render eia_fuel_curve_to_kafka.conf.tmpl: Template for job 'eia_fuel_curve_to_kafka.conf' references variables that were not provided: EIA_API_KEY. Export them or provide defaults.
- **eia_series_to_kafka.conf**: Failed to render eia_series_to_kafka.conf.tmpl: Template for job 'eia_series_to_kafka.conf' references variables that were not provided: EIA_API_KEY. Export them or provide defaults.
- **fred_series_to_kafka.conf**: Failed to render fred_series_to_kafka.conf.tmpl: Template for job 'fred_series_to_kafka.conf' references variables that were not provided: FRED_API_KEY. Export them or provide defaults.
- **noaa_ghcnd_to_kafka.conf**: Failed to render noaa_ghcnd_to_kafka.conf.tmpl: Template for job 'noaa_ghcnd_to_kafka.conf' references variables that were not provided: NOAA_GHCND_TOKEN. Export them or provide defaults.
- **pjm_genmix_to_kafka.conf**: Failed to render pjm_genmix_to_kafka.conf.tmpl: Template for job 'pjm_genmix_to_kafka.conf' references variables that were not provided: PJM_API_KEY. Export them or provide defaults.
- **pjm_lmp_to_kafka.conf**: Failed to render pjm_lmp_to_kafka.conf.tmpl: Template for job 'pjm_lmp_to_kafka.conf' references variables that were not provided: PJM_API_KEY. Export them or provide defaults.
- **pjm_load_to_kafka.conf**: Failed to render pjm_load_to_kafka.conf.tmpl: Template for job 'pjm_load_to_kafka.conf' references variables that were not provided: PJM_API_KEY. Export them or provide defaults.
- **pjm_pnodes_to_kafka.conf**: Failed to render pjm_pnodes_to_kafka.conf.tmpl: Template for job 'pjm_pnodes_to_kafka.conf' references variables that were not provided: PJM_API_KEY. Export them or provide defaults.

## Template Details
### aeso_genmix_to_kafka.conf ✅
- Placeholders: 20
- Rendered lines: 62
- Content length: 1620
- Valid: ❌
- Validation issues: 7
  - Line 19: Closing brace without matching opening block
  - Line 23: Closing brace without matching opening block
  - Line 26: Closing brace without matching opening block
  - ... and 4 more

### aeso_lmp_to_kafka.conf ✅
- Placeholders: 9
- Rendered lines: 82
- Content length: 2845
- Valid: ❌
- Validation issues: 8
  - Line 18: Closing brace without matching opening block
  - Line 24: Closing brace without matching opening block
  - Line 26: Closing brace without matching opening block
  - ... and 5 more

### aeso_load_to_kafka.conf ✅
- Placeholders: 21
- Rendered lines: 63
- Content length: 1788
- Valid: ❌
- Validation issues: 7
  - Line 19: Closing brace without matching opening block
  - Line 23: Closing brace without matching opening block
  - Line 26: Closing brace without matching opening block
  - ... and 4 more

### caiso_genmix_to_kafka.conf ✅
- Placeholders: 20
- Rendered lines: 62
- Content length: 1623
- Valid: ❌
- Validation issues: 7
  - Line 19: Closing brace without matching opening block
  - Line 23: Closing brace without matching opening block
  - Line 26: Closing brace without matching opening block
  - ... and 4 more

### caiso_lmp_to_kafka.conf ✅
- Placeholders: 9
- Rendered lines: 144
- Content length: 4838
- Valid: ❌
- Validation issues: 13
  - Line 48: Closing brace without matching opening block
  - Line 51: Closing brace without matching opening block
  - Line 58: Closing brace without matching opening block
  - ... and 10 more

### caiso_load_to_kafka.conf ✅
- Placeholders: 21
- Rendered lines: 63
- Content length: 1791
- Valid: ❌
- Validation issues: 7
  - Line 19: Closing brace without matching opening block
  - Line 23: Closing brace without matching opening block
  - Line 26: Closing brace without matching opening block
  - ... and 4 more

### cpi_series_kafka_to_timescale.conf ✅
- Placeholders: 8
- Rendered lines: 53
- Content length: 1124
- Valid: ❌
- Validation issues: 5
  - Line 16: Closing brace without matching opening block
  - Line 17: Closing brace without matching opening block
  - Line 38: Closing brace without matching opening block
  - ... and 2 more

### cpi_series_to_kafka.conf ❌
- Error: Failed to render cpi_series_to_kafka.conf.tmpl: Template for job 'cpi_series_to_kafka.conf' references variables that were not provided: FRED_API_KEY. Export them or provide defaults.

### drought_index_kafka_to_iceberg.conf ✅
- Placeholders: 6
- Rendered lines: 80
- Content length: 2381
- Valid: ❌
- Validation issues: 5
  - Line 34: Closing brace without matching opening block
  - Line 35: Closing brace without matching opening block
  - Line 63: Closing brace without matching opening block
  - ... and 2 more

### drought_index_kafka_to_timescale.conf ✅
- Placeholders: 5
- Rendered lines: 67
- Content length: 1730
- Valid: ❌
- Validation issues: 5
  - Line 26: Closing brace without matching opening block
  - Line 27: Closing brace without matching opening block
  - Line 52: Closing brace without matching opening block
  - ... and 2 more

### drought_usdm_kafka_to_iceberg.conf ✅
- Placeholders: 6
- Rendered lines: 71
- Content length: 1815
- Valid: ❌
- Validation issues: 5
  - Line 27: Closing brace without matching opening block
  - Line 28: Closing brace without matching opening block
  - Line 54: Closing brace without matching opening block
  - ... and 2 more

### drought_vector_http_to_kafka.conf ✅
- Placeholders: 4
- Rendered lines: 77
- Content length: 2041
- Valid: ❌
- Validation issues: 6
  - Line 36: Closing brace without matching opening block
  - Line 38: Closing brace without matching opening block
  - Line 39: Closing brace without matching opening block
  - ... and 3 more

### drought_vector_kafka_to_iceberg.conf ✅
- Placeholders: 6
- Rendered lines: 72
- Content length: 1921
- Valid: ❌
- Validation issues: 5
  - Line 26: Closing brace without matching opening block
  - Line 27: Closing brace without matching opening block
  - Line 55: Closing brace without matching opening block
  - ... and 2 more

### eia_bulk_to_kafka.conf ✅
- Placeholders: 23
- Rendered lines: 134
- Content length: 6601
- Valid: ❌
- Validation issues: 9
  - Line 47: Closing brace without matching opening block
  - Line 48: Closing brace without matching opening block
  - Line 50: Closing brace without matching opening block
  - ... and 6 more

### eia_fuel_curve_to_kafka.conf ❌
- Error: Failed to render eia_fuel_curve_to_kafka.conf.tmpl: Template for job 'eia_fuel_curve_to_kafka.conf' references variables that were not provided: EIA_API_KEY. Export them or provide defaults.

### eia_series_kafka_to_timescale.conf ✅
- Placeholders: 8
- Rendered lines: 76
- Content length: 2168
- Valid: ❌
- Validation issues: 5
  - Line 30: Closing brace without matching opening block
  - Line 31: Closing brace without matching opening block
  - Line 62: Closing brace without matching opening block
  - ... and 2 more

### eia_series_to_kafka.conf ❌
- Error: Failed to render eia_series_to_kafka.conf.tmpl: Template for job 'eia_series_to_kafka.conf' references variables that were not provided: EIA_API_KEY. Export them or provide defaults.

### ercot_lmp_to_kafka.conf ✅
- Placeholders: 9
- Rendered lines: 138
- Content length: 4570
- Valid: ❌
- Validation issues: 13
  - Line 46: Closing brace without matching opening block
  - Line 49: Closing brace without matching opening block
  - Line 56: Closing brace without matching opening block
  - ... and 10 more

### fred_series_kafka_to_timescale.conf ✅
- Placeholders: 8
- Rendered lines: 66
- Content length: 1784
- Valid: ❌
- Validation issues: 5
  - Line 28: Closing brace without matching opening block
  - Line 29: Closing brace without matching opening block
  - Line 51: Closing brace without matching opening block
  - ... and 2 more

### fred_series_to_kafka.conf ❌
- Error: Failed to render fred_series_to_kafka.conf.tmpl: Template for job 'fred_series_to_kafka.conf' references variables that were not provided: FRED_API_KEY. Export them or provide defaults.

### iso_lmp_kafka_to_timescale.conf ✅
- Placeholders: 8
- Rendered lines: 86
- Content length: 2317
- Valid: ❌
- Validation issues: 4
  - Line 30: Closing brace without matching opening block
  - Line 31: Closing brace without matching opening block
  - Line 72: Closing brace without matching opening block
  - ... and 1 more

### iso_load_kafka_to_timescale.conf ✅
- Placeholders: 9
- Rendered lines: 74
- Content length: 2106
- Valid: ❌
- Validation issues: 5
  - Line 31: Closing brace without matching opening block
  - Line 32: Closing brace without matching opening block
  - Line 60: Closing brace without matching opening block
  - ... and 2 more

### isone_lmp_to_kafka.conf ✅
- Placeholders: 17
- Rendered lines: 171
- Content length: 5970
- Valid: ❌
- Validation issues: 14
  - Line 42: Closing brace without matching opening block
  - Line 46: Closing brace without matching opening block
  - Line 52: Closing brace without matching opening block
  - ... and 11 more

### miso_asm_to_kafka.conf ✅
- Placeholders: 12
- Rendered lines: 82
- Content length: 2437
- Valid: ❌
- Validation issues: 8
  - Line 18: Closing brace without matching opening block
  - Line 26: Closing brace without matching opening block
  - Line 29: Closing brace without matching opening block
  - ... and 5 more

### miso_genmix_to_kafka.conf ✅
- Placeholders: 20
- Rendered lines: 62
- Content length: 1616
- Valid: ❌
- Validation issues: 7
  - Line 19: Closing brace without matching opening block
  - Line 23: Closing brace without matching opening block
  - Line 26: Closing brace without matching opening block
  - ... and 4 more

### miso_lmp_to_kafka.conf ✅
- Placeholders: 18
- Rendered lines: 148
- Content length: 5391
- Valid: ❌
- Validation issues: 12
  - Line 46: Closing brace without matching opening block
  - Line 48: Closing brace without matching opening block
  - Line 55: Closing brace without matching opening block
  - ... and 9 more

### miso_load_to_kafka.conf ✅
- Placeholders: 21
- Rendered lines: 63
- Content length: 1784
- Valid: ❌
- Validation issues: 7
  - Line 19: Closing brace without matching opening block
  - Line 23: Closing brace without matching opening block
  - Line 26: Closing brace without matching opening block
  - ... and 4 more

### miso_rtd_lmp_to_kafka.conf ✅
- Placeholders: 31
- Rendered lines: 146
- Content length: 4507
- Valid: ❌
- Validation issues: 15
  - Line 24: Closing brace without matching opening block
  - Line 26: Closing brace without matching opening block
  - Line 37: Closing brace without matching opening block
  - ... and 12 more

### noaa_ghcnd_to_kafka.conf ❌
- Error: Failed to render noaa_ghcnd_to_kafka.conf.tmpl: Template for job 'noaa_ghcnd_to_kafka.conf' references variables that were not provided: NOAA_GHCND_TOKEN. Export them or provide defaults.

### noaa_weather_kafka_to_timescale.conf ✅
- Placeholders: 8
- Rendered lines: 73
- Content length: 2087
- Valid: ❌
- Validation issues: 5
  - Line 30: Closing brace without matching opening block
  - Line 31: Closing brace without matching opening block
  - Line 59: Closing brace without matching opening block
  - ... and 2 more

### nyiso_lmp_to_kafka.conf ✅
- Placeholders: 7
- Rendered lines: 119
- Content length: 4298
- Valid: ❌
- Validation issues: 11
  - Line 33: Closing brace without matching opening block
  - Line 40: Closing brace without matching opening block
  - Line 50: Closing brace without matching opening block
  - ... and 8 more

### pjm_genmix_to_kafka.conf ❌
- Error: Failed to render pjm_genmix_to_kafka.conf.tmpl: Template for job 'pjm_genmix_to_kafka.conf' references variables that were not provided: PJM_API_KEY. Export them or provide defaults.

### pjm_lmp_to_kafka.conf ❌
- Error: Failed to render pjm_lmp_to_kafka.conf.tmpl: Template for job 'pjm_lmp_to_kafka.conf' references variables that were not provided: PJM_API_KEY. Export them or provide defaults.

### pjm_load_to_kafka.conf ❌
- Error: Failed to render pjm_load_to_kafka.conf.tmpl: Template for job 'pjm_load_to_kafka.conf' references variables that were not provided: PJM_API_KEY. Export them or provide defaults.

### pjm_pnodes_to_kafka.conf ❌
- Error: Failed to render pjm_pnodes_to_kafka.conf.tmpl: Template for job 'pjm_pnodes_to_kafka.conf' references variables that were not provided: PJM_API_KEY. Export them or provide defaults.

### spp_genmix_to_kafka.conf ✅
- Placeholders: 20
- Rendered lines: 62
- Content length: 1609
- Valid: ❌
- Validation issues: 7
  - Line 19: Closing brace without matching opening block
  - Line 23: Closing brace without matching opening block
  - Line 26: Closing brace without matching opening block
  - ... and 4 more

### spp_lmp_to_kafka.conf ✅
- Placeholders: 9
- Rendered lines: 138
- Content length: 4545
- Valid: ❌
- Validation issues: 13
  - Line 46: Closing brace without matching opening block
  - Line 49: Closing brace without matching opening block
  - Line 56: Closing brace without matching opening block
  - ... and 10 more

### spp_load_to_kafka.conf ✅
- Placeholders: 21
- Rendered lines: 63
- Content length: 1777
- Valid: ❌
- Validation issues: 7
  - Line 19: Closing brace without matching opening block
  - Line 23: Closing brace without matching opening block
  - Line 26: Closing brace without matching opening block
  - ... and 4 more
