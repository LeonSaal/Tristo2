

--WVG
-- number of WVG and supplied people/discharge
SELECT COUNT(wvgs.id) N_WVG, ROUND(SUM(wvg.supplied)/1e6,1) supplied_mio_EW ,round(SUM(wvg.discharge_m3_p_d)/1e6,2) discharge_mio_m3_p_d
FROM wvg 
JOIN (SELECT DISTINCT wvg.id  FROM wvg_lau 
JOIN response ON wvg_lau.LAU = response.LAU 
JOIN wvg ON wvg.id=wvg_lau.wvg) AS wvgs ON wvgs.id=wvg.id;

--WVG with data
SELECT COUNT(DISTINCT wvg_lau.wvg) N_WVG_with_data
FROM data 
INNER JOIN file_index ON file_index.hash2=data.hash2 
INNER JOIN response ON file_index.hash=response.hash 
INNER JOIN wvg_lau ON response.LAU= wvg_lau.LAU 
WHERE data.param_id NOT NULL AND data.omitted_id IS NULL;

--area of investigated lau_nuts
SELECT CAST(SUM(lau_nuts.area_m2)/1e6 AS INT) AS LAU_area_km2, 
(SELECT SUM(area_m2) FROm lau_nuts)/1e6 Germany_area_km2, CAST (SUM(lau_nuts.area_m2)/(SELECT SUM(area_m2) FROM lau_nuts)*100 AS INT) LAU_percent_area
FROM (SELECT DISTINCT LAU FROM response) resp 
JOIN lau_nuts ON resp.LAU = lau_nuts.LAU;


--LAU
-- scraped lau_nuts
SELECT COUNT(DISTINCT LAU) N_scraped_LAU
FROM response;

--LAUS with data
SELECT COUNT(DISTINCT response.LAU) N_LAU_with_data
FROM data 
INNER JOIN file_index ON file_index.hash2=data.hash2 
INNER JOIN response ON file_index.hash=response.hash 
WHERE data.param_id NOT NULL AND data.omitted_id IS NULL;

--LAU associated with two or more WVG
SELECT COUNT(*) N_LAU_multi_WVG FROM (SELECT LAU, COUNT(wvg) from wvg_lau WHERE LAU IN (SELECT DISTINCT LAU FROM response) GROUP BY wvg_lau.LAU having COUNT(wvg_lau.wvg) >1 ORDER BY COUNT(wvg) DESC);

--SEARCHES
--N searches
SELECT COUNT(id) N_searches from response;

-- omitted values
SELECT COUNT(id) N_omitted from response WHERE status=='OMITTED';

-- supplier values
SELECT COUNT(id) N_supplier from response WHERE status<>'OMITTED' AND supplier IN ('YES','CONFIRMED');

--DOWNLOADS
--total
SELECT COUNT(DISTINCT hash2) N_download FROM file_index;

--tabular
SELECT COUNT(DISTINCT hash2) N_tabular FROM file_index WHERE LOWER(ext) IN ('.xlsx','.xls');

--img
SELECT COUNT(DISTINCT hash2) N_img FROM file_index WHERE LOWER(ext) IN ('.jpeg','.jpg','.png');

--pdf
SELECT COUNT(DISTINCT hash2) N_pdf FROM file_index WHERE LOWER(ext) IN ('.pdf');

--extraction candidates
SELECT COUNT(DISTINCT hash2) N_file_candidates FROM file_index WHERE LOWER(ext) IN ('.xlsx','.xls', '.pdf');

--EXTRACTED 
--w/ date
SELECT COUNT(DISTINCT file_info.hash2) N_with_date
FROM file_cleaned 
JOIN file_info 
ON file_cleaned.hash2=file_info.hash2 
WHERE file_cleaned.status=='INSERTED' 
AND file_info.date NOT NULL;

--from 2022
SELECT COUNT(DISTINCT file_info.hash2) N_2022
FROM file_cleaned 
JOIN file_info 
ON file_cleaned.hash2=file_info.hash2 
WHERE file_cleaned.status=='INSERTED' 
AND file_info.date==2022;

--from 2021
SELECT COUNT(DISTINCT file_info.hash2) N_2021
FROM file_cleaned 
JOIN file_info 
ON file_cleaned.hash2=file_info.hash2 
WHERE file_cleaned.status=='INSERTED' 
AND file_info.date==2021;

--from 2020
SELECT COUNT(DISTINCT file_info.hash2) N_2020
FROM file_cleaned 
JOIN file_info 
ON file_cleaned.hash2=file_info.hash2 
WHERE file_cleaned.status=='INSERTED' 
AND file_info.date==2020;

--older than 2020
SELECT COUNT(DISTINCT file_info.hash2) N_older_2020
FROM file_cleaned 
JOIN file_info 
ON file_cleaned.hash2=file_info.hash2 
WHERE file_cleaned.status=='INSERTED' 
AND file_info.date<2020;

--VALUES
--number of params
SELECT COUNT(DISTINCT param_id) N_params
FROM data 
WHERE omitted_id IS NULL AND val_num NOT NULL AND unit_factor NOT NULL;

--valid values
SELECT COUNT(id) N_valid_data FROM data WHERE omitted_id IS NULL AND param_id NOT NULL AND val_num NOT NULL AND unit_factor NOT NULL;

--all values
SELECT COUNT(id) N_data_param_not_omitted FROM data WHERE omitted_id IS NULL AND param_id NOT NULL;

--ALL values
SELECT COUNT(id) N_all_data FROM data;

--blacklisted values
SELECT COUNT(id) N_blacklist_data FROM data WHERE omitted_id>0;

--Limit values
SELECT COUNT(id) N_limit_data FROM data WHERE omitted_id==-1;

--pagination values
SELECT COUNT(id) N_pagination_data FROM data WHERE omitted_id==-2;

--values without associated param
SELECT COUNT(id) N_no_param_data FROM data WHERE omitted_id IS NULL AND param_id IS NULL;

--values without numeric value
SELECT COUNT(id) N_not_numeric_data FROM data WHERE omitted_id IS NULL AND param_id IS NOT NULL AND val_num IS NULL;

--values without unit facor
SELECT COUNT(id) N_no_unit_data FROM data WHERE omitted_id IS NULL AND param_id IS NOT NULL AND val_num IS NOT NULL AND unit_factor IS NULL;

--outlier values
SELECT COUNT(id) N_outlier_data FROM data WHERE omitted_id==-3;

--candidates for valid data
SELECT COUNT(id) N_candidate_data FROM data WHERE (omitted_id IS NULL AND (val_num IS NULL OR unit_factor IS NULL OR param_id IS NULL)) OR omitted_id==-3;

--invalid values
SELECT COUNT(id) N_omitted_data FROM data WHERE omitted_id IS NOT NULL OR param_id IS NULL OR val_num IS NULL OR unit_factor IS NULL;

--TABLES
--files with data inserted 
SELECT COUNT(DISTINCT hash2) N_files_in_data FROM data WHERE param_id NOT NULL AND omitted_id IS NULL AND val_num NOT NULL AND unit_factor NOT NULL;
--files having limit column
SELECT COUNT(DISTINCT hash2) N_files_with_limit_col FROM data WHERE param_id NOT NULL AND omitted_id==-1 AND val_num NOT NULL AND unit_factor NOT NULL;
--files with value ranges
SELECT COUNT(DISTINCT hash2) N_value_range
FROM data 
WHERE hash2 IN 
(SELECT hash2 FROM data WHERE val LIKE 'mini%' OR val LIKE 'min.%' OR val LIKE 'maxi%' OR val LIKE 'max.%' OR val LIKE 'mittelw%' OR val LIKE 'median%') 
OR category=='RANGE' 
AND (omitted_id > 0 OR omitted_id IS NULL) ;
