-- Table S1: Water supply areas with associated LAU, supplied population and daily discharge
SELECT name 'Water supply area', 
wvgID 'ID', 
GROUP_CONCAT(LAU, ', '), 
supplied 'Supplied population', 
discharge_m3_p_d 'Discharge in m³/d'
FROM wvg 
JOIN wvg_lau ON wvg.id==wvg_lau.wvg 
GROUP BY wvg.id
ORDER BY supplied DESC;


-- Table S2: Filename blacklist (case insensitive regular expressions)
SELECT DISTINCT regex FROM regex WHERE category=='BLACKLIST_FNAME';

--Table S3: Parameter names used with alias and regular expressions
SELECT DISTINCT param Parameter, 

CASE
WHEN alias LIKE '$%' THEN NULL
ELSE alias
END Alias,

CASE
WHEN regex =='None' THEN NULL
WHEN regex LIKE '$%' THEN NULL
ELSE regex
END 'Regular expression'
FROM param 

ORDER BY param.param;

--Table S4: Currentness of data
SELECT date 'Year',
    COUNT(date) 'Counts', 
    ROUND(100*COUNT(date)/CAST((SELECT COUNT(date) FROM file_info 
    JOIN file_cleaned ON file_info.hash2=file_cleaned.hash2 
    WHERE file_cleaned.status=='INSERTED' AND file_info.date NOT NULL) as FLOAT), 2) 'Fraction [%]'
FROM file_info 
JOIN file_cleaned ON file_info.hash2=file_cleaned.hash2 
WHERE file_cleaned.status=='INSERTED' AND file_info.date NOT NULL
GROUP BY date 
ORDER BY date DESC;

--Table S5:	Reported values per parameter with associated limit or guidance value
SELECT param.param 'Parameter', 
    COUNT(data.param_id) 'Value count', 
    COUNT(DISTINCT data.hash2) 'Report count',
    ROUND(100*COUNT(DISTINCT data.hash2)/(SELECT CAST(COUNT(DISTINCT hash2) AS FLOAT) FROM data WHERE data.omitted_id IS NULL AND data.val_num NOT NULL AND data.unit_factor NOT NULL), 2) 'Fraction of reports [%]', 
    COUNT(
    CASE data.category
    WHEN '> BG' THEN 1
    END  
    ) 'Count > LOQ' , 
    COUNT(
    CASE data.category
    WHEN 'BG' THEN 1
    END  
    ) 'Count LOQ', 
    ROUND(AVG(
    CASE
    WHEN data.category=='> BG' AND val_num*unit_factor <=param.'limit' THEN  val_num*unit_factor
    END
    ),2) 'Mean value',
    IIF(CAST(param.'limit' AS FLOAT)<>0, param.'limit',NULL) 'Limit',
    param.unit 'Unit',
    CASE 
    WHEN param.origin LIKE 'Trinkwv%' THEN 'German Federal Ministry of Health, 2001'
    WHEN param.origin == 'Richtlinie 2013/39/EU' THEN 'European Council, 2013'
    WHEN param.origin == 'zugelassene PSM nach Kulturen' THEN 'Federal Office of Consumer Protection and Food Safety, 2023'
    WHEN param.origin == 'Richtlinie (EU) 2020/2184' THEN 'European Parliament and Council, 2021'
    WHEN param.origin == 'GOW' THEN 'German Environment Agency, 2020'
    END Origin 
FROM data 
JOIN param ON data.param_id=param.id 
WHERE data.omitted_id IS NULL AND data.val_num NOT NULL AND data.unit_factor NOT NULL
GROUP BY data.param_id 
ORDER BY COUNT(data.param_id) DESC;

--Table S6: 
SELECT CASE SUBSTRING(PRINTF('%08d', lau_nuts.LAU),0,3)
WHEN '01' THEN 'Schleswig-Holstein'
WHEN '02' THEN 'Hamburg'
WHEN '03' THEN 'Lower-Saxony'
WHEN '04' THEN 'Bremen'
WHEN '05' THEN 'North Rhine-Westphalia'
WHEN '06' THEN 'Hesse'
WHEN '07' THEN 'Rhineland-Palatinate'
WHEN '08' THEN 'Baden-Württemberg'
WHEN '09' THEN 'Bavaria'
WHEN '10' THEN 'Saarland'
WHEN '11' THEN 'Berlin'
WHEN '12' THEN 'Brandenburg'
WHEN '13' THEN 'Mecklenburg-West Pomerania'
WHEN '14' THEN 'Saxony'
WHEN '15' THEN 'Saxony-Anhalt'
WHEN '16' THEN 'Thuringia'
END State, 
ROUND(SUM(lau_nuts.area_m2)/1e6, 0) AS 'Area in km²', 
ROUND(SUM(lau_nuts.population)/1e6,2) AS 'Supplied population in Mio.'
FROM (SELECT DISTINCT LAU FROM response) resp 
JOIN lau_nuts 
ON resp.LAU = lau_nuts.LAU 
GROUP BY State
ORDER BY SUM(lau_nuts.area_m2) DESC;

--Table S7: 
SELECT CASE SUBSTRING(PRINTF('%08d', lau_nuts.LAU),0,3)
WHEN '01' THEN 'Schleswig-Holstein'
WHEN '02' THEN 'Hamburg'
WHEN '03' THEN 'Lower-Saxony'
WHEN '04' THEN 'Bremen'
WHEN '05' THEN 'North Rhine-Westphalia'
WHEN '06' THEN 'Hesse'
WHEN '07' THEN 'Rhineland-Palatinate'
WHEN '08' THEN 'Baden-Württemberg'
WHEN '09' THEN 'Bavaria'
WHEN '10' THEN 'Saarland'
WHEN '11' THEN 'Berlin'
WHEN '12' THEN 'Brandenburg'
WHEN '13' THEN 'Mecklenburg-West Pomerania'
WHEN '14' THEN 'Saxony'
WHEN '15' THEN 'Saxony-Anhalt'
WHEN '16' THEN 'Thuringia'
END State, 
ROUND(SUM(lau_nuts.area_m2)/1e6, 0) AS 'Area in km²', 
ROUND(SUM(lau_nuts.population)/1e6,2) AS 'Population in Mio.'
FROM lau_nuts 
GROUP BY State
ORDER BY SUM(lau_nuts.area_m2) DESC;
