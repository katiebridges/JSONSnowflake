USE "DB"."SCHEMA"

CREATE OR REPLACE STAGE boulder_weather;
CREATE OR REPLACE STAGE nola_weather;

CREATE OR REPLACE TABLE NOLA_INPUT
(JSON VARIANT);
CREATE OR REPLACE TABLE BOULDER_INPUT
(JSON VARIANT);

COPY INTO "KBRIDGES"."PUBLIC"."NOLA_INPUT"
FROM @nola_weather
FILE_FORMAT = (FORMAT_NAME = 'JSON' STRIP_OUTER_ARRAY=true);
COPY INTO "KBRIDGES"."PUBLIC"."BOULDER_INPUT"
FROM @boulder_weather
FILE_FORMAT = (FORMAT_NAME = 'JSON' STRIP_OUTER_ARRAY=true);

--CREATE OR REPLACE VIEW all_weather as 
--select * from NOLA_INPUT
--UNION ALL
--select * from BOULDER_INPUT;

select * from 
(select JSON as jsonfield from boulder_input) v,
table(flatten (input=>v.jsonfield, 
              recursive=>true))f
              where f.key is not null; 


CREATE OR REPLACE TABLE working_nola as
(select
JSON:currently:apparentTemperature:: float ApparentTemp,
JSON:currently:precipProbability:: float PrecipProb,
JSON:currently:summary:: string Summary,
JSON:daily.data[0]:sunriseTime:: float SunriseTime,
JSON:daily.data[0]:sunsetTime:: float SunsetTime
from NOLA_INPUT);
CREATE OR REPLACE TABLE working_boulder as
(select
JSON:currently:apparentTemperature:: float ApparentTemp,
JSON:currently:precipProbability:: float PrecipProb,
JSON:currently:summary:: string Summary,
JSON:daily.data[0]:sunriseTime:: float SunriseTime,
JSON:daily.data[0]:sunsetTime:: float SunsetTime
from BOULDER_INPUT);

CREATE OR REPLACE TABLE working_weather as 
select 
*,
'NewOrleans' as Location
from working_nola
union all
select 
*,
'Boulder' as Location
from working_boulder;


CREATE OR REPLACE VIEW weather_calc as select
LOCATION,
APPARENTTEMP as FARTEMP,
((APPARENTTEMP-32)*(5/9)) AS CELSIUSTEMP,
SUMMARY,
dateadd(d,-229,(dateadd(s,SUNSETTIME,'19725301'))) as SUNSET
from WORKING_WEATHER;
