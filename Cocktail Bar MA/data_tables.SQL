---Tables for glass stock by location

CREATE TABLE 
bar_ny(glass_type VARCHAR(50), 
stock VARCHAR(50),
bar VARCHAR(50));

CREATE TABLE  bar_bp(glass_type VARCHAR(50), 
stock VARCHAR(50),
bar VARCHAR(50));


CREATE TABLE  bar_ln(glass_type VARCHAR(50), 
stock VARCHAR(50),
bar VARCHAR(50));

----Tables for drinks corresponding with glass type for each location
----Transaction timestamp broken down to hours so we can measure nummber of glasses needed by hour

CREATE TABLE  ny_data(timest VARCHAR(50), 
drink VARCHAR(50), 
amount VARCHAR(50), 
category VARCHAR(50), 
glasstype VARCHAR(50), 
date1 VARCHAR(50), 
hrs VARCHAR(50));

CREATE TABLE  bp_data(timest VARCHAR(50), 
drink VARCHAR(50), 
amount VARCHAR(50), 
category VARCHAR(50), 
glasstype VARCHAR(50), 
date1 VARCHAR(50), 
hrs VARCHAR(50));

CREATE TABLE  ln_data(timest VARCHAR(50), 
drink VARCHAR(50), 
amount VARCHAR(50), 
category VARCHAR(50), 
glasstype VARCHAR(50), 
date1 VARCHAR(50), 
hrs VARCHAR(50));
