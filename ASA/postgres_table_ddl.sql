drop table temperatures;
create table temperatures ( 
    latitude float, 
    longitude float, 
    "time" varchar, 
    temperature_2m float
) ;


## For sql server
drop table temperatures;

create table temperatures ( 
    latitude float not null, 
    longitude float not null, 
    [time] varchar not null, 
    temperature_2m float,
    CONSTRAINT PK_temperatures PRIMARY KEY (latitude,longitude, [time])
)