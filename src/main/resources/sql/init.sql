create table VISITORS_STAT
(country char(2), 
url varchar(40), 
visits int);


create unique index unique_visitor on VISITORS_STAT (country, url)

;