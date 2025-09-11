-- create streaming table or materialised view (ST or MV)


create streaming table sales_pl as 
select *, current_date() as ingestion_date from stream read_files("/Volumes/dev/default/input_files/sales/",format=>"csv");

create streaming table customers_pl as 
select *, current_date() as ingestion_date from stream read_files("/Volumes/dev/default/input_files/customers/",format=>"csv");

create streaming table products_pl as 
select *, current_date() as ingestion_date from stream read_files("/Volumes/dev/default/input_files/products/",format=>"csv");