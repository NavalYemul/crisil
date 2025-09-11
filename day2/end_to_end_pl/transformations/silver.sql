--CONSTRAINT valid_current_page EXPECT (current_page_id IS NOT NULL and current_page_title IS NOT NULL) ON VIOLATION DROP ROW


create streaming table silver.sales_cleaned_pl
(CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW)
as 
select distinct * from stream sales_pl ;


-- products scd 1
-- https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-apply-changes-into
-- Create a streaming table, then use AUTO CDC to populate it:

CREATE OR REFRESH STREAMING TABLE silver.products_silver_pl;
CREATE FLOW product_flow
AS AUTO CDC INTO
  silver.products_silver_pl
FROM stream(bronze.products_pl)
  KEYS (product_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY seqNum
  COLUMNS * EXCEPT (operation, seqNum,_rescued_data,ingestion_date)
  STORED AS SCD TYPE 1;
  --TRACK HISTORY ON * EXCEPT (city)



-- customers scd 2 

CREATE OR REFRESH STREAMING TABLE silver.customers_silver_pl;
CREATE FLOW customers_flow
AS AUTO CDC INTO
  silver.customers_silver_pl
FROM stream(bronze.customers_pl)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation, sequenceNum,_rescued_data,ingestion_date)
  STORED AS SCD TYPE 2;



