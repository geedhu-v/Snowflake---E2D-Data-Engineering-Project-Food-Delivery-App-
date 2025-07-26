use role sysadmin;
use warehouse adhoc_wh;
use database sp_pipeline_db;
use schema consumption_sch;


CREATE OR REPLACE TABLE consumption_sch.order_item_fact (
    order_item_fact_sk NUMBER AUTOINCREMENT comment 'Surrogate Key (EDW)', -- Surrogate key for the fact table
    order_item_id NUMBER  comment 'Order Item FK (Source System)',                    -- Natural key from the source data
    order_id NUMBER  comment 'Order FK (Source System)',                         -- Reference to the order dimension
    customer_dim_key NUMBER  comment 'Order FK (Source System)',                      -- Reference to the customer dimension
    customer_address_dim_key NUMBER,                      -- Reference to the customer dimension
    restaurant_dim_key NUMBER,                    -- Reference to the restaurant dimension
    restaurant_location_dim_key NUMBER,                    -- Reference to the restaurant dimension
    menu_dim_key NUMBER,                          -- Reference to the menu dimension
    delivery_agent_dim_key NUMBER,                -- Reference to the delivery agent dimension
    order_date_dim_key NUMBER,                         -- Reference to the date dimension
    quantity NUMBER,                          -- Measure
    price NUMBER(10, 2),                            -- Measure
    subtotal NUMBER(10, 2),                         -- Measure
    delivery_status VARCHAR,                        -- Delivery information
    estimated_time VARCHAR                          -- Delivery information
)
comment = 'The item order fact table that has item level price, quantity and other details';

-- dim and fact table constraints 
alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_customer_dim
    foreign key (customer_dim_key)
    references consumption_sch.customer_dim (customer_hk);

alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_customer_address_dim
    foreign key (customer_address_dim_key)
    references consumption_sch.customer_address_dim (CUSTOMER_ADDRESS_HK);

alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_restaurant_dim
    foreign key (restaurant_dim_key)
    references consumption_sch.restaurant_dim (restaurant_hk);

alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_restaurant_location_dim
    foreign key (restaurant_location_dim_key)
    references consumption_sch.restaurant_location_dim (restaurant_location_hk);

alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_menu_dim
    foreign key (menu_dim_key)
    references consumption_sch.menu_dim (menu_dim_hk);

alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_delivery_agent_dim
    foreign key (delivery_agent_dim_key)
    references consumption_sch.delivery_agent_dim (delivery_agent_hk);

alter table consumption_sch.order_item_fact
    add constraint fk_order_item_fact_delivery_date_dim
    foreign key (order_date_dim_key)
    references consumption_sch.date_dim (date_dim_hk);
    