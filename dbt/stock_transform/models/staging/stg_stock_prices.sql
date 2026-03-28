with source as (
    select * from raw.stock_prices
),

cleaned as (
    select
        ticker,
        trade_date,
        open::numeric(10,2)   as open_price,
        high::numeric(10,2)   as high_price,
        low::numeric(10,2)    as low_price,
        close::numeric(10,2)  as close_price,
        volume,
        loaded_at
    from source
    where close is not null
      and close > 0
)

select * from cleaned