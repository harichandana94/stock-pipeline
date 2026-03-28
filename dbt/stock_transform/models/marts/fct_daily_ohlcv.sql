with staged as (
    select * from {{ ref('stg_stock_prices') }}
)

select
    ticker,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    high_price - low_price            as daily_range,
    close_price - open_price          as daily_change,
    round(
        (close_price - open_price)
        / open_price * 100, 2
    )                                 as daily_change_pct
from staged