with daily as (
    select * from {{ ref('fct_daily_ohlcv') }}
),

with_averages as (
    select
        ticker,
        trade_date,
        close_price,
        round(avg(close_price) over (
            partition by ticker
            order by trade_date
            rows between 6 preceding and current row
        ), 2) as ma_7_day,
        round(avg(close_price) over (
            partition by ticker
            order by trade_date
            rows between 29 preceding and current row
        ), 2) as ma_30_day,
        volume
    from daily
)

select * from with_averages
order by ticker, trade_date