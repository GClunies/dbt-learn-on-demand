with payments as (

    select * from {{ ref('stg_payments') }}

),

pivoted as (

    select
        order_id,

        sum(
            case
                when payment_method = 'bank trasfer' then amount
                else 0
            end
        ) as bank_transfer_amount
        
    from payments

    where status = 'success'

    group by order_id

)

select * from pivoted