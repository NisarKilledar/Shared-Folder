select distinct order_date,
    count(distinct a.orders) as orders,
    sum(a.actual) as ordered_units
from (
select distinct to_date(ordhead.order_date) as order_date, ordhead.order_no as orders
    , sum(ordline.original_ordered_qty - ordline.split_qty) as Actual
    from omsprd.yfs_order_header ordhead
    inner join omsprd.yfs_order_line ordline 
          on ordhead.order_header_key = ordline.order_header_key
    inner join omsprd.yfs_order_release_status ordrelstatus
        on ordline.order_line_key = ordrelstatus.order_line_key
    inner join omsprd.yfs_status os on ordrelstatus.status = os.status
    left join omsprd.yfs_header_charges cg on cg.header_key = ordhead.order_header_key
    where ENTERPRISE_KEY = 'OFF5'
          and os.process_type_key = 'RETURN_FULFILLMENT'
          and ordhead.document_type = '0003' 
          and ordhead.order_no < '9000000000'
          and ordhead.ORDER_DATE  >= '13-feb-22'-- and '20220106'
          and lower(os.status_name) like '%carrier delivered%'
          --and ordhead.ORDER_DATE <= '13-feb-22'
          --and ordrelstatus.status not in ('9000', '1310')
          --and order_no in ('109120109')       
          and ordline.DERIVED_FROM_ORDER_line_KEY in 
          (
          select distinct ordline.order_line_key
                from omsprd.yfs_order_header ordhead
                inner join omsprd.yfs_order_line ordline 
                      on ordhead.order_header_key = ordline.order_header_key
                inner join omsprd.yfs_order_release_status ordrelstatus
                    on ordline.order_line_key = ordrelstatus.order_line_key
                inner join omsprd.yfs_status os on ordrelstatus.status = os.status
                left join omsprd.yfs_header_charges cg on cg.header_key = ordhead.order_header_key
                where ENTERPRISE_KEY = 'OFF5'
                      and os.process_type_key = 'ORDER_FULFILLMENT'
                      and ordhead.document_type = '0001' 
                      and ordhead.order_no < '9000000000'
                      and ordhead.ORDER_DATE  >= '13-feb-22'-- and '20220106' 
                      and ordhead.ORDER_DATE <= '20-feb-22'
                      --and ordrelstatus.status not in ('9000', '1310')
                      --and order_no in ('109120109')    
          )
    group by to_date(ordhead.order_date), ordhead.order_no
    ) a
    group by order_date
    order by order_date;
    
    
    
    
    