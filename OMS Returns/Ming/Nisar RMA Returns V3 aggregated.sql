select order_date, ret_date, count(distinct order_no) sales_orders, count(distinct ret_order_no) return_orders, sum(ret_units) returned_units, sum(demand) return_demand
from(
select distinct order_Date, ret_date, order_no, ret_order_no, ret_line_key, ret_units, ret_value, ret_discount, (ret_value-ret_discount) demand
from(
select distinct 
     so.order_line_key, so.order_no, trunc(so.order_date,'DD') order_date, so.original_ordered_qty, 
                so.split_qty, so.unit_price,  so.derived_from_order_line_key, so.status, so.status_name, so.ORDERED_Qty, coalesce(so.CHARGEAMOUNT,0),
                so.sales_amount, so.ordered_units,

            ordhead.order_no ret_order_no, trunc(ordhead.order_date, 'DD') ret_date, ordline.order_line_key ret_line_key,  ordline.original_ordered_qty ret_org_qty, 
            ordline.split_qty ret_split_qty, ordline.unit_price ret_unit_price, ordline.derived_from_order_line_key ret_drv_order_line_key, 
            ordrelstatus.status ret_status, os.status_name as ret_status_name,  ordline.ordered_qty ret_ord_qty,
            coalesce(linechg.chargeamount,0) ret_discount, ((ordline.original_ordered_qty - ordline.split_qty) * ordline.unit_price) ret_value, 
            (ordline.original_ordered_qty - ordline.split_qty) ret_units

from omsprd.yfs_order_header ordhead
inner join omsprd.yfs_order_line ordline 
    on ordhead.order_header_key = ordline.order_header_key
inner join omsprd.yfs_order_line ordline 
          on ordhead.order_header_key = ordline.order_header_key
inner join (
    select order_line_Key, status
    from (select a.order_line_Key, a.status, os.status_name 
            , rank() over (partition by order_line_Key order by a.status, a.ORDER_RELEASE_STATUS_KEY asc) gg
            from omsprd.yfs_order_release_status a
            inner join omsprd.yfs_status os on a.status = os.status
                    where os.process_type_key = 'RETURN_FULFILLMENT'
                    --and lower(os.status_name) like '%transit%'
                    --and lower(os.status_name) like '%carrier delivered%'
                    --and lower(os.status_name) like '%created%'
                    --and lower(os.status_name) like '%received%'
                    
                    --and a.status = '1000.01'      /*Carrier received*/
                    and a.status = '1000.02'      /*Carrier delivered*/
                    --and a.status = '1100'         /*Return in process*/
                    --and a.status = '3900'           /*DC processed*/
                    and a.status <> '1000'
        )
        where gg = 1
    ) ordrelstatus 
          on ordline.order_line_key = ordrelstatus.order_line_key

    inner join omsprd.yfs_status os on ordrelstatus.status = os.status
    left join (
        select distinct line_key,  
            sum(chargeamount) as chargeamount
        from omsprd.yfs_line_charges
        where charge_name in ('DISCOUNT')--, 'FIRSTDAY')
            and record_type = 'ORD'
        group by  line_key
        ) linechg 
        on ordline.order_line_key = linechg.line_key
    right join (
        select ordline.order_line_key, ordhead.order_no, ordhead.order_date, ordline.original_ordered_qty, ordline.unit_price, 
                ordline.split_qty, ordline.derived_from_order_line_key, ordrelstatus.status, os.status_name, ordline.ORDERED_qty, linechg.CHARGEAMOUNT,
                ((ordline.original_ordered_qty - ordline.split_qty) * ordline.unit_price) sales_amount, 
                (ordline.original_ordered_qty - ordline.split_qty) ordered_units

            from omsprd.yfs_order_header ordhead
            inner join omsprd.yfs_order_line ordline 
                on ordhead.order_header_key = ordline.order_header_key
            inner join (
                select order_line_Key, status
                from (select a.order_line_Key, a.order_release_status_key, a.status, os.status_name
                        , rank() over (partition by order_line_Key order by a.status, a.ORDER_RELEASE_STATUS_KEY asc) gg
                        from omsprd.yfs_order_release_status a
                        inner join omsprd.yfs_status os on a.status = os.status
                        where os.process_type_key = 'ORDER_FULFILLMENT'
                        )
                where gg = 1
                ) ordrelstatus 
                on ordline.order_line_key = ordrelstatus.order_line_key
                inner join omsprd.yfs_status os on ordrelstatus.status = os.status
                left join (
                        select distinct line_key,  
                            sum(chargeamount) as chargeamount
                        from omsprd.yfs_line_charges
                        where charge_name in ('DISCOUNT')--, 'FIRSTDAY')
                            and record_type = 'ORD'
                        group by  line_key
                        ) linechg 
                on ordline.order_line_key = linechg.line_key
            where ENTERPRISE_KEY = 'OFF5'
              and ordhead.document_type = '0001' 
              and ordhead.order_no < '9000000000'
              and ordhead.order_date  >= '31-jan-21'
              and ordhead.order_date <= '30-jan-22' 
    ) so on so.order_line_key = ordline.derived_from_order_line_key
    where ordhead.ENTERPRISE_KEY = 'OFF5'
          and os.process_type_key = 'RETURN_FULFILLMENT'
          and ordhead.document_type = '0003' 
          and ordhead.order_no < '9000000000'
          
        and ordhead.order_date  >= '31-jan-21'
        
--          and ordhead.order_date  >= '30-jan-22'
--          and ordhead.order_date <= '06-feb-22'

--          and ordhead.order_date  >= '06-feb-22'
--          and ordhead.order_date <= '13-feb-22'

--          and ordhead.order_date  >= '13-feb-22'
--          and ordhead.order_date <= '20-feb-22'

--          and ordhead.order_date  >= '20-feb-22'
--          and ordhead.order_date <= '27-feb-22'

--          and ordhead.order_date  >= '27-feb-22'
--          and ordhead.order_date <= '06-mar-22'

--            and ordhead.order_date  >= '06-mar-22'
--            and ordhead.order_date <= '13-mar-22'
 )      
)group by order_date, ret_date
order by order_date, ret_date;
