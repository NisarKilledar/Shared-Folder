select distinct 
ret_order_no,
ret_line_key,
item_id,
alias_value,
ret_org_qty,
ret_unit_price,
ret_units,
draft_created, in_transit, carrier_delivered, created
from(
    select 
    distinct ret_date,
    ret_order_no,
    ret_line_key,
    item_id,
    alias_value,
    ret_org_qty,
    ret_unit_price,
    ret_units,
    max(draft_created) over (PARTITION by ret_date, ret_order_no, ret_line_key, item_id, alias_value order by ret_date) as draft_created,
    max(in_transit) over (PARTITION by ret_date, ret_order_no, ret_line_key, item_id, alias_value order by ret_date) as in_transit,
    max(carrier_delivered) over (PARTITION by ret_date, ret_order_no, ret_line_key, item_id, alias_value order by ret_date) as carrier_delivered,
    max(created) over (PARTITION by ret_date, ret_order_no, ret_line_key, item_id, alias_value order by ret_date) as created
    from(
        select 
        distinct ret_date,
        ret_order_no,
        ret_line_key,
        item_id,
        alias_value,
        ret_org_qty,
        ret_unit_price,
        ret_units,
        case when ret_status='1000' then ret_status_date else null end as Draft_created,
        case when ret_status='1000.01' then ret_status_date else null end as In_transit,
        case when ret_status='1000.02' then ret_status_date else null end as Carrier_delivered,
        case when ret_status='1100' then ret_status_date else null end as Created
        from
        (
            select distinct
                 ordhead.order_date ret_date, ordhead.order_no ret_order_no, ordhead.order_header_key, ordline.order_line_key ret_line_key,  ordline.original_ordered_qty ret_org_qty, 
                ordline.split_qty ret_split_qty, ordline.unit_price ret_unit_price, ordline.derived_from_order_line_key ret_drv_order_line_key, 
                ordrelstatus.status ret_status, os.status_name as ret_status_name, trunc(ordrelstatus.status_date, 'DD') as ret_status_date, 
                 ((ordline.original_ordered_qty - ordline.split_qty) * ordline.unit_price) ret_value, 
                (ordline.original_ordered_qty - ordline.split_qty) ret_units,
                ordline.item_id, als.alias_value
        
                from omsprd.yfs_order_header ordhead
                inner join omsprd.yfs_order_line ordline 
                    on ordhead.order_header_key = ordline.order_header_key
                inner join omsprd.yfs_order_release_status ordrelstatus 
                    on ordline.order_line_key = ordrelstatus.order_line_key
                inner join omsprd.yfs_status os on ordrelstatus.status = os.status
                left join omsprd.yfs_item yi on ordline.item_id=yi.item_id
                left join omsprd.yfs_item_alias als on yi.ITEM_KEY = als.ITEM_KEY
                where ordhead.ENTERPRISE_KEY = 'OFF5'
                  and os.process_type_key = 'RETURN_FULFILLMENT'
                  and ordhead.document_type = '0003'
                  and os.status != '9000'
                  and ordhead.order_no < '9000000000'
                  and ordhead.order_type = 'Online_Return'
                /*and ordhead.order_no in ('6000744912',
                                            '6001123534',
                                            '6000914062',
                                            '6000827962',
                                            '6001078361',
                                            '6001087139',
                                            '6000965806',
                                            '6000864914',
                                            '6000728861',
                                            '6000724025',
                                            '6001029563'
                                            )  */
        )
    )
)
where draft_created between to_date('31-jan-21') and to_date('28-feb-21')
--and draft_created is not null 
--and in_transit is not null
--and carrier_delivered is not null
--and created is not null
;


