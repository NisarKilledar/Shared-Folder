select distinct 
oh.order_no,
oh.createts as return_create_date,  
ol.DERIVED_FROM_ORDER_HEADER_KEY, 
rs.status, os.status_name, rs.modifyts as processed_date, oh.entry_type, rs.status_quantity,
yi.item_id, 
yi.extn_department, yi.extn_department_name, inv.modifyts as invoice_publish_date, yor.scac as carrier, node.node_type,
person.city, person.state, person.country, person.short_zip_code
from omsprd.yfs_order_line ol
join omsprd.yfs_order_header oh on oh.order_header_key = ol.order_header_key
join omsprd.yfs_person_info person on person.person_info_key = oh.bill_to_key
join OMSPRD.YFS_ORDER_RELEASE_STATUS rs on oh.order_header_key = rs.order_header_key
join omsprd.yfs_status os on rs.status = os.status
join omsprd.yfs_header_charges hc on oh.order_header_key= hc.header_key
join omsprd.yfs_item yi on ol.item_id=yi.item_id
join omsprd.yfs_item_alias als on yi.ITEM_KEY = als.ITEM_KEY

join omsprd.yfs_order_invoice inv on oh.order_header_key = inv.order_header_key
join omsprd.yfs_ship_node node on node.ship_node = ol.shipnode_key
join omsprd.YFS_ORDER_RELEASE yor on yor.order_header_key = oh.order_header_key

where os.status!='9000' 
and os.process_type_key='RETURN_FULFILLMENT'
and ol.DERIVED_FROM_ORDER_HEADER_KEY between '20210601' and '20210631'
--and substr(ol.DERIVED_FROM_ORDER_HEADER_KEY,6) like '%202101%'
and hc.charge_name like '%PREPA%'
and als.ALIAS_NAME='ACTIVE_UPC'
--and rs.status_quantity > 0
and oh.document_type='0003'
--and oh.order_no in ('6000941440')
and oh.enterprise_key like '%OFF5%'
;
