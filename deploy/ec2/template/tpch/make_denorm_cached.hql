set mapred.reduce.tasks={{num_partitions}};
set hive.map.aggr=false;
create table denorm_cached as
select r_regionkey, l_linenumber, r_name, n_nationkey, n_name, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_returnflag, l_shipdate, l_linestatus, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_tax, o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_shippriority, o_comment, c_nationkey, c_custkey, c_name, c_mktsegment
from denorm
group by r_regionkey, l_linenumber, r_name, n_nationkey, n_name, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_returnflag, l_shipdate, l_linestatus, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_tax, o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_shippriority, o_comment, c_nationkey, c_custkey, c_name, c_mktsegment;
set hive.map.aggr=true;
