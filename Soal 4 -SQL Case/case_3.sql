/*CTE net_revenue digunakan untuk membuat perhitungan total productquantity, total refund, total revenue dan net revenue*/
with net_revenue as (
	select distinct v2productname, sum(case when productquantity is null then 0 else productquantity end) as productquantity, 
  sum((case when productquantity is null then 0 else productquantity end)*productprice) as total_refund_amount,
  sum(case when totaltransactionrevenue  is null then 0 else totaltransactionrevenue end) as total_revenue,
  sum(case when totaltransactionrevenue  is null then 0 else totaltransactionrevenue end) - sum((case when productquantity is null then 0 else productquantity end)*productprice) as net_revenue
  from "ecommerce-session-bigquery" esb
  group by v2productname
)

/*Menampilkan total productquantity, total refund, total revenue dan net revenue, serta flag yang diberikan kepada produk
yang memiliki jumlah refund lebih dari 10% total revenue
*/ 
select *,
	case when
		total_refund_amount > total_revenue/10 then 'yes'
		else 'no'
	end as flag
from net_revenue
order by net_revenue desc;

/*awalnya saya menghitung product revenue dengan mengalikan harga produk dengan quantity, tetapi saat ingin menghitung refund
saya melihat bahwa tidak ada produk yang memiliki nilai di kolom refundamount. Kemudian saya melihat total revenue untuk setiap produk
dan hampir semuanya memiliki nilai total revenue jauh lebih banyak dibanding jumlah product revenue yang saya hitung sebelumnya. Karena
tidak adanya nilai pada kolom refund amount dan jauhnya perbandingan product revenue dengan total revenue akhirnya saya berasumsi bahwa
perkalian productquantity dengan price adalah jumlah harga barang yang direfund. Net-revenue yang didapatkan dari sini menampilkan Google
tote bag sebagai barang dengan net_revenue terbesar diikuti collapsible shopping bag dan sport bag. terdapat dua produk yang mengalami kerugian
yaitu dog frisbee dan google leather profilated journal
*/