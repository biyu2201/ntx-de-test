/*Select country name and it's toal transaction revenue*/
select country, sum(coalesce(totaltransactionrevenue, 0)) as total,
    /*Select each channel grouping and the revenue it generated, used coalesce function to try to replace null with 0*/
	sum(coalesce(totaltransactionrevenue, 0)) filter (where channelgrouping = 'Referral') as Referral,
	sum(coalesce(totaltransactionrevenue, 0)) filter (where channelgrouping = 'Organic Search') as Organic,
	sum(coalesce(totaltransactionrevenue, 0)) filter (where channelgrouping = 'Display') as Display,
	sum(coalesce(totaltransactionrevenue, 0)) filter (where channelgrouping = 'Paid Search') as Paid,
	sum(coalesce(totaltransactionrevenue, 0)) filter (where channelgrouping = 'Affiliates') as Affiliates,
	sum(coalesce(totaltransactionrevenue, 0)) filter (where channelgrouping = 'Social') as Social,
	sum(coalesce(totaltransactionrevenue, 0)) filter (where channelgrouping = 'Direct') as Direct,
	sum(coalesce(totaltransactionrevenue, 0)) filter (where channelgrouping = '(Other)') as Other
from "ecommerce-session-bigquery" esb
/*Group by country and order by total transaction and limit top 5 countries with the most revenue*/
group by country  
order by total desc
limit 5;

/*Query yang dihasilkan menampilkan US sebagai negara yang menghasilkan revenue terbesar, 10 kali lebih besar dari Venezuela
pada peringkat kedua. Channel group Referral dan Organic menjadi dua channel dengan revenue terbanyak di US, Revenue yang
dihasilkan oleh channel organic di Venezuela mencapai 90% dari total revenue di Venezuela.*/

/*Banyak nilai NULL pada data, walaupun sudah menggunakan fungsi coalesce untuk menggantikan NULL menjadi 0, tetapi pada output
nilai NULL tersebut sayangnya masih muncul*/