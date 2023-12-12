/*cte average digunakan untuk mengetahui rata-rata time_on_site dan page_views dari semua user.
  cte user_beahviour menampilkan rata-rata time_on_site dan page_views dari masing-masing user dan menandai 
  user yang memiliki time_on_site lebih banyak dari rata-rata tetapi page_views yang lebih rendah dari rata-rata
*/
with average (total_avg_timeonsite, total_avg_pageviews) as(
	select round(avg(coalesce(timeonsite, 0)), 2) as total_avg_timeonsite,
		round(avg(coalesce(pageviews, 0)), 2) as avg_pageviews
	from "ecommerce-session-bigquery" esb 
),
user_behaviour (fullvisitorid, avg_timeonsite, avg_pageviews, avg_sessionqualitydim, behaviour) as (
 SELECT distinct fullvisitorid, 
	round(avg(coalesce(timeonsite, 0)), 2) as avg_timeonsite,
	round(avg(coalesce(pageviews, 0)), 2) as avg_pageviews,
	round(avg(coalesce(sessionqualitydim, 0)), 2) as avg_sessionqualitydim,
	case when
		round(avg(coalesce(timeonsite, 0)), 2) > (select total_avg_timeonsite from average) 
		and round(avg(coalesce(pageviews, 0)), 2) < (select total_avg_pageviews from average)
			then 1
		else 0
		end as behaviour
 FROM public."ecommerce-session-bigquery"
 group by fullvisitorid, timeonsite, pageviews  
)

/*Menampilkan rata-rata timeonsite dan pageviews daris emua user serta jumlah user yang memiliki time_on_site 
lebih banyak dari rata-rata tetapi page_views yang lebih rendah dari rata-rata*/
select (select total_avg_timeonsite from average), (select total_avg_pageviews from average),
 count(case when behaviour = 1 then 1 else NULL end) as high_time_low_pageviews,
 round((100*(count(case when behaviour = 1 then 1 else NULL end)/(count(behaviour)::float)))) as percentage
from user_behaviour;

/*Hasil dari query ini menampilkan rata-rata waktu yang dihabiskan di situs adalah sebesar 699, tidak ada penjelasan
besaran waktu yang digunakan, tetapi jika diasumsikan menggunakan detik maka waktu rata-rata adalah 10 menit. Rata- rata halaman
yang dilihat adalah sebanyak 22 halaman. Jumlah user yang termasuk ke kategori time_on_site 
lebih banyak dari rata-rata tetapi page_views yang lebih rendah dari rata-rata sebanyak 648 atau 9% dari total user.
*/