import asyncio
import httpx
from bs4 import BeautifulSoup
from tqdm import tqdm
import polars as pl
import json

base_url = "https://www.fortiguard.com/encyclopedia?type=ips&risk={level}&page={page}"

#Fungsi untuk memanggil url dan mengambil table-body untuk discrape
async def fetch_web(client, url: str) -> object:
    #Membuat error handling untuk timeout dan http error
    try:
        response = await client.get(url, timeout = 10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")
        return soup.find("section", class_="table-body")
    except httpx.TimeoutException:
        raise TimeoutError(f"Timeout error for URL: {url}")
    except httpx.HTTPStatusError as e:
        raise Exception(f"HTTP error {e.response.status_code} for URL: {url}")

#Fungsi untuk scrape title dan link dari tiap baris pada sebuah page
async def scrape(scraped_page: object) -> tuple[list[str], list[str]]:
    list_of_links = []
    list_of_titles = []
    
    #Iterasikan tiap baris artikel yang terdapat pada sebuah page
    for row in scraped_page.find_all("div", class_="row"):
        link = "https://www.fortiguard.com/" + row.get('onclick')[18:].strip("'")
        title = row.find("b").get_text()
        list_of_links.append(link)
        list_of_titles.append(title)

    #Mengembalikan daftar link dan judul artikel   
    return list_of_links, list_of_titles

#Fungsi untuk iterasi semua halaman pada satu level
async def iterate_pages(pages: int, level: int) -> tuple[object, list[int]]:
    tasks = []
    skipped_pages_per_level = []

    #For loop untuk iterasi semua halaman pada sebuah level
    for page in tqdm(range(1, pages + 1), desc=f"Level {level}"):
            url = base_url.format(level=level, page=page)
            try:
                #Memanggil fungsi fetch_web
                async with httpx.AsyncClient() as client:
                    scraped_page = await fetch_web(client, url)
                    if scraped_page == None:
                        skipped_pages_per_level.append(page)
                        continue
            #jika terjadi timeout maka page tersbut akan dimasukkan ke daftar page yang diskip
            except TimeoutError as e:
                skipped_pages_per_level.append(page)
                continue
            #jika terjadi error maka page tersbut akan dimasukkan ke daftar page yang diskip
            except Exception as e:
                skipped_pages_per_level.append(page)
                continue
            #Memasukkan task scrape untuk tiap-tiap halaman agar bisa dijalankan secara bersamaan
            tasks.append(scrape(scraped_page))
    
    return tasks, skipped_pages_per_level

#Fungsi untuk menyimpan data ke csv
async def write_to_csv(pages: int, level: int) -> list[int]:
    link_list = []
    title_list = []

    page_task, skipped_pages_per_level = await iterate_pages(pages, level)

    #Menjalankan task scrape dari semua halaman dan menyimpan hasilnya di results
    results = await asyncio.gather(*page_task)

    for result in results:
        list_of_links, list_of_titles = result
        link_list += list_of_links
        title_list += list_of_titles

    #Memasukan daftar link dan title yang sudah discrape ke dalam dictionary kemudian dimasukan ke dataframe
    data = {
        'title': title_list,
        'link': link_list
    }
    df = pl.DataFrame(data)

    #Membuat file csv berdasarkan data yang sudah diambil
    df.write_csv(f"datasets/forti_lists_{level}.csv")

    #Mengembalikan variabel berisi list dari halaman yang diskip
    return skipped_pages_per_level

async def main(levels: list[int], pages: list[int]) -> None:
    skip_page_dict = {}
    tasks = []
    count = 1
    
    #For loop untuk iterasi tiap level
    for index in range(len(levels)):

        #Menambahkan task scrape_Pages untuk tiap level agar bisa dijalankan secara bersamaan
        tasks.append(write_to_csv(pages[index], levels[index]))

    #Menjalankan scraping untuk tiap level secara bersamaan
    results = await asyncio.gather(*tasks)
    
    for result in results:
        #Memasukan data halaman yang diskip pada sebuah dictionary untuk diubah menjadi json
        skip_page_dict[f"Level {count}"] = tuple(result)
        count += 1
    print("DONE\n")

    #Membuat file skipped.json untuk page yang diskip
    with open("datasets/skipped.json", "w") as outfile:
        json.dump(skip_page_dict, outfile)

if __name__ == "__main__":
    levels = [1, 2, 3, 4, 5]
    pages = [13, 56, 197, 421, 271]

    asyncio.run(main(levels, pages))