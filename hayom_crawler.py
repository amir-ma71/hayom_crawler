import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

#       ****  CRAWl ******
# ***** israelhayom.co.il/ *********
#  get RSS bof this site, it runs every day

# -* request settings *-
headers = {'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"}
proxyDict = {"https": "10.253.38.1:808"}

print(" ************** this is israelhayom site ***********")
def get_last_date():
    crawled_data_old = pd.read_csv("data.csv", quoting=1, encoding="utf-8",
                                   sep="~")  # reading crawled data to add new crawl
    if len(crawled_data_old) != 0:
        last_date = list(crawled_data_old["date"])[-1]
    else:
        # "%d/%m/%Y, %H:%M:%S"
        last_date = "01 Aug 2000 00:00:00"
    print("last date: ", last_date)
    del crawled_data_old
    return last_date


counterLINK = 1
counterRSS = 1
counterNOTEXT = 1
while True:
    site = requests.get('https://www.israelhayom.co.il/rss.xml',
                        proxies=proxyDict,
                        headers=headers)
    soup = BeautifulSoup(site.content.decode(), 'lxml')

    last_Date_old_df = get_last_date()
    last_Date_old_df_changed = time.strptime(last_Date_old_df, "%d %b %Y %H:%M:%S")

    print(counterRSS, " RSS crawled")
    counterRSS += 1

    for link_index in range(1, len(soup.findAll("item"))):
        crawled_data = pd.DataFrame()

        if link_index % 10 == 0:
            last_Date_old_df = get_last_date()
            last_Date_old_df_changed = time.strptime(last_Date_old_df, "%d %b %Y %H:%M:%S")

        now_news_date = str(soup.findAll("link")[-link_index].parent.contents[5]).split(",")[1].split("+")[0].strip()
        now_news_date_changed = time.strptime(now_news_date, "%d %b %Y  %H:%M:%S")

        if now_news_date_changed > last_Date_old_df_changed:
            link = soup.findAll("link")[-link_index].next
            link = re.sub('\n', '', link).strip()
            crawled_data["title"] = [soup.findAll("title")[-link_index].next]
            crawled_data["description"] = [soup.findAll("description")[-link_index].next]
            time.sleep(5)
            site_selected = requests.get(link,
                                         proxies=proxyDict,
                                         headers=headers,
                                         )

            soup_selected = BeautifulSoup(site_selected.content, 'html.parser')
            texts = soup_selected.select('.single-post-content > .text-content > p')
            if len(texts) != 0:
                news_text = ""
                for txt in texts:
                    tx = re.sub(r'[A-Za-z0-9]', '', str(txt))
                    tx = re.sub('[<=-_>:,;%/~"]', '', tx)
                    tx = re.sub('\n', '', tx).strip()
                    tx = re.sub('\t', '', tx).strip()
                    news_text = news_text + tx + " "
                crawled_data["text"] = [news_text]

            else:
                print(counterNOTEXT, "have no text! ----", link)
                counterNOTEXT += 1
                continue
            crawled_data["category"] = [link.split("/")[3]]
            try:
                crawled_data["creator"] = [str(
                    soup_selected.select('.single-post-meta_info > .single-post-meta-author_names > span > a')[
                        0].contents[0])]
            except:
                crawled_data["creator"] = ["**no creator**"]

            crawled_data["date"] = [now_news_date]

            crawled_data["link"] = [link]
            # time.sleep(4)

            crawled_data.to_csv("data.csv", quoting=1, encoding="utf-8", sep="~", index=False, mode="a", header=False)
            print(counterLINK, " link crawled Done!")
            counterLINK += 1
        else:
            continue

    df = pd.read_csv("data.csv", quoting=1, sep="~", encoding="utf-8")
    df = df.drop_duplicates(subset=["link"])
    df.to_csv("data.csv", quoting=1, encoding="utf-8", sep="~", index=False)
    del df

    time.sleep(3600)
