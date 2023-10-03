import time
import re
import pymongo
import pymongo.errors
import requests
from bs4 import BeautifulSoup
from datetime import timedelta, datetime

# MongoDB에 연결
client = pymongo.MongoClient(host='mongodb://admin:123@svc.sel3.cloudtype.app:32019/?authMechanism=DEFAULT')
database_name = "newchat"
db = client[database_name]

media_list = {586: '시사저널', 9: '매일경제', 660: 'kbc광주방송', 52: 'YTN', 23: '조선일보', 16: '헤럴드경제', 607: '뉴스타파', 437: 'JTBC',
              32: '경향신문', 11: '서울경제', 55: 'SBS', 20: '동아일보', 5: '국민일보', 56: 'KBS', 448: 'TV조선', 25: '중앙일보', 15: '한국경제',
              8: '머니투데이', 469: '한국일보', 18: '이데일리', 215: '한국경제TV', 3: '뉴시스', 629: '더팩트', 421: '뉴스1', 21: '문화일보',
              28: '한겨레', 277: '아시아경제', 36: '한겨레21', 640: '코리아중앙데일리', 22: '세계일보', 88: '매일신문', 57: 'MBN', 82: '부산일보',
              1: '연합뉴스', 366: '조선비즈', 29: '디지털타임스', 14: '파이낸셜뉴스', 214: 'MBC', 79: '노컷뉴스', 81: '서울신문', 422: '연합뉴스TV',
              449: '채널A', 346: '헬스조선', 30: '전자신문', 47: '오마이뉴스', 293: '블로터', 138: '디지털데일리', 6: '미디어오늘', 127: '기자협회보',
              119: '데일리안', 374: 'SBS Biz', 87: '강원일보', 24: '매경이코노미', 2: '프레시안', 92: '지디넷코리아', 7: '일다', 296: '코메디닷컴',
              648: '비즈워치', 44: '코리아헤럴드', 31: '아이뉴스24', 308: '시사IN', 50: '한경비즈니스', 37: '주간동아', 654: '강원도민일보',
              243: '이코노미스트', 33: '주간경향', 53: '주간조선', 262: '신동아', 662: '농민신문', 659: '전주MBC', 655: 'CJB청주방송', 123: '조세일보',
              417: '머니S', 584: '동아사이언스', 658: '국제신문', 94: '월간 산', 657: '대구MBC', 666: '경기일보', 656: '대전일보', 661: 'JIBS',
              665: '더스쿠프', 145: '레이디경향'}
category_list = {"100": "정치", "101": "경제", "102": "사회", "103": "생활/문화", "104": "세계", "105": "IT/과학"}
head = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/114.0.0.0 Whale/3.21.192.18 Safari/537.36"}

DELAY_TIME = 0.5


def save_news_in_mongodb(database, news_data):
    try:
        collection = database["news"]
        collection.insert_one(news_data)
    except pymongo.errors.DuplicateKeyError:
        print("중복된 기사가 크롤링 되었습니다.")
    except Exception as e:
        print(f"데이터 베이스 저장 중 에러 발생 : {e}")


def load_date_from_mongodb(database):
    collection = database["logs"]
    latest_data = collection.find_one(sort=[("date", pymongo.ASCENDING)]).get('date')

    yesterday_date = latest_data - timedelta(days=1)
    collection.insert_one({"date": yesterday_date})
    yesterday_date_str = yesterday_date.strftime("%Y%m%d")

    return yesterday_date_str


def _get_content_from_naver_news_url(url):
    response = requests.get(url, headers=head)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    title = soup.find('div', {"class": "media_end_head_title"}).get_text(strip=True)
    content = soup.select_one("#newsct_article").get_text()
    content = re.sub(r'\s+', ' ', content)

    write_time = soup.find('span', {"class": "media_end_head_info_datestamp_time _ARTICLE_DATE_TIME"}).attrs[
        'data-date-time']
    write_time = datetime.strptime(write_time, "%Y-%m-%d %H:%M:%S")
    image = soup.find('img', id='img1')

    if image is None:
        image = ''
    else:
        image = image.attrs['data-src']

    return title, content, image, write_time


def crawling_naver_news():
    while True:
        start_day = load_date_from_mongodb(db)
        print(start_day, "크롤링 시작")

        # 100 : 정치, 101 : 경제, 102 : 사회, 103: 생활/문화, 104 : 세계, 105 : IT/과학
        for category in range(100, 106):
            page = 0

            while True:
                page += 1
                try:
                    newsListUrl = f"https://news.naver.com/main/list.naver?mode=LSD&mid=shm&sid1={category}&date={start_day}&page={page}"
                    response = requests.get(newsListUrl, headers=head)
                    soup = BeautifulSoup(response.text, 'html.parser')

                    cur_page = soup.select_one("#main_content > div.paging > strong").get_text()

                    # 같은 페이지를 두번 탐색하는 경우
                    if int(cur_page) != page:
                        break

                    urls = [url.attrs['href'] for url in soup.select(".type06_headline > li > dl > dt > a")]
                    urls.extend([url.attrs['href'] for url in soup.select(".type06 > li > dl > dt > a")])

                    time.sleep(DELAY_TIME)
                    for url in set(urls):
                        try:
                            title, content, image, write_time = _get_content_from_naver_news_url(url)

                            summary = None

                            news_id = url[43:53]
                            media = media_list[int(url[39:42])]

                            news_data = {"news_id": news_id, "category": category_list[str(category)],
                                         "title": title, "content": content, "summary": summary,
                                         "media": media, "image": image, "time": write_time, "url": url}
                            if len(content) > 512:
                                save_news_in_mongodb(db, news_data)

                        except Exception as e:
                            print(f"뉴스 페이지에서 에러 발생 : {e}")
                            time.sleep(DELAY_TIME)

                except Exception as e:
                    print(f"뉴스 리스트에서 에러 발생 : {e.args}")
                    time.sleep(DELAY_TIME)


if __name__ == "__main__":
    crawling_naver_news()

