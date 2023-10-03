import threading
from kobart import KoBart
import time


def summarize_news(database, max_thread=3, chunk_size=10):
    # 요약 모델
    model = KoBart()

    collection = database["news"]
    query = {"summary": None}

    # 쿼리를 사용하여 데이터 조회
    while True:
        # 스레드 리스트 초기화
        threads = []
        try:
            result = []

            result_cursor = collection.find(query)

            for result_cursor in result_cursor:
                result.append({"content": result_cursor["content"], "_id": result_cursor["_id"]})

            if len(result) > chunk_size:
                for i in range(0, len(result), chunk_size):
                    if len(threads) < max_thread:
                        thread = threading.Thread(target=summarize_news_in_mongodb,
                                                  args=(collection, model, result[i:i+chunk_size]))
                        threads.append(thread)
                        thread.start()
                    else:
                        thread = threads[0]
                        thread.join()
                        threads.remove(thread)          
            else:
                time.sleep(5)
        except Exception as e:
            print(f"뉴스 요약 스레드에서 에러 발생 : {e.args}")
            # 모든 스레드가 종료될 때까지 대기
            for thread in threads:
                thread.join()
                threads.remove(thread)


def summarize_news_in_mongodb(collection, model, results):
    for result in results:
        try:
            summary = model(result["content"])
            collection.update_one({"_id": result["_id"]}, {"$set": {"summary": summary}})
        except Exception as e:
            print(f"뉴스 요약 모델에서 에러 발생 : {e.args}")
            print(len(result["content"]))