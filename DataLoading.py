import glob
from KakfaFolder.Admin import createTopic
import time
from timeit import default_timer as timer
from multiprocessing import Pool, cpu_count
from KakfaFolder.Producer import uploading


if __name__  == "__main__" :
    fileList = glob.glob("D:\Docker\ETL\Dataset\*.json")
    topicList = list(map(lambda path : path.split("\\")[-1].split(".")[0] ,fileList))
    createTopic(topicList)
    start = timer()
    p = Pool()
    for topic in topicList:
        p.apply_async(uploading,[topic])
    p.close()
    p.join()
    end = timer()
    print(f'elapsed time: {end - start}')