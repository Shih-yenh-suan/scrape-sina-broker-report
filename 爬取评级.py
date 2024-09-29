# -*- encoding: utf-8-sig -*-
import requests
import time
import os
import re
import pandas as pd
from lxml import etree
import random
import datetime
from tqdm import tqdm
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
}
SAVEPATH = r"E:\[待整理]Source_for_sale\分析师研报\个股评级"


def get_proxies():
    tunnel = "u799.kdltps.com:15818"
    username = "t12646522330157"
    password = "ylyjxhby"
    proxies = {
        "http": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel},
        "https": "http://%(user)s:%(pwd)s@%(proxy)s/" % {"user": username, "pwd": password, "proxy": tunnel}
    }
    return proxies
    proxies = {}
    return proxies


def retry_on_failure(func):
    """对于请求失败的情况，暂停一段时间"""
    pause_time = 3
    try:
        result = func()
        return result
    except:
        tqdm.write(f'Error, 暂停 {pause_time} 秒')
        # time.sleep(pause_time)
        return retry_on_failure(func)


def main():
    with open('scrape-sina-broker-report\list-A.txt', 'r') as f:
        symbols = [l.strip() for l in f.readlines()]
    for s in symbols:
        print(f'{s}：开始')
        get(s)
    # with ThreadPoolExecutor() as executor:
    #     futures = [executor.submit(get, s)
    #                for s in symbols]
    #     for future in as_completed(futures):
    #         future.result()  # 获取结果，以捕获异常


def get(symbol):
    for root, dirs, files in os.walk(f"{SAVEPATH}"):
        if f"{symbol}.csv" in files:
            print(f'{symbol}：\t已存在，跳过下载')
            return
    url = f"https://stock.finance.sina.com.cn/stock/go.php/vIR_StockSearch/key/{symbol}.phtml?num=99999"
    file_content = retry_on_failure(lambda:
                                    requests.get(url, headers=HEADERS, proxies=get_proxies()).text)
    soup = BeautifulSoup(file_content, 'html.parser')

    # 找到表格
    index = ["股票代码", "目标价", "评级", "评级机构", "分析师", "评级日期", "报告链接"]
    table = soup.find('table')
    股票代码 = []
    目标价 = []
    评级 = []
    评级机构 = []
    分析师 = []
    评级日期 = []
    报告链接 = []
    # 提取表格数据
    try:
        table.find_all('tr')
    except:
        return
    for row in table.find_all('tr'):
        cols = row.find_all('td')
        if len(cols) >= 4:  # 确保有至少4列
            股票代码.append(cols[0].text.strip())
            目标价.append(cols[2].text.strip())
            评级.append(cols[3].text.strip())
            评级机构.append(cols[4].text.strip())
            分析师.append(cols[5].text.strip())
            评级日期.append(cols[7].text.strip())
            报告链接.append(cols[8].find('a')['href'].strip()
                        if cols[8].find('a') else '')

    报告链接 = [f"https:{f}" for f in 报告链接]
    股票代码 = 股票代码[1:]
    目标价 = 目标价[1:]
    评级 = 评级[1:]
    评级机构 = 评级机构[1:]
    分析师 = 分析师[1:]
    评级日期 = 评级日期[1:]
    报告链接 = 报告链接[1:]

    table = [股票代码, 目标价, 评级, 评级机构, 分析师, 评级日期, 报告链接]
    table = [list(row) for row in zip(*table)]
    df = pd.DataFrame(table, columns=index)
    df = df[df["股票代码"] == symbol]

    save_df(symbol, df)


def save_df(symbol, df):
    报告链接 = [str(row["报告链接"])
            for index, row in df.iterrows()]
    csv_file_path = f'{SAVEPATH}\{symbol}.csv'
    # 检查CSV文件是否存在，如果不存在则创建
    if not os.path.exists(csv_file_path):
        # 如果文件不存在，先将DataFrame写入文件
        df.to_csv(csv_file_path, index=False,
                  mode='w', encoding='utf-8-sig')
    else:
        existing_df = pd.read_csv(csv_file_path, encoding='utf-8-sig',
                                  encoding_errors="ignore", dtype=str)
        urls = [str(row["报告链接"])
                for index, row in existing_df.iterrows()]
        for url in 报告链接:
            if url in urls:
                df = df[df["报告链接"] != url]
                continue

        # 如果文件存在，则追加到文件末尾
        df.to_csv(csv_file_path, index=False, mode='a',
                  header=False, encoding='utf-8-sig')
    print("数据已成功写入到", csv_file_path)


main()
# get("000001", 1)
