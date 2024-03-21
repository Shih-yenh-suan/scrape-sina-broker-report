import os
import pandas as pd
import requests
import time
import random
from lxml import etree

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
}


def process_csv_files(folder_path):
    # 获取文件夹中所有csv文件的路径
    csv_files = [file for file in os.listdir(
        folder_path) if file.endswith('.csv')]

    # 遍历每个csv文件
    for file in csv_files:
        file = os.path.join(folder_path, file)
        process_csv(file)


def process_csv(file_path):
    print(f"正在处理文件：{file_path}")
    # 读取csv文件
    df = pd.read_csv(file_path)
    # 打印将被处理的行的内容
    rows_to_process = df[df['研报摘要'].isna()]
    if not rows_to_process.empty:
        print("将被处理的行的内容：")
        print(rows_to_process)
        # 对于每一行，获取文件内容并添加到“研报摘要”列
        for i, (index, row) in enumerate(rows_to_process.iterrows()):
            print(
                f"{time.strftime('%H:%M:%S', time.localtime())}: {row['股票代码']}_{row['发布日期']}：正在处理{i+1}/{len(rows_to_process)}")
            report_link = row['报告链接']
            # 使用你已经实现的 get_file_content 函数
            file_content = get_file_content(report_link)
            df.at[index, '研报摘要'] = file_content
            df.to_csv(file_path, encoding='utf-8-sig', index=False)


def get_file_content(url):
    file_content = requests.get(url, headers=HEADERS).text
    time.sleep(random.uniform(0.5, 1.5))
    file_content = etree.HTML(file_content).xpath(
        '//div[@class="blk_container"]/p/text()')
    file_content = [f.strip() for f in file_content]
    file_content = "\n".join(file_content)
    print(file_content)
    return file_content


# 输入文件夹路径
folder_path = r"E:\[待整理]Source_for_sale\券商研报\已整理"
file_path = r"E:\[待整理]Source_for_sale\券商研报\新浪研报\2021-04.csv"

# 处理csv文件夹
process_csv_files(folder_path)
# 处理csv文件
# process_csv(file_path)
