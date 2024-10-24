import os
import csv
import time
from SinaCoreScrape import get_file_content
from config import get_proxies

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
}


def process_csv_files(folder_path):
    # 获取文件夹中所有csv文件的路径
    csv_files = [file for file in os.listdir(
        folder_path) if file.endswith('.csv')]

    # 遍历每个csv文件
    for file in csv_files[::]:
        file = os.path.join(folder_path, file)
        process_csv(file)


proxies = get_proxies()


def process_csv(file_path):
    print(f"正在处理文件：{file_path}")

    # 读取csv文件
    with open(file_path, mode='r', errors="ignore", encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)

    # 打印将被处理的行的内容
    rows_to_process = [row for row in rows if row['研报文本'] == '']
    if rows_to_process:
        print("将被处理的行的内容：")
        for row in rows_to_process:
            print(row)

    # 删除将被处理的行
    rows = [row for row in rows if row['研报文本'] != '']

    # 将更新后的数据写回csv文件
    with open(file_path, mode='w', encoding='utf-8-sig', newline='') as csvfile:
        fieldnames = rows[0].keys()  # 获取字段名
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


folder_path = r"N:\Source_for_sale\分析师研报\分析师债券报告"
process_csv_files(folder_path)
# file_path = r"N:\Source_for_sale\分析师研报\分析师行业报告\2023-05.csv"
# process_csv(file_path)
