# -*- encoding: utf-8 -*-
import requests
import time
import os
import re
import pandas as pd
from lxml import etree
import random
import datetime
from tqdm import tqdm
HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
}
TYPES = {
    "个股": ["公司", "创业板"],
    "行业": ["行业"],
    "策略": ["策略"],
    "宏观": ["宏观"],
    "基金": ["基金"],
    "债券": ["债券"],
}


df = pd.read_excel(r"scrape-sina-broker-report\basicInfo.xlsx", header=None, names=[
    "stock_code", "company_name"])
name_to_code = df.set_index('company_name')['stock_code'].to_dict()


def scrape_page(URL, HEADERS, proxies):
    """爬取代码封装"""
    result = retry_on_failure(
        lambda: requests.get(URL, headers=HEADERS, proxies=proxies).text)
    # time.sleep(random.uniform(0.5, 1.5))
    parsed_html = etree.HTML(result)
    return parsed_html


def unpack_and_standarise_response(parsed_html):
    """将返回的代码整理成需要的元素，并组成元组列表"""
    # 处理超链接
    file_url = parsed_html.xpath('//td[@class="tal f14"]/a/@href')
    file_url = [f"https:{url}" for url in file_url]
    # 处理标题
    file_title = parsed_html.xpath('//td[@class="tal f14"]/a/@title')
    # 报告类型，债券/行业/宏观/公司等等
    file_type = parsed_html.xpath(
        '//div[@class="main"]/table/tr/td[3]/text()')
    # 处理机构
    file_broker = parsed_html.xpath(
        '//div[@class="main"]/table/tr/td[5]/a/div/span/text()')
    # 处理研究员
    file_researcher = parsed_html.xpath(
        '//div[@class="main"]/table/tr/td[6]/div/span/text()')
    file_info = [file_url, file_title, file_type,
                 file_broker, file_researcher]
    file_info = list(zip(*file_info))
    return file_info


def find_stock_code(input_string):
    # 读取Excel文件
    # 查找股票代码模式
    stock_code_pattern = re.compile(r'\d{6}')
    stock_code_match = stock_code_pattern.search(input_string)
    if stock_code_match:
        matched_code = stock_code_match.group()
        if matched_code in df['stock_code'].values:
            return matched_code

        return stock_code_match.group()

    # 如果没有找到股票代码，则查找企业简称
    for company_name in name_to_code:
        if company_name in input_string:
            return name_to_code[company_name]

    # 如果没有找到匹配的股票代码或企业简称，返回None
    return None


class DateProcesser:
    def __init__(self, reportDate, records_txt, saving_path, report_types, proxies={}):
        self.reportDate = reportDate
        self.records_txt = records_txt
        self.csv_index = ["股票代码", "券商简称", "发布日期",
                          "研报标题", "报告链接", "研报文本", "研究员"]
        self.saving_path = saving_path
        self.proxies = proxies
        self.report_types = TYPES[report_types]

    def process_url_from_files(self):
        """从文件中获取URL，重新执行爬取"""
        # 检查记录文件是否存在，不存在则创建
        if not os.path.exists(self.records_txt):
            with open(self.records_txt, 'w') as file:
                pass
        # 循环读取文件的第一行，以实现爬取一个删除一个。
        while True:
            with open(self.records_txt, 'r', encoding='utf-8', errors='ignore') as f:
                records = f.readlines()
            # 如果没有行，则跳出
            if not records:
                print(f"全部记录已重新爬取")
                break
            record = records[0]
            url = record.split(',')[0]
            reportDate = record.split(',')[1].rstrip()
            self.reportDate = reportDate
            saving_file = f"{self.saving_path}\{self.reportDate[:7]}.csv"
            df = pd.read_csv(saving_file, encoding='utf-8-sig',
                             encoding_errors="ignore", dtype=str)
            urls = [str(row["报告链接"])
                    for index, row in df.iterrows()]
            # 修改 url，从 url提供的数字开始，向上 +1，循环爬取
            match = re.search(r"&p=(\d+)&", url)
            pageNum = int(match.group(1))
            while True:
                new_url = re.sub(r'(&p=)\d+', r'\g<1>' + str(pageNum), url)
                print(f"开始：{new_url}")
                parsed_html = scrape_page(new_url, HEADERS, self.proxies)
                is_final_page = parsed_html.xpath(
                    '//table[@class="tb_01"]/tr')
                if len(is_final_page) == 3:
                    tr_length = len(parsed_html.xpath(
                        '//table[@class="tb_01"]/tr[3]/td'))
                    if tr_length == 1:
                        print(
                            f"第 {pageNum} 页无内容：{len(is_final_page)}, {new_url}")
                        break
                elif len(is_final_page) == 0:
                    # 为0说明爬取错误了，直接退出，同时更新链接
                    with open(self.records_txt, 'w', encoding='utf-8', errors='ignore') as file:
                        file.writelines(f"{new_url},{reportDate}\n")
                        file.writelines(records[1:])
                    print(f"第 {pageNum} 页错误：{len(is_final_page)}, {new_url}")
                    return
                file_info = unpack_and_standarise_response(parsed_html)
                for files in file_info:
                    self.download_file(files, urls)
                pageNum += 1
            # 处理完成后，从记录中移除当前URL，也就是不将当前URL加回去
            with open(self.records_txt, 'w', encoding='utf-8', errors='ignore') as file:
                file.writelines(records[1:])
            print(f"已从记录中删除：{url}")

    def process_page_for_downloads(self, pageNum: int):
        """处理指定页码的公告信息并下载相关文件"""
        # 持久化存储
        saving_file = f"{self.saving_path}\{self.reportDate[:7]}.csv"
        if not os.path.exists(saving_file):
            df = pd.DataFrame(columns=self.csv_index)
            df.to_csv(saving_file, index=False)
        # 以文件链接作为主键，防止重复下载
        df = pd.read_csv(saving_file, encoding='utf-8-sig',
                         encoding_errors="ignore", dtype=str)
        urls = [str(row["报告链接"])
                for index, row in df.iterrows()]
        URL = f'https://stock.finance.sina.com.cn/stock/go.php/vReport_List/kind/search/index.phtml?t1=6&symbol=&p={pageNum}&pubdate={self.reportDate}'
        parsed_html = scrape_page(URL, HEADERS, self.proxies)
        is_final_page = parsed_html.xpath(
            '//table[@class="tb_01"]/tr')
        if len(is_final_page) == 3:
            tr_length = len(parsed_html.xpath(
                '//table[@class="tb_01"]/tr[3]/td'))
            if tr_length == 1:
                # 刚好为3行，且只有一列的时候，是无内容的
                print(f"第 {pageNum} 页开始，可能是最后一页：{URL}")
                file_info = unpack_and_standarise_response(parsed_html)
                for files in file_info:
                    self.download_file(files, urls)
                print("==" * 10 +
                      f"{self.reportDate} 日第 {pageNum} 页：已完成" + "==" * 10)
                return False
        elif len(is_final_page) == 0:
            # 为0说明爬取错误了，记下来。
            print(f"第 {pageNum} 页错误：{len(is_final_page)}, {URL}")
            with open(self.records_txt, 'a', encoding='utf-8', errors='ignore') as f:
                f.write(f'{URL},{self.reportDate}\n')
            return False
        print(f"第 {pageNum} 页开始：{URL}")
        file_info = unpack_and_standarise_response(parsed_html)
        for files in file_info:
            self.download_file(files, urls)
        print("==" * 10 + f"{self.reportDate} 日第 {pageNum} 页：已完成" + "==" * 10)

    def download_file(self, files, urls):
        """分块下载文件"""
        (url, title, type, broker, researcher) = files
        # 跳过不是个股研究的报告
        if type not in self.report_types:
            print(f"\t不是{self.report_types}文件：{type}\t{title}")
            return
        # 从标题中获取代码、简称和文章题目
        if type in ["公司", "创业板"]:
            ids = find_stock_code(title)
        else:
            ids = type
        file_short_name = f"{ids}_{broker}_{self.reportDate}"
        # 组成文件在表中的列表
        csv_info_list = [ids, broker, self.reportDate,
                         title, url, [], researcher]
        # 如果主键已经存在在文件中，则跳过
        if url in urls:
            print(f"{file_short_name}：已存在，跳过")
            return
        # 获取文件内容
        csv_info_list[5] = get_file_content(url, self.proxies)
        # 追加文件
        saving_file = f"{self.saving_path}\{self.reportDate[:7]}.csv"
        df = pd.DataFrame([csv_info_list], columns=self.csv_index)
        df.to_csv(saving_file, mode='a', header=False, index=False)
        print(f"{file_short_name}：已保存")


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


def create_date_intervals(start_date="2000-01-01", end_date=None) -> list:
    """创建包含开始到结束的日期列表"""
    # 将字符串日期转换为datetime对象
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    # 如果没有提供结束日期，则默认为今天
    if end_date is None:
        end = datetime.datetime.today()
    else:
        end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    # 初始化日期列表
    date_intervals = []
    current_date = start
    while current_date <= end:
        date_intervals.append(current_date.strftime("%Y-%m-%d"))
        current_date += datetime.timedelta(days=1)

    return date_intervals


def get_file_content(url, proxies):
    repeat_times = 1
    while True:
        file_content = retry_on_failure(lambda:
                                        requests.get(url, headers=HEADERS, proxies=proxies).text)
        # time.sleep(random.uniform(0.5, 1.5))
        file_content = etree.HTML(file_content).xpath(
            '//div[@class="blk_container"]/p//text()')
        file_content = [f.strip() for f in file_content]
        file_content = "\n".join(file_content)
        if file_content or repeat_times > 10:
            break
        else:
            t = random.uniform(2, 5) * repeat_times
            print(f"{url} 为空，暂停 {t} 秒")
            # time.sleep(t)
            repeat_times += 1
    print(f"{file_content[:50]}…………")
    return file_content


# ######################################################
# """此处为需要修改的代码"""
# start_date = "2000-06-01"  # "2004-06-01"
# end_date = None  # None # "2023-09-09"
# records_txt = r"N:\Source_for_sale\分析师研报\个股报告\已下载记录.txt"
# get_url_from_file = 0
# saving_path = r"N:\Source_for_sale\分析师研报"
# report_type = "个股"
# saving_path = rf"N:\Source_for_sale\分析师研报\分析师{report_type}报告"

# ######################################################
# if __name__ == "__main__":
#     if get_url_from_file == 1:
#         DateProcesser("2000-01-01", records_txt,
#                       saving_path, proxies).process_url_from_files()
#     else:
#         for Times in create_date_intervals(start_date, end_date)[::-1]:
#             page = 1
#             while True:
#                 if DateProcesser(Times, records_txt, saving_path, report_type, proxies).process_page_for_downloads(page) == False:
#                     break
#                 page += 1
