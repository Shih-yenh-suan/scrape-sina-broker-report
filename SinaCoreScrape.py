# -*- encoding: utf-8 -*-
import requests
import time
import os
import pandas as pd
from lxml import etree
import random
import datetime

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0',
}


class DateProcesser:
    def __init__(self, reportDate):
        self.reportDate = reportDate

    def process_page_for_downloads(self, pageNum: int):
        """处理指定页码的公告信息并下载相关文件"""
        URL = f'https://stock.finance.sina.com.cn/stock/go.php/vReport_List/kind/search/index.phtml?t1=6&symbol=&p={pageNum}&pubdate={self.reportDate}'
        # 向网站获取内容和总页数，必须分开获取，否则容易报错
        result = retry_on_failure(
            lambda: requests.get(URL, headers=HEADERS).text)
        time.sleep(random.uniform(5, 15))
        print("==" * 10 + f"{self.reportDate} 日第 {pageNum} 页：开始" + "==" * 10)
        parsed_html = etree.HTML(result)
        # 判断是否已经结束
        is_final_page = parsed_html.xpath(
            '//table[@class="tb_01"]/tr')
        if len(is_final_page) <= 3 and pageNum == 1:
            parsed_html = etree.HTML(
                result=requests.get(URL, headers=HEADERS).text)
            is_final_page = parsed_html.xpath(
                '//table[@class="tb_01"]/tr')
        if len(is_final_page) <= 3:
            print(f"第 {pageNum} 页无内容：{URL}")
            return False
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
        for files in file_info:
            self.download_file(files)
        print("==" * 10 + f"{self.reportDate} 日第 {pageNum} 页：已完成" + "==" * 10)

    def download_file(self, files):
        """分块下载文件"""
        (url, title, type, broker, researcher) = files
        # 跳过不是个股研究的报告
        if type not in ["公司", "创业板"]:
            print(f"\t不是个股文件：{type}\t{title}")
            return
        # 从标题中获取代码、简称和文章题目
        shortName, ids, headline = get_id_and_name(title)
        # 有些证券公司总喜欢搞些奇怪的格式
        shortName = shortName.replace("深度*公司*", "")
        # 组成文件的简称
        file_short_name = f"{ids}_{broker}_{shortName}_{self.reportDate}"
        # 组成文件在表中的列表
        csv_info_list = [ids, broker, self.reportDate,
                         shortName, headline, url, []]
        csv_index = ["股票代码", "券商简称", "发布日期", "企业简称", "研报标题", "报告链接", "研报摘要"]
        # 持久化存储
        saving_file = f"{SAVING_PATH}\{self.reportDate[:7]}.csv"
        if not os.path.exists(saving_file):
            df = pd.DataFrame(columns=csv_index)
            df.to_csv(saving_file, index=False)
        # 以文件链接作为主键，防止重复下载
        df = pd.read_csv(saving_file, encoding='utf-8-sig',
                         encoding_errors="ignore", dtype=str)
        urls = [str(row["报告链接"])
                for index, row in df.iterrows()]
        # 如果主键已经存在在文件中，则跳过
        if url in urls:
            print(f"{file_short_name}：已存在，跳过")
            return
        # 获取文件内容
        file_content = retry_on_failure(lambda:
                                        requests.get(url, headers=HEADERS).text)
        time.sleep(random.uniform(0.5, 1.5))
        file_content = etree.HTML(file_content).xpath(
            '//div[@class="blk_container"]/p/text()')
        file_content = [f.strip() for f in file_content]
        file_content = "\n".join(file_content)
        csv_info_list[6] = file_content
        # 追加文件
        df = pd.DataFrame([csv_info_list], columns=csv_index)
        df.to_csv(saving_file, mode='a', header=False, index=False)
        print(f"{file_short_name}：已保存")


def retry_on_failure(func):
    """对于请求失败的情况，暂停一段时间"""
    pause_time = 3
    try:
        result = func()
        return result
    except requests.exceptions.ProxyError as e:
        print(f'Error: {e}, 暂停 {pause_time} 秒')
        time.sleep(pause_time)
        return retry_on_failure(func)


def get_id_and_name(title: str) -> tuple:
    """从研报标题中提取代码、简称和文章题目"""
    left_parenthesis_index = title.find('(')
    if left_parenthesis_index != -1:
        # 提取左括号左边的内容作为简称
        short_name = title[:left_parenthesis_index]

        # 找到右括号的位置
        right_parenthesis_index = title.find(')', left_parenthesis_index)
        if right_parenthesis_index != -1:
            # 提取括号内的内容作为代码
            code = title[left_parenthesis_index + 1:right_parenthesis_index]
            code = str(code)
            # 提取括号右边的部分作为标题
            title = title[right_parenthesis_index + 2:]
            title = title.title().replace(":", "", 1)
        else:
            code = None
            title = None
    else:
        short_name = None
        code = None
        title = None

    return short_name, code, title


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


SAVING_PATH = rf"E:\[待整理]Source_for_sale\券商研报"
# if __name__ == "__main__":
#     start_date = "2000-12-31"
#     end_date = None
#     for Times in create_date_intervals(start_date, end_date)[::-1]:
#         page = 1
#         while True:
#             if DateProcesser(Times).process_page_for_downloads(page) == False:
#                 break
#             page += 1
