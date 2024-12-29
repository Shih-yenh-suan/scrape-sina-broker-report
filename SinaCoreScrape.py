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

df = pd.read_excel(r"scrape-sina-broker-report\basicInfo.xlsx", header=None, names=[
    "stock_code", "company_name"])
name_to_code = df.set_index('company_name')['stock_code'].to_dict()


def scrape_page(URL, HEADERS, proxies):
    """爬取代码封装"""
    result = retry_on_failure(
        lambda: requests.get(URL, headers=HEADERS, proxies=proxies).text)
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
    file_id = []
    for ty, ti in list(zip(*(file_type, file_title))):
        if ty in ["公司", "创业板"]:
            file_id.append(find_stock_code(ti))
        else:
            file_id.append(ty)

    file_info = [file_id, file_url, file_title, file_type,
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
    def __init__(self, reportDate, saving_path, customerized_type=[""], proxies={}):
        self.reportDate = reportDate
        self.csv_index = ["股票代码", "券商简称", "发布日期",
                          "研报标题", "报告链接", "研报文本", "研究员"]
        self.saving_path = f"{saving_path}"
        self.customerized_type = customerized_type
        self.proxies = proxies

    def process_page_for_downloads(self):
        """处理指定页码的公告信息并下载相关文件"""
        first_url = f'https://stock.finance.sina.com.cn/stock/go.php/vReport_List/kind/search/index.phtml?t1=6&symbol=&p=1&pubdate={self.reportDate}'
        parsed_html_first = scrape_page(first_url, HEADERS, self.proxies)
        total_pages = parsed_html_first.xpath(
            '//a[text()="最末页"]/@onclick')
        if not total_pages:
            tqdm.write(f"{self.reportDate} 为空，跳过")
            return
        total_pages = total_pages[0].split(
            "set_page_num(\'")[1].split("\')")[0]
        total_pages = int(total_pages)
        for pageNum in tqdm(range(total_pages), desc=f"{self.reportDate}"):
            URL = f'https://stock.finance.sina.com.cn/stock/go.php/vReport_List/kind/search/index.phtml?t1=6&symbol=&p={pageNum + 1}&pubdate={self.reportDate}'
            parsed_html = scrape_page(URL, HEADERS, self.proxies)
            file_info = unpack_and_standarise_response(parsed_html)
            for files in file_info:
                self.download_file(files)

    def download_file(self, files):
        """分块下载文件"""
        (file_id, url, title, type, broker, researcher) = files
        file_short_name = f"{file_id}_{broker}_{self.reportDate}"

        # 跳过未指定的类型
        if self.customerized_type == [""]:
            self.customerized_type = ["行业", "公司",
                                      "创业板", "债券", "策略", "晨报", "基金", "宏观"]
        elif self.customerized_type == ["个股"]:
            self.customerized_type = ["公司", "创业板"]
        else:
            self.customerized_type = self.customerized_type
        if type not in self.customerized_type:
            # tqdm.write(f"\t{file_short_name}：不属于指定的报告类型")
            return

        # 指定下载文件夹
        if type in ["公司", "创业板"]:
            downloading_type = "个股"
        else:
            downloading_type = type

        # 以文件链接作为主键，防止重复下载
        saving_file = f"{self.saving_path}\分析师{downloading_type}报告\{self.reportDate[:7]}.csv"
        if not os.path.exists(saving_file):
            df = pd.DataFrame(columns=self.csv_index)
            df.to_csv(saving_file, index=False)
        df = pd.read_csv(saving_file, encoding='utf-8-sig',
                         encoding_errors="ignore", dtype=str)
        urls = [str(row["报告链接"])
                for index, row in df.iterrows()]
        if url in urls:
            tqdm.write(f"\t{file_short_name}：已存在，跳过")
            return

        # 组成文件在表中的列表
        csv_info_list = [file_id, broker, self.reportDate,
                         title, url, [], researcher]
        # 获取文件内容
        csv_info_list[5] = get_file_content(url, self.proxies)
        # 追加文件
        df = pd.DataFrame([csv_info_list], columns=self.csv_index)
        df.to_csv(saving_file, mode='a', header=False, index=False)
        tqdm.write(f"{file_short_name}：已保存")


def retry_on_failure(func):
    """对于请求失败的情况，暂停一段时间"""
    pause_time = 3
    try:
        result = func()
        return result
    except Exception as e:
        tqdm.write(f'Error, 重试中')
        time.sleep(pause_time)
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
        file_content = etree.HTML(file_content).xpath(
            '//div[@class="blk_container"]/p//text()')
        file_content = [f.strip() for f in file_content]
        file_content = "\n".join(file_content)
        if file_content or repeat_times > 10:
            break
        else:
            t = random.uniform(2, 5) * repeat_times
            tqdm.write(f"{url} 为空，暂停 {t} 秒")
            time.sleep(t)
            repeat_times += 1
    # tqdm.write(f"{file_content[:30]}…………")
    return file_content


# #####################################################
# """此处为需要修改的代码"""
# start_date = "2004-06-01"  # "2004-06-01"
# end_date = None  # None # "2023-09-09"
# saving_path = r""

# ######################################################
# if __name__ == "__main__":
#     for Times in create_date_intervals(start_date, end_date)[::-1]:
#         if DateProcesser(Times, saving_path, proxies={}).process_page_for_downloads() == False:
#             break
