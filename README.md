# 爬取新浪财经券商研报

从新浪财经的券商研报中爬取个股报告，并按月存入csv文件中。

## 网站

https://stock.finance.sina.com.cn/stock/go.php/vReport_List/kind/search/index.phtml

## 思路

1. 给出日期和页码，循环爬取网站，将对应内容保存到csv文件中，按月份分类。
2. 由于网站反爬，经常遇到网页数据加载不出来的问题。故在所有查到页面无内容的情况下，将页面和对应日期保存到txt文件中。然后，通过 get_url_from_file 变量，设为1的时候，就重新爬取txt文件的内容。重新获取后，无论是否有内容，都删除这条记录

## 使用方式

1. 将 SAVING_PATH 修改为文件的保存路径

```
SAVING_PATH = r""
```

2. 运行代码

```
DateProcesser(Times, records_txt).process_page_for_downloads(page)
```

其中：
Times 为指定的日期，形如“YYYY-MM-DD”
records_txt 为下载异常的网址名单存放路径
page 为指定的下载页。

如需要循环爬取，请使用如下代码：

```
for Times in create_date_intervals(start_date, end_date):
   page = 1
   while True:
         if DateProcesser(Times, records_txt).process_page_for_downloads(page) == False:
            break
         page += 1
```
其中：
start_date：定义爬取的开始日期（含）
end_date：定义爬取的结束日期（含）
默认开始日期为2000年1月1日，结束日期为当前日期


3. 对爬取失败的网址进行重新爬取：

```
DateProcesser("", records_txt).process_url_from_files()
```

其中：
records_txt 为下载异常的网址名单存放路径


## 问题和Todo

1. 对于 get_id_and_name 函数，早年的报告标题不规范，目前的提取方式可能无法胜任
2. 目前通过报告类型筛选个股研报，选择的报告类型包括“公司”和“创业板”，因此
   1. 可能遗漏其他个股研报，但目前没发现
   2. 完全可以实现对其他类型的爬取，但目前无需求，且未想到好的保存方法
3. 有时候会抽风，明明页面还有内容，就显示没有了，会造成遗漏
   1. 已解决：见上面的3