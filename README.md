# 爬取新浪财经券商研报

从新浪财经的券商研报中爬取个股报告，并按月存入 csv 文件中。

## 网站

https://stock.finance.sina.com.cn/stock/go.php/vReport_List/kind/search/index.phtml

## 思路

给出日期和页码，循环爬取网站，将对应内容保存到 csv 文件中，按月份分类。

## 使用方式

1. 解除最后的注释部分。
2. 修改参数：

   1. start_date：爬取的开始日期
   2. end_date：爬取的结束日期，默认为 None，即为当日
   3. saving_path：代码的保存文件夹。
   4. report_type：研报的类型，共有个股、行业、策略、宏观、基金、债券、晨报六种。除了个股对应着网页中“报告类型”为公司和创业板两个类型外，其他类型均与网页中“报告类型”一致。

3. 运行代码

## 注意事项

1. 新浪财经存在一定的反爬。只要特定 ip 在一段时间内执行爬取任务，即使每次请求间隔一分钟以上，也会被检测到，造成爬取失败。因此，最好使用代理池（广告位招租）。如需使用代理池，请自行修改`proxies={}`的部分。
