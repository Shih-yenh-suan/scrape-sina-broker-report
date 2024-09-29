import pandas as pd
import os


def process_csv_files(folder_path):
    # 遍历指定文件夹中的所有CSV文件
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(folder_path, filename)

            # 读取CSV文件
            df = pd.read_csv(file_path, encoding="utf-8",
                             encoding_errors="ignore")

            # 新增“研究员”列
            # df['研究员'] = ''

            # 重命名“研报摘要”列为“研报文本”
            if '研报摘要' in df.columns:
                df.rename(columns={'研报摘要': '研报文本'}, inplace=True)

                # 保存修改后的CSV文件
                df.to_csv(file_path, index=False)
                print(f'Processed file: {filename}')
            else:
                print(
                    f'File {filename} does not contain the column "���报��要". Skipping.')
                continue


# 示例使用
folder_path = r'N:\Source_for_sale\分析师研报\分析师个股报告'  # 替换为你的文件夹路径
process_csv_files(folder_path)
