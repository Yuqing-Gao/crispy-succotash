import pandas as pd
import os
import sys
import numpy as np


class DocumentReconstruction:
    def __init__(self):
        self.current_directory = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(
            os.path.abspath(__file__))
        self.volume_data = pd.read_excel('volume_data.xlsx', header=18)
        self.running_data = pd.read_excel('running_data.xlsx', header=18)

    def volume_data_basic_values_calculation(self):
        # 文件重构
        self.volume_data = self.volume_data.fillna(0)
        volume_data2 = self.volume_data.copy().drop(['Sales Units R1 (NE,NC)', 'Sales Units R1 (E,C)'], axis=1)
        self.volume_data.drop(['Sales Units CP (NE,NC)', 'Sales Units CP (E,C)'], axis=1, inplace=True)
        self.volume_data.insert(0, 'Period', 'R1')
        volume_data2.insert(0, 'Period', 'CP')
        self.volume_data = self.volume_data.rename(
            columns={'Sales Units R1 (NE,NC)': 'Sales Units(NE,NC)', 'Sales Units R1 (E,C)': 'Sales Units(E,C)'})
        volume_data2 = volume_data2.rename(
            columns={'Sales Units CP (NE,NC)': 'Sales Units(NE,NC)', 'Sales Units CP (E,C)': 'Sales Units(E,C)'})
        self.volume_data = pd.concat([self.volume_data, volume_data2])
        self.volume_data.loc[self.volume_data['COPIES'] != 'REGULAR', 'Sales Units(NE,NC)'] = 0
        self.volume_data.to_excel(r'results_volume_data/origin.xlsx', index=False)  # 整合后的表
        # 分组聚合
        self.volume_data = pd.pivot_table(self.volume_data, index=['REGION2', 'CITY2'], columns='Period',
                                          values=['Sales Units(NE,NC)', 'Sales Units(E,C)'], aggfunc=sum)
        self.volume_data.reset_index(inplace=True)
        self.volume_data.to_excel(r'results_volume_data/result.xlsx')
        # 以下为非常愚蠢的裁缝操作
        self.volume_data = pd.read_excel(r'results_volume_data/result.xlsx')
        self.volume_data.drop(self.volume_data.columns[0], axis=1, inplace=True)
        self.volume_data.drop([0, 1], axis=0, inplace=True)
        self.volume_data.to_excel(r"results_volume_data/temp.xlsx")
        self.volume_data.rename(
            columns={'Sales Units(E,C)': 'Sales Units CP (E,C)', 'Unnamed: 4': 'Sales Units R1 (E,C)',
                     'Sales Units(NE,NC)': 'Sales Units CP (NE,NC)',
                     'Unnamed: 6': 'Sales Units R1 (NE,NC)'}, inplace=True)
        self.volume_data.to_excel(r"results_volume_data/temp2.xlsx", index=False)
        new_order = ['REGION2', 'CITY2', 'Sales Units R1 (NE,NC)', 'Sales Units R1 (E,C)',
                     'Sales Units CP (NE,NC)', 'Sales Units CP (E,C)']
        self.volume_data = self.volume_data[new_order]
        self.volume_data.to_excel(r"results_volume_data/temp3.xlsx", index=False)
        self.volume_data['real_ratio'] = self.volume_data.apply(
            lambda row: (row['Sales Units CP (NE,NC)'] / row['Sales Units R1 (NE,NC)'] - 1) if row[
                                                                                                   'Sales Units R1 (NE,NC)'] != 0 else None,
            axis=1)
        self.volume_data['model_ratio'] = self.volume_data.apply(
            lambda row: (row['Sales Units CP (E,C)'] / row['Sales Units R1 (E,C)'] - 1) if row[
                                                                                               'Sales Units R1 (E,C)'] != 0 else np.nan,
            axis=1)
        self.volume_data.to_excel(r"results_volume_data/temp4.xlsx", index=False)
        # 生成判断值ACT
        conditions = [
            (self.volume_data['Sales Units CP (NE,NC)'] is None) | (self.volume_data['Sales Units CP (NE,NC)'] < 100),
            (self.volume_data['model_ratio'] > 0.3) | (
                    self.volume_data['model_ratio'] * self.volume_data['real_ratio'] < 0)
        ]
        values = ['无法处理', '异常']
        self.volume_data['ACT'] = np.select(conditions, values, default='正常')
        self.volume_data.to_excel(r'results_volume_data/temp5.xlsx', index=False)

    def running_data_basic_values_calculation(self):
        # 文件重构
        self.running_data = self.running_data.fillna(0)
        running_data2 = self.running_data.copy().drop(['Sales Units R1 (NE,NC)', 'Sales Units R1 (E,C)'], axis=1)
        self.running_data.drop(['Sales Units CP (NE,NC)', 'Sales Units CP (E,C)'], axis=1, inplace=True)
        self.running_data.insert(0, 'Period', 'R1')
        running_data2.insert(0, 'Period', 'CP')
        self.running_data = self.running_data.rename(
            columns={'Sales Units R1 (NE,NC)': 'Sales Units(NE,NC)', 'Sales Units R1 (E,C)': 'Sales Units(E,C)'})
        running_data2 = running_data2.rename(
            columns={'Sales Units CP (NE,NC)': 'Sales Units(NE,NC)', 'Sales Units CP (E,C)': 'Sales Units(E,C)'})
        self.running_data = pd.concat([self.running_data, running_data2])
        self.running_data.loc[self.running_data['COPIES'] != 'REGULAR', 'Sales Units(NE,NC)'] = 0
        self.running_data.to_excel(r'results_running_data/origin.xlsx', index=False)  # 整合后的表
        # 分组聚合
        self.running_data = pd.pivot_table(self.running_data, index=['REGION2', 'CITY2', 'BRAND'], columns='Period',
                                           values=['Sales Units(NE,NC)', 'Sales Units(E,C)'], aggfunc=sum)
        self.running_data.reset_index(inplace=True)
        self.running_data.to_excel(r'results_running_data/result.xlsx')
        # 以下为非常愚蠢的裁缝操作
        self.running_data = pd.read_excel(r'results_running_data/result.xlsx')
        self.running_data.drop(self.running_data.columns[0], axis=1, inplace=True)
        self.running_data.drop([0, 1], axis=0, inplace=True)
        self.running_data.to_excel(r"results_running_data/temp.xlsx")
        self.running_data.rename(
            columns={'Sales Units(E,C)': 'Sales Units CP (E,C)', 'Unnamed: 5': 'Sales Units R1 (E,C)',
                     'Sales Units(NE,NC)': 'Sales Units CP (NE,NC)',
                     'Unnamed: 7': 'Sales Units R1 (NE,NC)'}, inplace=True)
        self.running_data.to_excel(r"results_running_data/temp2.xlsx", index=False)

        # 按照city计算份额占比
        self.running_data['Sales_Units_share R1 (NE,NC)'] = self.running_data.groupby('CITY2')[
            'Sales Units R1 (NE,NC)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.running_data['Sales_Units_share R1 (E,C)'] = self.running_data.groupby('CITY2')[
            'Sales Units R1 (E,C)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.running_data['Sales_Units_share CP (NE,NC)'] = self.running_data.groupby('CITY2')[
            'Sales Units CP (NE,NC)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)
        self.running_data['Sales_Units_share CP (E,C)'] = self.running_data.groupby('CITY2')[
            'Sales Units CP (E,C)'].transform(lambda x: (x / x.sum()) if x.sum() else np.nan).astype(float)

        new_order = ['REGION2', 'CITY2', 'BRAND', 'Sales Units R1 (NE,NC)', 'Sales Units R1 (E,C)', 'Sales_Units_share R1 (NE,NC)', 'Sales_Units_share R1 (E,C)',
                     'Sales Units CP (NE,NC)', 'Sales Units CP (E,C)', 'Sales_Units_share CP (NE,NC)', 'Sales_Units_share CP (E,C)']

        self.running_data = self.running_data[new_order]
        self.running_data['real share diff'] = self.running_data['Sales_Units_share CP (NE,NC)'] - self.running_data['Sales_Units_share R1 (NE,NC)']
        self.running_data['model share diff'] = self.running_data['Sales_Units_share CP (E,C)'] - self.running_data['Sales_Units_share R1 (E,C)']
        self.running_data.to_excel(r"results_running_data/temp3.xlsx", index=False)

        # 生成判断值ACT
        conditions = [
            ((self.running_data['Sales Units R1 (NE,NC)'] is None) & (self.running_data['Sales Units CP (NE,NC)'] is None)),
            (self.running_data['Sales Units CP (E,C)'] < 100) | (self.running_data['real share diff'] is None) ,
            (self.running_data['model share diff'] > 0.03) | (self.running_data['model share diff'] * self.running_data['real share diff'] < 0)
        ]
        values = ['无法处理', '无法处理', '异常']
        self.running_data['ACT'] = np.select(conditions, values, default='正常')
        self.running_data.to_excel(r'results_running_data/temp4.xlsx', index=False)

    def volume_data_file_processing(self):
        pass

    def running_data_file_processing(self):
        pass


if __name__ == '__main__':
    document_reconstruction = DocumentReconstruction()
    document_reconstruction.volume_data_basic_values_calculation()
    document_reconstruction.running_data_basic_values_calculation()
