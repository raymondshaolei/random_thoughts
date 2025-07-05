from itertools import product
from collections import defaultdict
import time
import numpy as np
import pandas as pd
from typing import Union


#
# class BackIndexSet:
#     def __init__(self, *args):
#         if not args:
#             return
#         sizes = []
#         total = 1
#         self.back_index = []
#         self.index_dict = {}
#
#         for s in args:
#             size = len(s)
#             sizes.append(size)
#             total *= size
#             self.back_index.append(defaultdict(set))
#
#         for idx in range(total):
#             combination = []
#             temp = idx
#             for i in range(len(args)):
#                 # 类似"进制转换"，计算当前集合的索引
#                 size = sizes[i]
#                 pos = temp % size
#                 ele = list(args[i])[pos]
#                 combination.append(ele)
#                 self.back_index[i][ele].add(idx)
#                 temp = temp // size
#             self.index_dict[idx] = combination
#
#     def select(self, *args):
#         index_set = set(self.index_dict.keys())
#         for i, arg in enumerate(args):
#             if arg == '*':
#                 continue
#             elif isinstance(arg, list):
#                 tmp_set = set()
#                 for ele in arg:
#                     tmp_set |= self.back_index[i][ele]
#                 index_set &= tmp_set
#             else:
#                 index_set &= set(self.back_index[i][arg])
#         return index_set

# set1 = [i for i in range(1000)]
# set2 = [j for j in range(100)]
# set3 = [k for k in range(10)]
#
#
# result = BackIndexSet(set1, set2, set3)
# print(f'back_index 占用内存：{asizeof.asizeof(result.back_index)/1024/1024} MB')
# print(f'index_dict 占用内存：{asizeof.asizeof(result.index_dict)/1024/1024} MB')
# print(f'result 占用内存：{asizeof.asizeof(result)/1024/1024} MB')
#
# # print(result.index_dict)
# # print(result.back_index)
# ind = [i for i in range(1000) if i % 10 == 3]
# ind2 = [i for i in range(100) if i % 8 == 3]
#
# st_time = time.time()
# x = result.select(ind, ind2, '*')
# ed_time = time.time()
# print(f'select query used time: {ed_time - st_time} s')
# print(f'x 长度{len(x)}')
# st_time2 = time.time()
# y = set(index for index, (idx1, idx2, idx3) in result.index_dict.items() if idx1 in ind and idx2 in ind2)
# ed_time2 = time.time()
# print(f'for loop query used time: {ed_time2 - st_time2} s')
# print(f'y 长度{len(y)}')

# 生成数据
arrays = [np.arange(1000), np.arange(100), np.arange(10)]
multi_idx = pd.MultiIndex.from_product(arrays, names=['A', 'B', 'C'])
df = pd.DataFrame(index=multi_idx).reset_index()

# 定义条件范围
ind = [i for i in range(1000) if i % 10 == 3]  # A: 3, 13, 23, ..., 993
ind2 = [i for i in range(100) if i % 8 == 3]  # B: 3, 11, 19, ..., 99

# 测试布尔 Mask
# start = time.perf_counter()
# result = df.loc[df['A'].isin(ind) & df['B'].isin(ind2)].index
# mask_time = time.perf_counter()
# print(f'loc 耗时：{mask_time - start} s')
#
# st = time.perf_counter()
# result1 = df.index[df['A'].isin(ind) & df['B'].isin(ind2)]
# et = time.perf_counter()
#
# print(f'index 耗时：{et - st} s')
# print(len(result))
# print(result)

class InputData:
    def __init__(self):
        self.plan_horizon: Union[pd.DataFrame, None] = None
        self.work_order_routing: Union[pd.DataFrame, None] = None
        self.work_order_routing_step: Union[pd.DataFrame, None] = None
        self.work_order_routing_step_input: Union[pd.DataFrame, None] = None
        self.work_order_routing_step_output: Union[pd.DataFrame, None] = None
        self.work_order_routing_step_resource: Union[pd.DataFrame, None] = None
        self.input_supply: Union[pd.DataFrame, None] = None
        self.input_demand: Union[pd.DataFrame, None] = None

    def read_data(self):
        self.load_plan_horizon(r'planHorizon.csv')
        self.load_work_order_routing(r'workOrderRouting.csv')
        self.load_work_order_routing_step(r'workOrderRoutingStep.csv')
        self.load_work_order_routing_step_input(r'workOrderRoutingStepInput.csv')
        self.load_work_order_routing_step_output(r'workOrderRoutingStepOutput.csv')
        self.load_work_order_routing_step_resource(r'workOrderRoutingStepResource.csv')

    def load_plan_horizon(self, plan_horizon_file: str):
        df = pd.read_csv(
            plan_horizon_file, dtype={'periodSequence': int}, parse_dates=['periodStartTime', 'periodEndTime']
        )
        self.plan_horizon = df

    def load_work_order_routing(self, work_order_routing_file: str):
        df = pd.read_csv(work_order_routing_file)
        self.work_order_routing = df

    def load_work_order_routing_step(self, work_order_routing_step_file: str):
        df = pd.read_csv(work_order_routing_step_file)
        self.work_order_routing_step = df

    def load_work_order_routing_step_input(self, work_order_routing_step_input_file: str):
        df = pd.read_csv(work_order_routing_step_input_file)
        self.work_order_routing_step_input = df

    def load_work_order_routing_step_output(self, work_order_routing_step_output_file: str):
        df = pd.read_csv(work_order_routing_step_output_file)
        self.work_order_routing_step_output = df

    def load_work_order_routing_step_resource(self, work_order_routing_step_resource_file: str):
        df = pd.read_csv(work_order_routing_step_resource_file)
        self.work_order_routing_step_resource = df

    def find_output_which_used_given_input(self):
        given_input = ['1403000495', '1403000475', '1403000476']
        rs_id = self.work_order_routing_step_input.loc[self.work_order_routing_step_input['productStockPointId'].isin(given_input), 'routingStepId']
        res = self.work_order_routing_step_output.loc[self.work_order_routing_step_output['routingStepId'].isin(rs_id), 'productStockPointId']
        print(res)
        return res

    def create_set(self, *args):
        pass


input_data = InputData()
input_data.read_data()
# input_data.find_output_which_used_given_input()