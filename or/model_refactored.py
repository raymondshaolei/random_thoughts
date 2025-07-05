import pandas as pd
from input_refactored import input_data


class Model:
    class Sets:
        def __init__(self, parent_model):
            self.parent = parent_model
            self.J_ptlr = None

    class Parameter:
        def __init__(self, parent_model):
            self.parent = parent_model

    class Variable:
        def __init__(self, parent_model):
            self.parent = parent_model
            self.ptlr = None

    class Constraint:
        def __init__(self, parent_model):
            self.parent = parent_model
            self.ptlr = None

    class Objective:
        def __init__(self, parent_model):
            self.parent = parent_model
            self.ptlr = None

    def __init__(self, input_data):
        self.input_data = input_data
        self.sets = self.Sets(self)
        self.parameters = self.Parameter(self)
        self.variables = self.Variable(self)
        self.constraints = self.Constraint(self)
        self.objectives = self.Objective(self)

    def run_work_flow(self):
        self.create_sets()

    def create_sets(self):
        self.create_ptlr()

    def create_ptlr(self):
        df = self.input_data.work_order_routing_step
        print(df)


# m = Model(input_data)
# m.run_work_flow()

class Father:
    def __init__(self):
        self.func_in()

    def func_in(self):
        print('father')


class Child(Father):
    def __init__(self):
        super(Child, self).__init__()
        self.func_in()

    def func_in(self):
        print('child')


import pandas as pd


def find_zero_slack_combinations(slack_df, resource_df):
    """
    找出同班次某种尺寸Slack之和==0且另一个尺寸Slack之和不为0的组合

    参数:
        slack_df (pd.DataFrame): 包含standardResourceCode, shift和Slack的数据
        resource_df (pd.DataFrame): 包含standardResourceCode, processId和尺寸的数据

    返回:
        pd.DataFrame: 包含需要调整的resourceCode, shift, 尺寸和调整方向的数据
    """
    # 合并Slack数据和资源尺寸数据
    merged_df = pd.merge(slack_df, resource_df, on='standardResourceCode', how='left')

    # 按resourceCode, shift和尺寸分组计算Slack总和
    grouped = merged_df.groupby(['standardResourceCode', 'shift', '尺寸'])['Slack'].sum().reset_index()

    # 找出每种resourceCode和shift组合下的尺寸情况
    pivot_df = grouped.pivot_table(index=['standardResourceCode', 'shift'],
                                   columns='尺寸',
                                   values='Slack',
                                   aggfunc='sum').reset_index()

    # 填充NaN为0，表示该尺寸没有记录(可能意味着Slack为0)
    pivot_df = pivot_df.fillna(0)

    # 找出需要调整的情况
    # 情况1: 大=0, 小!=0
    condition1 = (pivot_df['大'] == 0) & (pivot_df['小'] != 0)
    # 情况2: 小=0, 大!=0
    condition2 = (pivot_df['小'] == 0) & (pivot_df['大'] != 0)
    # 情况3: 大=0, 小=0 (不需要调整)

    # 筛选出需要调整的记录
    adjust_df = pivot_df[condition1 | condition2].copy()

    # 添加调整方向列
    adjust_df['adjust_direction'] = ''
    adjust_df.loc[condition1, 'adjust_direction'] = '大'
    adjust_df.loc[condition2, 'adjust_direction'] = '小'

    # 整理结果
    result = adjust_df[['standardResourceCode', 'shift', 'adjust_direction']]
    result.columns = ['resourceCode', 'shift', 'adjust_size']

    return result


# 示例数据构造
slack_data = {
    'standardResourceCode': ['BU22-J6327', 'BU22-J6327', 'BU22-J6327', 'BU22-J6327',
                             'BU22-J6327', 'RC45-K8912', 'RC45-K8912', 'RC45-K8912'],
    'shift': [0, 1, 2, 3, 4, 0, 1, 2],
    'Slack': [5.0, 5.0, 0.0, 5.0, 5.0, 0.0, 3.0, 0.0]
}
slack_df = pd.DataFrame(slack_data)
print(slack_df)
x = slack_df[slack_df['shift'] == 4]
print(x)
y = x['shift']
print(y)
print(type(y))

