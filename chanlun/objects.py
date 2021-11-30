# coding: utf-8
from dataclasses import dataclass
from datetime import datetime
from typing import List
from .enums import Mark, Direction, Freq, Operate

@dataclass
class Tick:
    symbol: str
    name: str = ""
    price: float = 0
    vol: float = 0

@dataclass
class RawBar:
    """原始K线元素"""
    symbol: str
    id: int          # id 必须是升序
    dt: datetime
    freq: str
    open: [float, int]
    close: [float, int]
    high: [float, int]
    low: [float, int]
    vol: [float, int]

@dataclass
class NewBar:
    """去除包含关系后的K线元素"""
    symbol: str
    id: int          # id 必须是升序
    dt: datetime
    freq: str
    open: [float, int]
    close: [float, int]
    high: [float, int]
    low: [float, int]
    vol: [float, int]
    elements: List[RawBar]   # 存入具有包含关系的原始K线

@dataclass
class FX:
    symbol: str
    freq: str
    id: int
    dt: datetime
    mark: str
    high: [float, int]
    low: [float, int]
    fx: [float, int]
    power: str
    elements: List[NewBar]


@dataclass
class FakeBI:
    """虚拟笔：主要为笔的内部分析提供便利"""
    symbol: str
    sdt: datetime
    edt: datetime
    direction: Direction
    high: [float, int]
    low: [float, int]
    power: [float, int]

@dataclass
class BI:
    symbol: str
    freq: str
    id: int
    direction: str
    fx_a: FX = None    # 笔开始的分型
    fx_b: FX = None    # 笔结束的分型
    fxs: List[FX] = None    # 笔内部的分型列表
    high: float = None
    low: float = None
    power: float = None
    bars: List[NewBar] = None

@dataclass
class Seq:
    """特征序列"""
    symbol: str
    id: int
    start_dt: datetime
    end_dt: datetime
    freq: str
    direction: str
    high: [float, int]
    low: [float, int]

@dataclass
class SeqFX:
    symbol: str
    freq: str
    id: int
    dt: datetime
    mark: str
    high: [float, int]
    low: [float, int]
    fx: [float, int]
    power: str
    direction: str
    elements: List[Seq]

@dataclass
class Line:
    """线段"""
    symbol: str
    freq: str
    id: int
    direction: str
    start_dt: datetime
    end_dt: datetime
    high: float = None
    low: float = None
    power: float = None
    seqs: List[Seq] = None
    fx_a: SeqFX = None  # 线段开始的分型
    fx_b: SeqFX = None  # 线段结束的分型

@dataclass
class Hub:
    """笔构成的中枢"""
    id: int
    symbol: str
    freq: str
    ZG: float
    ZD: float
    GG: float
    DD: float
    entry: BI = None
    leave: BI = None
    elements: List[BI] = None # 奇数位的笔

@dataclass
class Point:
    """买卖点"""
    id: int
    symbol: str
    freq: str
    dt: datetime
    type: str
    high: float
    low: float

@dataclass
class Signal:
    signal: str = None

    # score 取值在 0~100 之间，得分越高，信号越强
    score: int = 0

    # k1, k2, k3 是信号名称
    k1: str = "任意"
    k2: str = "任意"
    k3: str = "任意"

    # v1, v2, v3 是信号取值
    v1: str = "任意"
    v2: str = "任意"
    v3: str = "任意"

    # 任意 出现在模板信号中可以指代任何值

    def __post_init__(self):
        if not self.signal:
            self.signal = self.__repr__()
        else:
            self.k1, self.k2, self.k3, self.v1, self.v2, self.v3, score = self.signal.split("_")
            self.score = int(score)

        if self.score > 100 or self.score < 0:
            raise ValueError("score 必须在0~100之间")

    def __repr__(self):
        return f"{self.k1}_{self.k2}_{self.k3}_{self.v1}_{self.v2}_{self.v3}_{self.score}"

    @property
    def key(self) -> str:
        """获取信号名称"""
        key = ""
        for k in [self.k1, self.k2, self.k3]:
            if k != "任意":
                key += k + "_"
        return key.strip("_")

    @property
    def value(self) -> str:
        """获取信号值"""
        return f"{self.v1}_{self.v2}_{self.v3}_{self.score}"

    def is_match(self, s: dict) -> bool:
        """判断信号是否与信号列表中的值匹配

        :param s: 所有信号字典
        :return: bool
        """
        key = self.key
        v = s.get(key, None)
        if not v:
            raise ValueError(f"{key} 不在信号列表中")

        v1, v2, v3, score = v.split("_")
        if int(score) >= self.score:
            if v1 == self.v1 or self.v1 == '任意':
                if v2 == self.v2 or self.v2 == '任意':
                    if v3 == self.v3 or self.v3 == '任意':
                        return True
        return False


@dataclass
class Factor:
    name: str
    # signals_all 必须全部满足的信号
    signals_all: List[Signal]
    # signals_any 满足其中任一信号，允许为空
    signals_any: List[Signal] = None

    def is_match(self, s: dict) -> bool:
        """判断 factor 是否满足"""
        for signal in self.signals_all:
            if not signal.is_match(s):
                return False

        if not self.signals_any:
            return True

        for signal in self.signals_any:
            if signal.is_match(s):
                return True
        return False

@dataclass
class Event:
    name: str
    operate: Operate

    # 多个信号组成一个因子，多个因子组成一个事件。
    # 单个事件是一系列同类型因子的集合，事件中的任一因子满足，则事件为真。
    factors: List[Factor]

    def is_match(self, s: dict):
        """判断 event 是否满足"""
        for factor in self.factors:
            if factor.is_match(s):
                # 顺序遍历，找到第一个满足的因子就退出。建议因子列表按关注度从高到低排序
                return True, factor.name

        return False, None

    def __repr__(self):
        return f"{self.name}_{self.operate.value}"

