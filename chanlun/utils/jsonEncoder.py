
import json

from chanlun.objects import BI

class BiJSONEncoder(json.JSONEncoder):
    def default(self, o: BI):
        # 返回字典类型
        return {"high": o.high,
                "low": o.low,
                # "fx_a_low": o.fx_a.low,
                # "fx_a_high": o.fx_a.high,
                # "fx_b_low": o.fx_b.low,
                # "fx_b_high": o.fx_b.high,
                # "direction": o.direction.value,
                "power": o.power,
                "left_dt": o.fx_a.dt.strftime('%Y-%m-%d %H:%M:%S'),
                "right_dt": o.fx_b.dt.strftime('%Y-%m-%d %H:%M:%S')}