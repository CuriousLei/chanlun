# 计算线段相关变量（支持增量计算）
stroke_i = 0  # stroke_i指向strokeList中元素（全局变量）
stroke_combine = []  # 合并后的特征序列端点列表,在出现线段转折时清空
strokesList = []  # 笔端点列表
lineList = []  # 线段端点列表
stroke_j = 1  # j用于合并，指向_combine中待判断合并的特征序列的起始端(线段端点的后一个元素（1）)，只有未出现包含时才更新j,且指向最后一个特征序列起始端
jLen = -1  # j及其后元素个数
up = 0  # up=1表示向上笔，up=-1表示向下笔，为0表示开始新一轮线段走势判断
last_i = 0  # 加入线段端点的最后一个i，用于在出现转折点时还原i值
last_line = []  # 更新线段端点时的旧端点，用于在出现包含时还原端点

pivotList = []  # 中枢列表[[日期1，日期2，中枢低点，中枢高点]]
currentPivot = []  # 暂存中枢前三段的四个顶点纵坐标，用于排序获得中枢低点和高点
pivot_i = 1  # pivot_i为lineList中元素下标（全局变量），用于判断中枢的延申或是新生
startDate = ''  # 中枢开始日期
endDate = ''  # 中枢结束日期
pivot_j = 0  # pivot_j为最近一个中枢的元素下标

buy_list = []  # 买点列表[[日期，值，类型]]
sell_list = []  # 卖点列表[[日期，值，类型]]
buy_list_history = []  # 买点列表[[日期，值，类型]]，三类买点分别为B1、B2、B3
sell_list_history = []  # 卖点列表[[日期，值，类型]]，三类卖点分别为S1、S2、S3
update_buy_and_sell = []  # 判断更新买卖点列表的关键位置[[日期，值]]


def calculate_bs(k_list, stroke_with_new=0, pivot_with_stroke=0):
    """"计算买卖点"""
    global stroke_i, stroke_j, jLen, stroke_combine, strokesList, lineList, up, last_i, last_line, \
        pivotList, currentPivot, pivot_i, pivot_j, startDate, endDate, buy_list, sell_list, \
        buy_list_history, sell_list_history, update_buy_and_sell
    combine, stroke = combineK(k_list)  # 旧笔
    combine_new, stroke_new = combineKNew(k_list)  # 新笔
    if stroke_with_new:  # 若使用新笔
        strokesList = stroke_new
    else:  # 若使用旧笔
        strokesList = stroke

    stroke_i = 0
    stroke_combine = []
    lineList = []
    stroke_j = 1
    jLen = -1
    up = 0
    last_i = 0
    last_line = []

    pivotList = []  # 中枢列表[[日期1，日期2，中枢低点，中枢高点]]
    currentPivot = []  # 暂存中枢前三段的四个顶点纵坐标，用于排序获得中枢低点和高点
    pivot_i = 1  # line_i为lineList中元素下标（全局变量）
    startDate = ''  # 中枢开始日期
    endDate = ''  # 中枢结束日期
    pivot_j = 0  # pivot_j为最近一个中枢的元素下标

    buy_list = []
    sell_list = []
    buy_list_history = []
    sell_list_history = []
    update_buy_and_sell = []

    getLine()  # 根据笔端点列表计算线段端点列表
    getPivot(pivot_with_stroke)  # 计算中枢
    getBuyAndSell()  # 计算买卖点
    return strokesList, lineList, pivotList, buy_list, sell_list, buy_list_history, sell_list_history


def combineK(kList):
    """
    K线合并与旧笔划分。

    :param kList: 合并前K线列表【日期，最高价，最低价】
    :type kList: list
    :return combineList: 合并后的K线列表【日期，最高价，最低价】
    :return strokeList: 笔端点列表【日期，纵坐标（最高价或最低价）】
    :rtype: list
    """
    topParting = 0  # 连接笔的最后一个顶分型
    bottomParting = 0  # 连接笔的最后一个底分型
    strokeList = []  # 笔端点列表
    combineList = [kList[0]]
    j = 0  # j指向combineList最后一根K线
    for i in range(1, len(kList)):  # i指向待判断的未合并K线
        if kList[i][1] > combineList[j][1] and kList[i][2] > combineList[j][2]:  # 非包含上升K线
            if j > 0:
                if combineList[j - 1][1] > combineList[j][1]:  # 底分型
                    if j - topParting > 2 and topParting >= bottomParting:  # 更新笔(底)
                        bottomParting = j
                        strokeList.append([combineList[j][0], combineList[j][2]])
                    elif combineList[j][2] < combineList[bottomParting][
                        2] and bottomParting > topParting:  # 出现连续两个底分型，若后者更低，则更新底
                        bottomParting = j
                        strokeList[-1] = [combineList[j][0], combineList[j][2]]
            combineList.append(kList[i])
            j += 1
        elif kList[i][1] < combineList[j][1] and kList[i][2] < combineList[j][2]:  # 非包含下降K线
            if j > 0:
                if combineList[j - 1][1] < combineList[j][1]:  # 顶分型
                    if j - bottomParting > 2 and bottomParting >= topParting:  # 更新笔（顶）
                        topParting = j
                        strokeList.append(combineList[j][0:2])
                    elif combineList[j][1] > combineList[topParting][
                        1] and topParting > bottomParting:  # 出现连续两个顶分型，若后者更高，则更新顶
                        topParting = j
                        strokeList[-1] = combineList[j][0:2]
            combineList.append(kList[i])
            j += 1
        elif kList[i][1] >= combineList[j][1] and kList[i][2] <= combineList[j][2]:  # 包含，i包i-1
            if j > 0:  # 若合并列表中已有2根及以上K线，则根据前一根K线处理包含
                if combineList[j - 1][1] < combineList[j][1]:  # 向上处理
                    combineList[j][1] = kList[i][1]  # 更新高点
                    combineList[j][0] = kList[i][0]  # 更新时间（只有i包i-1情况需要向后更新时间）
                else:  # 向下处理
                    combineList[j][2] = kList[i][2]  # 更新低点
                    combineList[j][0] = kList[i][0]  # 更新时间
            else:  # 合并列表中只有1根K线,则根据最大与最小关系处理包含
                combineList[j] = kList[i]
        elif kList[i][1] <= combineList[j][1] and kList[i][2] >= combineList[j][2]:  # 包含，i-1包i
            if j > 0:  # 若合并列表中已有2根及以上K线，则根据前一根K线处理包含
                if combineList[j - 1][1] < combineList[j][1]:  # 向上处理
                    combineList[j][2] = kList[i][2]  # 更新高点
                else:  # 向下处理
                    combineList[j][1] = kList[i][1]  # 更新低点
    return combineList, strokeList


def combineKNew(kList):
    """
    K线合并与新笔划分。

    :param kList: 合并前K线列表【日期，最高价，最低价】
    :type kList: list
    :return combineList: 合并后的K线列表【日期，最高价，最低价】
    :return strokeList: 笔端点列表【日期，纵坐标（最高价或最低价）】
    :rtype: list
    """
    topParting = 0  # 连接笔的最后一个顶分型
    topParting_i = 0  # 连接笔的最后一个顶分型(对应合并前K线列表)
    bottomParting = 0  # 连接笔的最后一个底分型
    bottomParting_i = 0  # 连接笔的最后一个底分型(对应合并前K线列表)
    strokeList = []  # 笔端点列表
    combineList = [kList[0]]
    j = 0  # j指向combineList最后一根K线
    for i in range(1, len(kList)):  # i指向待判断的未合并K线
        if kList[i][1] > combineList[j][1] and kList[i][2] > combineList[j][2]:  # 非包含上升K线
            if j > 0:
                if combineList[j - 1][1] > combineList[j][1]:  # 底分型
                    if i - 1 - topParting_i > 3 and topParting >= bottomParting:  # 更新笔(底)
                        bottomParting = j
                        bottomParting_i = i - 1
                        strokeList.append([combineList[j][0], combineList[j][2]])
                    elif combineList[j][2] < combineList[bottomParting][
                        2] and bottomParting > topParting:  # 出现连续两个底分型，若后者更低，则更新底
                        bottomParting = j
                        bottomParting_i = i - 1
                        strokeList[-1] = [combineList[j][0], combineList[j][2]]
            combineList.append(kList[i])
            j += 1
        elif kList[i][1] < combineList[j][1] and kList[i][2] < combineList[j][2]:  # 非包含下降K线
            if j > 0:
                if combineList[j - 1][1] < combineList[j][1]:  # 顶分型
                    if i - 1 - bottomParting_i > 3 and bottomParting >= topParting:  # 更新笔（顶）
                        topParting = j
                        topParting_i = i - 1
                        strokeList.append(combineList[j][0:2])
                    elif combineList[j][1] > combineList[topParting][
                        1] and topParting > bottomParting:  # 出现连续两个顶分型，若后者更高，则更新顶
                        topParting = j
                        topParting_i = i - 1
                        strokeList[-1] = combineList[j][0:2]
            combineList.append(kList[i])
            j += 1
        elif kList[i][1] >= combineList[j][1] and kList[i][2] <= combineList[j][2]:  # 包含，i包i-1
            if j > 0:  # 若合并列表中已有2根及以上K线，则根据前一根K线处理包含
                if combineList[j - 1][1] < combineList[j][1]:  # 向上处理
                    combineList[j][1] = kList[i][1]  # 更新高点
                    combineList[j][0] = kList[i][0]  # 更新时间（只有i包i-1情况需要向后更新时间）
                else:  # 向下处理
                    combineList[j][2] = kList[i][2]  # 更新低点
                    combineList[j][0] = kList[i][0]  # 更新时间
            else:  # 合并列表中只有1根K线,则根据最大与最小关系处理包含
                combineList[j] = kList[i]
        elif kList[i][1] <= combineList[j][1] and kList[i][2] >= combineList[j][2]:  # 包含，i-1包i
            if j > 0:  # 若合并列表中已有2根及以上K线，则根据前一根K线处理包含
                if combineList[j - 1][1] < combineList[j][1]:  # 向上处理
                    combineList[j][2] = kList[i][2]  # 更新高点
                else:  # 向下处理
                    combineList[j][1] = kList[i][1]  # 更新低点
    return combineList, strokeList


def getLine():
    """计算缠论指标线段。"""
    global stroke_i, stroke_j, jLen, strokesList, stroke_combine, lineList, up, last_i, last_line
    # 判断起始点合理性
    strokeLen = len(strokesList)  # strokeList的长度
    if stroke_i == 0 and strokeLen - stroke_i > 3:  # 若从i开始的strokeList中还有超过三个元素，则有构成线段的可能
        if ((strokesList[stroke_i][1] < strokesList[stroke_i + 1][1]) and (
                strokesList[stroke_i + 3][1] < strokesList[stroke_i][1])) or (
                (strokesList[stroke_i][1] > strokesList[stroke_i + 1][1]) and (
                strokesList[stroke_i + 3][1] > strokesList[stroke_i][1])):  # 三笔间无重叠区域，不构成线段，转向下一坐标点判断
            stroke_i += 1
        lineList.append(strokesList[stroke_i])  # i所指元素构成线段的左端点,只有从头开始判断线段（非增量判断）时才加入左端点值

    while stroke_i < strokeLen:
        stroke_combine.append(strokesList[stroke_i])  # i所指元素加入合并列表
        jLen += 1

        # 若从端点开始判断线段，需确定线段开始的笔方向
        if stroke_j == 1 and jLen == 1:  # 线段刚开始判断且已有一笔在combine列表中
            if stroke_combine[1][1] > stroke_combine[0][1]:  # 向上笔
                up = 1
            else:
                up = -1
        if up == 1:  # 向上笔
            if jLen == 3:  # stroke_combine只包括连续三笔时，构成最初的线段
                if stroke_j == 1:
                    lineList.append(stroke_combine[stroke_j + 2])
                    last_i = stroke_i
                elif stroke_combine[stroke_j + 2][1] > stroke_combine[stroke_j][1]:
                    last_line = lineList[-1]
                    lineList[-1] = stroke_combine[stroke_j + 2]  # 更新线段右端点

            if jLen == 4:  # stroke_j后出现两个特征序列
                if stroke_j >= 3 and stroke_combine[stroke_j + 1][1] < stroke_combine[stroke_j - 2][1]:  # 无缺口时
                    if stroke_combine[stroke_j + 2][1] < stroke_combine[stroke_j][1]:  # 无缺口笔破坏
                        last_line = stroke_combine[stroke_j]
                        stroke_i = last_i - 1
                        stroke_combine = []  # 清空合并列表
                        stroke_j = 1
                        jLen = -1
                        up = 0
                    else:  # 无缺口延伸
                        stroke_j += 2  # stroke_j移向下一个特征序列的起始端
                        last_line = lineList[-1]
                        lineList[-1] = stroke_combine[stroke_j]  # 更新线段右端点
                        last_i = stroke_i - 1
                        jLen = 2
                elif stroke_j >= 3 and stroke_combine[stroke_j + 1][1] >= stroke_combine[stroke_j - 2][1]:  # 有缺口时
                    if stroke_combine[stroke_j + 2][1] > stroke_combine[stroke_j][1]:  # 有缺口延伸
                        stroke_j += 2  # stroke_j移向下一个特征序列的起始端
                        last_line = lineList[-1]
                        lineList[-1] = stroke_combine[stroke_j]  # 更新线段右端点
                        last_i = stroke_i - 1
                        jLen = 2
                    else:  # 有缺口反向分型
                        last_line = stroke_combine[stroke_j]
                        stroke_i = last_i - 1
                        stroke_combine = []  # 清空合并列表
                        stroke_j = 1
                        jLen = -1
                        up = 0
                else:
                    stroke_j += 2
                    jLen = 2
        elif up == -1:  # 向下笔
            if jLen == 3:  # stroke_combine只包括连续三笔时，构成最初的线段
                if stroke_j == 1:
                    lineList.append(stroke_combine[stroke_j + 2])
                    last_i = stroke_i
                elif stroke_combine[stroke_j + 2][1] < stroke_combine[stroke_j][1]:
                    last_line = lineList[-1]
                    lineList[-1] = stroke_combine[stroke_j + 2]  # 更新线段右端点

            if jLen == 4:  # stroke_j后出现两个特征序列
                if stroke_j >= 3 and stroke_combine[stroke_j + 1][1] > stroke_combine[stroke_j - 2][1]:  # 无缺口时
                    if stroke_combine[stroke_j + 2][1] > stroke_combine[stroke_j][1]:  # 无缺口笔破坏
                        last_line = stroke_combine[stroke_j]
                        stroke_i = last_i - 1
                        stroke_combine = []  # 清空合并列表
                        stroke_j = 1
                        jLen = -1
                        up = 0
                    else:  # 无缺口延伸
                        stroke_j += 2  # stroke_j移向下一个特征序列的起始端
                        last_line = lineList[-1]
                        lineList[-1] = stroke_combine[stroke_j]  # 更新线段右端点
                        last_i = stroke_i - 1
                        jLen = 2
                elif stroke_j >= 3 and stroke_combine[stroke_j + 1][1] <= stroke_combine[stroke_j - 2][1]:  # 有缺口时
                    if stroke_combine[stroke_j + 2][1] < stroke_combine[stroke_j][1]:  # 有缺口延伸
                        stroke_j += 2  # stroke_j移向下一个特征序列的起始端
                        lineList[-1] = stroke_combine[stroke_j]  # 更新线段右端点
                        last_i = stroke_i - 1
                        jLen = 2
                    else:  # 有缺口反向分型
                        last_line = stroke_combine[stroke_j]
                        stroke_i = last_i - 1
                        stroke_combine = []  # 清空合并列表
                        stroke_j = 1
                        jLen = -1
                        up = 0
                else:
                    stroke_j += 2
                    jLen = 2
        stroke_i += 1


def getPivot(pivot_with_stroke=1):
    """
    计算缠论指标中枢。

    :param pivot_with_stroke: 是否使用笔计算中枢， pivot_with_stroke=1时使用笔计算中枢，否则使用线段计算中枢
    """
    # 是否需要全局变量？（是：可以增量更新中枢，无须重复计算； *否：中枢数据量小，重新计算开销小）
    global pivot_i, pivot_j, pivotList, strokesList, lineList, currentPivot, startDate, endDate
    if pivot_with_stroke:
        calculateList = strokesList
    else:
        calculateList = lineList
    listLen = len(calculateList)
    if not pivotList and listLen - pivot_i > 3:  # 若从i开始的calculateList中还有超过三个元素，则有构成中枢的可能
        if ((calculateList[pivot_i][1] < calculateList[pivot_i + 1][1]) and (
                calculateList[pivot_i + 3][1] < calculateList[pivot_i][1])) or (
                (calculateList[pivot_i][1] > calculateList[pivot_i + 1][1]) and (
                calculateList[pivot_i + 3][1] > calculateList[pivot_i][1])):  # 三笔间无重叠区域，不构成线段，转向下一坐标点判断
            pivot_i += 1
    while pivot_i < listLen:
        if pivot_j == 0:  # 第一段端点，记录起始日期
            startDate = calculateList[pivot_i][0]
            currentPivot.append(calculateList[pivot_i][1])
            pivot_j += 1
        elif pivot_j < 3:  # 前三段
            currentPivot.append(calculateList[pivot_i][1])
            pivot_j += 1
        elif pivot_j == 3:  # 三段构成最小中枢
            if ((calculateList[pivot_i][1] < calculateList[pivot_i - 1][1]) and (
                    calculateList[pivot_i - 3][1] < calculateList[pivot_i][1])) or (
                    (calculateList[pivot_i][1] > calculateList[pivot_i - 1][1]) and (
                    calculateList[pivot_i - 3][1] > calculateList[pivot_i][1])):
                pivot_j -= 1
                startDate = calculateList[pivot_i - 2][0]
                del currentPivot[0]
            else:
                endDate = calculateList[pivot_i][0]
                currentPivot.append(calculateList[pivot_i][1])
                currentPivot.sort()  # 排序得到中枢低点与高点（前三段次低点与次高点）
                pivotList.append([startDate, endDate, currentPivot[1], currentPivot[2]])
                pivot_j += 1
        else:  # 三段以后
            if pivotList[-1][2] <= calculateList[pivot_i][1] <= pivotList[-1][3]:  # 若下一段端点落在中枢内，则中枢延伸
                pivotList[-1][1] = calculateList[pivot_i][0]
            elif ((pivotList[-1][3] < calculateList[pivot_i][1]) and (
                    pivotList[-1][2] > calculateList[pivot_i - 1][1])) or (
                    (pivotList[-1][2] > calculateList[pivot_i][1]) and (
                    pivotList[-1][3] < calculateList[pivot_i - 1][1])):  # 若pivot_i与pivot_i-1所指元素刚好包住中枢值区间，则中枢延伸
                pivotList[-1][1] = calculateList[pivot_i - 1][0]
            elif ((pivotList[-1][3] < calculateList[pivot_i][1]) and (
                    pivotList[-1][3] < calculateList[pivot_i - 1][1])) or (
                    (pivotList[-1][2] > calculateList[pivot_i][1]) and (
                    pivotList[-1][2] > calculateList[pivot_i - 1][1])):  # 若pivot_i与pivot_i-1所指元素都在中枢值区间外且在同侧，则中枢新生
                if pivot_j == 4:
                    pivot_i -= 1
                else:
                    pivot_i -= 2
                pivot_j = 0
                currentPivot = []

        pivot_i += 1


def getBuyAndSell():
    """计算三类买卖点。"""
    global pivotList, strokesList, buy_list, sell_list, buy_list_history, sell_list_history, update_buy_and_sell
    buy_list = []  # 买点列表
    sell_list = []  # 卖点列表
    buy_list_history = []  # 历史买点列表
    sell_list_history = []  # 历史卖点列表
    update_buy_and_sell = []  # 判断更新买卖点关键位置列表
    it_stroke = 0  # 笔序号
    it_pivot = 1  # 从第二个中枢开始判断（两个及以上中枢形成走势）

    flag = 0  # 走势标记位，用于判断是否更新一类买卖点，1表示上涨，2表示下跌，初始为0，跳过该中枢时flag取反，用于判断是否有连续走势

    buy_n = 0  # 用于记录标记了几个买点
    buy_2 = 0  # 用于记录一买与二买间的间隔笔端点
    old_buy = 0  # 记录上一个中枢的买点数

    sell_n = 0  # 用于记录标记了几个卖点
    sell_2 = 0  # 用于记录一卖与二卖间的间隔笔端点
    old_sell = 0  # 记录上一个中枢的卖点数
    while it_stroke < len(strokesList) and it_pivot < len(pivotList):  # 若笔或中枢列表其一遍历完，则停止循环
        # 若笔端点在中枢左侧，则判断下一个笔端点
        if strokesList[it_stroke][0] < pivotList[it_pivot][0]:
            it_stroke += 1
        # 若笔端点恰为中枢左端点，则笔端点跳至其后第三个端点（至少三笔构成中枢）
        elif strokesList[it_stroke][0] == pivotList[it_pivot][0]:
            if buy_n and buy_list[-1][0] != pivotList[it_pivot - 1][1]:  # 若上一个中枢存在买点，则当前中枢端点为三类买点
                buy_list.append([strokesList[it_stroke][0], strokesList[it_stroke][1], 'B3'])
                buy_n += 1
            if sell_n and sell_list[-1][0] != pivotList[it_pivot - 1][1]:  # 若上一个中枢存在卖点，则当前中枢端点为三类卖点
                sell_list.append([strokesList[it_stroke][0], strokesList[it_stroke][1], 'S3'])
                sell_n += 1

            old_buy = buy_n  # 记录上一个中枢的买点数
            buy_n = 0  # 用于记录标记了几个买点
            buy_2 = 0  # 用于记录一买与二买间的间隔笔端点

            old_sell = sell_n  # 记录上一个中枢的卖点数
            sell_n = 0  # 用于记录标记了几个卖点
            sell_2 = 0  # 用于记录一卖与二卖间的间隔笔端点

            it_stroke += 3  # 跳转至形成中枢后的一个笔端点（至少三笔才能构成中枢，也才能继续判断买卖点的产生情况）
        # 若笔端点在中枢内部或右端点，则判断是否为买卖点
        elif strokesList[it_stroke][0] <= pivotList[it_pivot][1]:
            # 卖点计算函数
            if (pivotList[it_pivot][2] + pivotList[it_pivot][3]) > (
                    pivotList[it_pivot - 1][2] + pivotList[it_pivot - 1][3]):  # 中枢上升，上涨走势
                if flag == -1:  # 出现连续上涨走势
                    update_buy_and_sell.append(strokesList[it_stroke])  # 判断更新卖点数据的关机键位置
                    # 删除原来记录的卖点（更新卖点前的操作）
                    t = -1
                    while old_sell:
                        # s = sell_list.pop()
                        s = sell_list[t]
                        t -= 1
                        sell_list_history.append(s)
                        old_sell -= 1
                # 先判断第一类卖点
                if sell_n == 0:
                    if strokesList[it_stroke][1] > pivotList[it_pivot][3]:
                        sell_list.append([strokesList[it_stroke][0], strokesList[it_stroke][1], 'S1'])
                        sell_n += 1
                # 再判断第二类卖点
                else:
                    if sell_2 == 0:  # 跳过第一类卖点的下一个端点才到第二个卖点
                        sell_2 = 1
                    elif sell_2 == 1:
                        sell_list.append([strokesList[it_stroke][0], strokesList[it_stroke][1], 'S2'])
                        sell_n += 1
                        sell_2 = 2
                flag = 1  # 在最后标记为上涨走势
            # 买点计算函数
            else:  # 中枢下降，下跌走势
                if flag == -2:  # 出现连续下跌走势
                    update_buy_and_sell.append(strokesList[it_stroke])  # 判断更新卖点数据的关机键位置
                    # 删除原来记录的买点（更新买点前的操作）
                    t = -1
                    while old_buy:
                        # b = buy_list.pop()
                        b = buy_list[t]
                        t -= 1
                        buy_list_history.append(b)
                        old_buy -= 1
                # 先判断第一类买点
                if buy_n == 0:
                    if strokesList[it_stroke][1] < pivotList[it_pivot][2]:
                        buy_list.append([strokesList[it_stroke][0], strokesList[it_stroke][1], 'B1'])
                        buy_n += 1
                # 再判断第二类买点
                else:
                    if buy_2 == 0:  # 跳过第一类买点的下一个端点才到第二个买点
                        buy_2 = 1
                    elif buy_2 == 1:
                        buy_list.append([strokesList[it_stroke][0], strokesList[it_stroke][1], 'B2'])
                        buy_n += 1
                        buy_2 = 2
                flag = 2  # 在最后标记为下跌走势
            it_stroke += 1
        # 若笔端点超过中枢右端点，则跳至下一中枢判断
        elif strokesList[it_stroke][0] > pivotList[it_pivot][1]:
            it_pivot += 1
            flag *= -1  # 将flag取反，若出现连续走势，则检测到flag=-1时更新卖点，检测到flag=-2时更新买点
