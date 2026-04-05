# 行业深度研究工作流

这个模块把“行业报告生成”做成独立主线，而不是附着在文献监测脚本上的一个选项。

核心设计是 5 个智能体组成的 DAG：

- `Orchestrator`
  - 负责任务拆解、DAG 编排、运行记忆与输出汇总
- `Searcher`
  - 负责把一句话任务拆成关键词、结构化目标、非结构化目标和报告关注点
- `Collector`
  - 负责采集多源数据，当前支持：
    - 现有文献表 `literature_table.csv`
    - 本地文本资料
    - 本地结构化数据 `csv/xlsx`
    - `Baostock` A 股 K 线、盈利能力、杜邦分析
    - `Yahoo Finance` 港股、美股 K 线、估值快照与利润表摘要
    - `Akshare` 宏观指标、财务摘要、财务分析指标、个股新闻
    - Google News RSS 新闻检索
    - 国务院政策文件库官方接口检索
    - 东方财富股吧与 `Stocktwits` 社区讨论
- `Analyst`
  - 负责定性和定量分析、风险机会提炼、情景构建与图表生成
- `Aggregator`
  - 负责按 MECE、SWOT、金字塔方法生成专业级研究报告

## 当前特点

- 功能彼此独立，但共享同一个 `workflow_memory.json`
- 可以单独运行整条 DAG，也可以以后裁成更简版
- 报告是主输出，不再只是监测脚本的附属结果
- 当前环境没有 Docker，所以量化分析默认在本地 Python 环境中执行；金融数据与舆情接口保持可插拔
- 一旦你后面装好依赖并配置 API，这条链路可以直接升级
- 当前已实接：
  - `Baostock`：A 股行情与财务指标
  - `Yahoo Finance`：港股/美股行情、估值、利润表摘要
  - `Akshare`：宏观、财务摘要、个股新闻
  - `Google News RSS`：新闻舆情
  - `Gov.cn Policy Library`：政策动向
  - `Eastmoney Guba + Stocktwits`：社区讨论

## 裁剪工作流

如果你想跑最简版，可以在配置里关闭部分 agent，例如：

```json
{
  "workflow": {
    "enable_searcher": false,
    "enable_collector": true,
    "enable_analyst": true,
    "enable_aggregator": true
  }
}
```

这时工作流会直接使用你手工提供的 `keywords`、本地数据和文献库，形成 `Collector -> Analyst -> Aggregator` 的轻量链路。

## 运行方式

```bash
python3 /Users/jie/Desktop/editor/deep_research_workflow.py \
  --config /Users/jie/Desktop/editor/config/deep_research_workflow.example.json \
  --task "生成式AI在企业数字化转型中的行业影响研究" \
  --keywords "生成式AI,企业数字化转型,AI adoption,digital transformation" \
  --metrics "收入增速,利润率,研发投入,市场份额" \
  --output-name "genai_digital_transformation"
```

如果你需要股票或公司分析，也可以加：

```bash
python3 /Users/jie/Desktop/editor/deep_research_workflow.py \
  --config /Users/jie/Desktop/editor/config/deep_research_workflow.example.json \
  --task "比较宁德时代与同行在新能源电池行业的盈利与风险" \
  --symbols "300750,002594,002074" \
  --metrics "营收,净利率,ROE,市占率,研发费用率" \
  --keywords "新能源电池,盈利能力,风险,政策,舆情" \
  --output-name "battery_profitability_compare"
```

如果你要覆盖港股和美股，可以直接把 Yahoo 风格代码放进 `--symbols`：

```bash
python3 /Users/jie/Desktop/editor/deep_research_workflow.py \
  --config /Users/jie/Desktop/editor/config/deep_research_workflow.example.json \
  --task "比较腾讯、苹果和特斯拉在平台生态与资本市场表现上的差异" \
  --symbols "0700.HK,AAPL,TSLA" \
  --metrics "收盘价,区间涨跌幅,成交活跃度,市值,PE,ROE,净利率,营收,净利润" \
  --keywords "平台生态,AI,舆情,政策,社区讨论" \
  --output-name "hk_us_cross_market_compare"
```

## 主要输出

- `*_report.md`
- `*_payload.json`
- `charts/`
- `workflow_trace.jsonl`
- `workflow_memory.json`

## 当前边界

- `Baostock` 目前优先覆盖 A 股；港股和美股默认改走 `Yahoo Finance`
- 当前环境没有 Docker，所以量化分析默认在本地 Python 环境中执行
- `Akshare` 的部分历史行情接口在当前网络环境下会出现远端断开，所以当前默认用 `Baostock` 做 A 股行情主源，用 `Akshare` 补宏观、财务摘要与新闻
- 出于稳定性考虑，示例配置默认关闭了 `Akshare` 的个股财务摘要批量抓取；如果你确认网络环境稳定，可以把 `connectors.finance.enable_symbol_financials` 改成 `true`
- 港股/美股当前默认改走 `Yahoo Finance`，这样在当前网络环境下比 `Akshare` 历史行情接口更稳定
- 社区讨论当前以“东方财富股吧 + Stocktwits”的方式组织，因此报告已经能同时输出“新闻 + 政策 + 社区讨论”三层情绪
