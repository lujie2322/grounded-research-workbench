# Grounded Research Workbench

![Grounded Research Workbench](assets/readme-hero.svg)

一个把“扎根理论文献编码”和“行业深度研究报告”放进同一套研究工作台里的仓库。

这个项目适合两类工作同时推进：

- 学术研究：每天追踪新文献，抽取研究假设、理论命题、变量角色与未来研究方向，形成可继续写论文的编码表。
- 行业研究：把财务、行情、新闻、政策、社区讨论接到同一条工作流里，生成结构化行业报告。

## 为什么做这个仓库

很多研究流程都卡在同一个问题上：搜集、整理、编码、比较、写作被切成了很多零散动作。这个仓库把这些动作重新组织成两条可复用的主线：

1. `grounded_daily_monitor.py`
   面向扎根理论和文献综述写作，重点是“文献追踪 + 编码 + 缺口提醒”。
2. `deep_research_workflow.py`
   面向行业报告和公司研究，重点是“多源采集 + 定性定量分析 + 报告生成”。

## 你可以得到什么

- 每日新增文献表：题目、作者、年份、摘要、主题、来源一张表整理好。
- 扎根编码结果：研究假设、理论命题、自变量、中介/调节变量、因变量、控制变量、未来研究方向与初级编码。
- 研究缺口提醒：识别你论文里还没覆盖的新主题、新变量关系链、新命题方向。
- 专业报告输出：自动组织成 MECE、SWOT、金字塔结构的行业研究报告。
- 可追溯流程：运行状态、搜索历史、下载历史、记忆文件、agent trace 都会保留下来。

## 仓库结构

![Workflow Overview](assets/readme-workflows.svg)

```text
.
├── grounded_daily_monitor.py
├── deep_research_workflow.py
├── deep_research/
│   ├── connectors.py
│   ├── workflow.py
│   ├── models.py
│   ├── memory.py
│   └── llm.py
├── config/
│   ├── grounded_monitor.example.json
│   └── deep_research_workflow.example.json
├── prompts/
│   └── grounded_monitor_prompts.md
├── scripts/
└── README_grounded_monitor.md
```

## 两条核心主线

### 1. 扎根文献监测

适合“先系统梳理已有文献，再从研究假设/命题和未来研究方向中做编码”的论文写法。

当前已经支持：

- 多源检索：本地文献库、OpenAlex、arXiv、Semantic Scholar
- 字段抽取：标题、作者、年份、期刊、摘要、主题、来源
- 扎根式编码：
  - `hypotheses_propositions`
  - `independent_vars`
  - `mediator_moderator_vars`
  - `dependent_vars`
  - `control_vars`
  - `future_research_directions`
  - `future_direction_codes`
- 关系提炼：
  - `open_code_details`
  - `axial_relations`
  - `selective_proposition`
  - `novel_relations`
  - `gap_focus`
- 输出：
  - `literature_table.csv`
  - `literature_table.xlsx`
  - `daily_report_YYYY-MM-DD.md`

快速运行：

```bash
python3 grounded_daily_monitor.py \
  --config config/grounded_monitor.example.json
```

如果你想直接基于已有文献回答问题：

```bash
python3 grounded_daily_monitor.py \
  --config config/grounded_monitor.example.json \
  --skip-monitor \
  --ask "当前创业即兴行为研究中最常见的前因和边界条件是什么？"
```

详细说明见 [README_grounded_monitor.md](README_grounded_monitor.md)。

### 2. 行业深度研究工作流

适合一句话触发多源研究，最后落成一份完整、多章节、可追溯的研究报告。

当前已经支持：

- 五智能体 DAG：`Orchestrator / Searcher / Collector / Analyst / Aggregator`
- 数据源：
  - `Baostock` A 股行情与财务指标
  - `Yahoo Finance` 港股/美股行情与利润表摘要
  - `Akshare` 宏观、财务摘要、个股新闻
  - `Google 新闻 RSS`
  - `国务院政策库`
  - `东方财富股吧`
  - `Stocktwits`
- 报告能力：
  - 财务比对
  - 新闻舆情
  - 政策动向
  - 社区讨论
  - 一致性校验
  - 风险矩阵
  - 重点证据

快速运行：

```bash
python3 deep_research_workflow.py \
  --config config/deep_research_workflow.example.json \
  --task "比较腾讯、苹果和特斯拉在平台生态与资本市场表现上的差异" \
  --symbols "0700.HK,AAPL,TSLA" \
  --metrics "收盘价,区间涨跌幅,成交活跃度,市值,PE,ROE,净利率,营收,净利润" \
  --keywords "平台生态,AI,舆情,政策,社区讨论" \
  --output-name "hk_us_cross_market_compare"
```

详细说明见 [README_deep_research_workflow.md](README_deep_research_workflow.md)。

## 一张图看清工作流

```mermaid
flowchart LR
    A["Research Question"] --> B["Searcher"]
    B --> C["Collector"]
    C --> D["Structured Data"]
    C --> E["Unstructured Text"]
    D --> F["Analyst"]
    E --> F["Analyst"]
    F --> G["Aggregator"]
    G --> H["Research Report"]
    C --> I["Grounded Coding"]
    I --> J["Theme / Gap Alerts"]
```

## 典型输出

- 扎根监测输出
  - `literature_table.csv`
  - `literature_table.xlsx`
  - `daily_report_YYYY-MM-DD.md`
  - `theme_memory.json`
  - `agent_trace.jsonl`
- 深度研究输出
  - `*_report.md`
  - `*_payload.json`
  - `charts/`
  - `workflow_trace.jsonl`
  - `workflow_memory.json`

## 适用场景

- 想模仿“先系统搜文献，再从变量和未来研究方向做编码”的论文写法。
- 需要长期跟踪某个研究主题，持续发现新增文献和研究缺口。
- 想把“文献研究”和“行业报告”放在同一套可复用代码里。
- 需要一个可以继续扩展 API、skills、agent 和数据源的研究底座。

## 下一步建议

- 把你自己的论文文件填进 `baseline_paths`，让缺口提醒真正对齐你的论文内容。
- 配置 OpenAI 兼容 API，让问答、翻译、报告和自动编码更完整。
- 按你的研究方向继续扩展专用提示词、变量词典和行业模板。

## 相关文件

- 主文献监测脚本：[grounded_daily_monitor.py](grounded_daily_monitor.py)
- 深度研究入口：[deep_research_workflow.py](deep_research_workflow.py)
- 扎根监测文档：[README_grounded_monitor.md](README_grounded_monitor.md)
- 行业研究文档：[README_deep_research_workflow.md](README_deep_research_workflow.md)
- 扎根提示词：[prompts/grounded_monitor_prompts.md](prompts/grounded_monitor_prompts.md)
