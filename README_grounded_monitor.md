# 扎根文献每日监测

这个小工具面向“像马鸿佳、蔡莉那样先系统搜文献，再做编码、提炼主题框架”的写作流程。它的目标不是替代人工判断，而是先把每天新增文献、候选编码和缺口提醒整理好，帮你把文献追踪和初步编码自动化。

现在它已经吸收了三类设计：

- 来自 `paper_search`：多源检索抽象、下载历史、可选翻译思路
- 来自 `ARIS`：本地论文库优先、多源文献工作流、研究型 skill 组织方式
- 来自 `infiAgent`：运行状态文件、历史记忆、断点续跑式设计
- 来自 `ai-agent-deep-dive`：最小 agent loop、skills 自动发现、显式步骤化执行
- 来自 `ms-agent`：skills 选择、长短期记忆、上下文压缩、agent 轨迹化运行
- 新增：基于文献库的问答能力与一键行业报告能力，可选接 OpenAI 兼容 API

## 它现在能做什么

- 按你设定的主题词每天增量检索本地文献库、OpenAlex、arXiv、Semantic Scholar
- 自动抽取题目、作者、年份、期刊、主题、摘要等元数据
- 根据全文或摘要做一轮规则化的扎根式编码
- 按前因、结果、机制、边界条件、未来研究方向、研究对象、研究方法、理论基础输出表格
- 自动生成开放编码明细、主轴关系链、命题草案和编码可信度
- 和你的论文或 `covered_topics` 对比，不只提醒“你还没写到的新主题”，还会提醒“你还没写到的新变量关系链”
- 输出 `csv`、`xlsx` 和一份 Markdown 日报
- 记录 `run_state.json`、`run_history.jsonl`、`search_history.jsonl`、`download_history.json`
- 维护 `theme_memory.json`，把历次运行出现过的高频主题沉淀下来
- 发现 `skill_dirs` 下的 `SKILL.md`，按查询相关性挑选外部 skills
- 输出 `agent_trace.jsonl`，把 agent 每一步真正做了什么写下来
- 输出 `agent_memory.json` 和 `compact_context.md`，便于长周期继续运行
- 可选接入 OpenAI 兼容翻译接口，生成中文标题和摘要
- 支持 `--ask` 基于现有文献与 PDF 片段回答问题
- 支持 `--generate-report` 一键输出行业报告 Markdown
- 支持 `--skip-monitor` 跳过重新检索，直接使用已有文献表回答问题或生成报告

## 文件说明

- 主脚本：`/Users/jie/Desktop/editor/grounded_daily_monitor.py`
- 示例配置：`/Users/jie/Desktop/editor/config/grounded_monitor.example.json`
- 冒烟配置：`/Users/jie/Desktop/editor/config/grounded_monitor.smoke.json`
- 提示词草案：`/Users/jie/Desktop/editor/prompts/grounded_monitor_prompts.md`

## 使用步骤

1. 复制并修改配置文件。
2. 把 `queries` 改成你的研究主题关键词。
3. 把 `baseline_paths` 改成你的论文文件路径，支持 `pdf`、`txt`、`md`，若本地装了 `python-docx` 也可读 `docx`。
4. 把 `sources` 改成你要启用的来源，支持 `local`、`openalex`、`arxiv`、`semantic_scholar`。
5. 把 `local_library_paths` 改成你的本地论文库目录，比如 Zotero 导出的 PDF 文件夹。
6. 如果还没准备好论文全文，先在 `covered_topics` 里填你已经写过的主题。
7. 如果你想要中文翻译，打开 `translation.enabled`，再配置翻译接口。
8. 运行命令：

```bash
python3 /Users/jie/Desktop/editor/grounded_daily_monitor.py \
  --config /Users/jie/Desktop/editor/config/grounded_monitor.example.json
```

## 问答与行业报告

如果你想直接基于已有文献库回答问题：

```bash
python3 /Users/jie/Desktop/editor/grounded_daily_monitor.py \
  --config /Users/jie/Desktop/editor/config/grounded_monitor.example.json \
  --skip-monitor \
  --ask "当前创业即兴行为研究中最常见的前因和边界条件是什么？"
```

如果你想一键生成行业报告：

```bash
python3 /Users/jie/Desktop/editor/grounded_daily_monitor.py \
  --config /Users/jie/Desktop/editor/config/grounded_monitor.example.json \
  --skip-monitor \
  --generate-report "生成式AI在创业研究与创新管理中的应用"
```

如果你已经在 `assistant` 里配置了 OpenAI 兼容接口，这两种模式会自动调用 API 生成更完整的回答或报告；如果没有配置，也会退化成基于规则和现有编码结果的本地版输出。

## 输出位置

默认输出到配置里的 `outdir`，主要文件有：

- `literature_table.csv`
- `literature_table.xlsx`
  - `literature_table` 工作表：总表
  - `open_coding` 工作表：逐条开放编码证据
  - `axial_matrix` 工作表：前因-机制-结果-边界条件关系链与命题草案
- `daily_report_YYYY-MM-DD.md`
- `builtin_skills_and_prompts.json`
- `run_state.json`
- `run_history.jsonl`
- `search_history.jsonl`
- `download_history.json`
- `translation_cache.json`
- `theme_memory.json`
- `agent_trace.jsonl`
- `agent_memory.json`
- `compact_context.md`
- `qa_answers/`
- `industry_reports/`
- `pdfs/`

## 当前边界

- 现在的编码是“规则化初筛 + 句级开放编码 + 主轴关系链归纳”，已经更接近论文工作流，但还不是最终版的人类扎根编码。
- 如果你后面接入 OpenAI API，我们可以把 `prompts/grounded_monitor_prompts.md` 里的提示词升级成更强的自动编码器。
- “你的论文未覆盖主题”这件事，在你提供论文全文后会明显更准。
- 对“新增变量关系链”的提醒已经加进去了，但如果你的论文全文没有放进 `baseline_paths`，它仍然只能做近似比较。
