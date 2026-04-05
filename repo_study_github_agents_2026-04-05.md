# GitHub 仓库学习笔记

日期：2026-04-05

## 目标

对 3 个 GitHub 仓库做源码级学习，并判断哪些设计适合迁移到“每日自动搜文献 + 扎根编码 + 缺口提醒”的论文软件中。

## 仓库与版本

- `DabinSheng/paper_search`
  - 本地路径：`/tmp/paper_search`
  - 提交：`c4cf3d8b2752ae3f1e88f8fc0f9e41fea8972d35`
- `polyuiislab/infiAgent`
  - 本地路径：`/tmp/infiAgent`
  - 提交：`ac97e4661597720d94d03a6c93e4cd2299bf29c0`
- `wanshuiyin/Auto-claude-code-research-in-sleep`
  - 本地路径：`/tmp/aris`
  - 提交：`37be42e0301a71638a28e55446bc8998abda792f`

## 一、paper_search 学到了什么

### 定位

这是一个偏“单机可用”的学术检索与下载工具，重点在：

- 多源搜文献
- 中英翻译
- 下载去重
- 前端可视化操作

### 核心结构

- `app.py`
  - Streamlit 前端
  - 搜索、翻译、勾选下载、历史记录展示都在这里组织
- `search_engines.py`
  - 把 ArXiv、OpenReview、Google Scholar 封装成统一搜索接口
  - `Paper` 数据对象很清晰，适合直接复用成我们的标准文献记录结构
- `qwen_client.py`
  - 用 Qwen 做标题/摘要翻译
- `download_history.py` / `search_history.py`
  - 做下载去重和历史追踪

### 最值得借鉴的点

- “统一 `Paper` 数据结构”这个思路非常适合我们
- 搜索源的抽象层比较清楚，便于以后接更多源
- 下载历史与已下载标记，适合直接纳入我们的增量抓取体系
- 前端交互思路很实用，后面如果你要 GUI，可以参考它的工作流

### 不适合直接照搬的点

- 主要是“搜索 + 翻译 + 下载”，没有真正进入文献编码和主题归纳
- Google Scholar 依赖 Selenium，长期自动化不够稳
- 没有“与我的论文对比，看缺什么”的分析层

### 对你项目的直接启发

- 我们可以吸收它的：
  - 多源检索抽象
  - 去重下载
  - 搜索历史
  - 双语展示
- 但必须自己补：
  - 扎根理论编码层
  - 主题缺口检测
  - 每日自动运行
  - 文献总表和日报

## 二、infiAgent 学到了什么

### 定位

这是一个“长时间运行、多层级智能体框架”，重点不是某个具体论文任务，而是：

- 长周期任务执行
- 断点恢复
- 多层级 agent 架构
- skill 标准化加载
- 持久化记忆与任务历史检索

### 核心结构

- `config/agent_library/Researcher/`
  - 研究型 agent 的配置中心
  - 用 YAML 定义不同层级 agent、工具和提示词
- `utils/skill_loader.py`
  - 扫描 skills 目录
  - 解析 `SKILL.md`
  - 生成可注入 prompt 的 skill 描述
- `infiagent/sdk.py`
  - 用 SDK 方式管理 runtime、workspace、skills、hooks、resume 等能力
- `task_history_search`
  - 把任务历史索引化，必要时再取回旧上下文

### 最值得借鉴的点

- “skill 作为独立目录 + `SKILL.md` frontmatter”的组织方式非常值得学
- Resume / 断点恢复 很适合我们的“每天自动监测”系统
- 任务历史检索很适合解决“每天都跑，但不想把历史全塞进 prompt”这个问题
- Researcher 配置说明它天然适合做长期研究任务，而不是一次性问答

### 不适合直接照搬的点

- 框架很重，接入成本高
- 对你当前项目而言，完整多层 agent 系统有点超配
- 你现在更需要“稳定文献流水线”，而不是先搭一个巨型 agent runtime

### 对你项目的直接启发

- 借鉴它的：
  - skill 管理方式
  - 断点恢复
  - 历史任务索引
  - 分层配置思路
- 暂时不建议整套迁移

## 三、ARIS 学到了什么

### 定位

这是一个“研究工作流方法库”，不是重框架，而是一套围绕论文研究的 skill 集合。

它最强的地方在于把复杂研究流程拆成很多可组合 skill：

- `research-lit`
- `idea-discovery`
- `novelty-check`
- `research-pipeline`
- `auto-review-loop`
- `paper-writing`
- `rebuttal`

### 核心结构

- `skills/`
  - 每个 skill 一个 `SKILL.md`
  - 强调可组合、可迁移、可换模型
- `tools/arxiv_fetch.py`
  - 直接查 arXiv
- `tools/semantic_scholar_fetch.py`
  - 查 Semantic Scholar
- `skills/research-lit/SKILL.md`
  - 已经把“本地文献库 + Zotero + Obsidian + Web + Semantic Scholar”串起来
- `skills/auto-review-loop/SKILL.md`
  - 做多轮评审改进循环
- `skills/research-pipeline/SKILL.md`
  - 把 idea、实验、写作、评审串成总流水线

### 最值得借鉴的点

- 它非常适合“论文工作流”而不是普通 agent
- `research-lit` 这条 skill 对你现在最有价值
  - 先查本地论文
  - 再查 Zotero / Obsidian
  - 再查外网
  - 做去重和文献表
- `auto-review-loop` 对你后续“论文写完后自动审查逻辑漏洞”很有帮助
- “用 skill 驱动流程，而不是把逻辑都写死在代码里”这点很适合你

### 不适合直接照搬的点

- 它更偏机器学习论文写作全流程
- 包含实验、GPU、rebuttal、LaTeX 等大量你当前阶段还用不上的部分
- 你现在更关心“文献检索 + 编码 + 补方向”，不需要整条 submission pipeline

### 对你项目的直接启发

- 直接借鉴 `research-lit` 的数据源分层策略
- 借鉴 `auto-review-loop` 的“多轮迭代提醒”思想
- 借鉴 skill 化组织，把你的“扎根编码”也变成独立 skill

## 四、三者对比

### 最适合你当前项目的部分

- `paper_search`
  - 适合拿来补“搜索源 + 下载 + 去重 + UI”
- `infiAgent`
  - 适合拿来补“长任务、断点恢复、skill 加载、历史检索”
- `ARIS`
  - 适合拿来补“研究型 workflow 和 skill 设计”

### 如果只选一个最值得深学

- 当前阶段最值得深学的是 `ARIS`

原因：

- 它离“论文研究助手”最近
- skill 切分方式最适合你的目标
- 它不是单纯搜文献，而是围绕研究流程组织能力

### 如果要组合使用

建议组合路线：

1. 用 `paper_search` 的检索与下载抽象做底层数据采集
2. 用 `ARIS` 的 `research-lit` 风格做文献工作流编排
3. 用 `infiAgent` 的 resume / task-history 思想做长期稳定运行

## 五、对你的软件的下一步建议

建议把你的系统拆成 5 个模块：

1. `collector`
   - 多源检索：OpenAlex、arXiv、Semantic Scholar、Crossref、Google Scholar
2. `library`
   - 本地论文库、去重、增量同步、下载历史
3. `coder`
   - 开放编码、聚焦编码、选择编码
4. `gap_detector`
   - 用你的论文全文或主题清单做对比，发现新主题和漏项
5. `runner`
   - 每日调度、断点恢复、日报生成

## 六、我建议我们真正吸收的内容

优先吸收：

- `paper_search` 的文献记录模型和下载历史
- `ARIS` 的 `research-lit` / skill 化组织方式
- `infiAgent` 的 resume 与历史任务索引思想

暂缓吸收：

- `infiAgent` 整套多层 agent runtime
- `ARIS` 的 GPU 实验和 rebuttal 流程
- `paper_search` 的 Scholar Selenium 方案

## 七、结论

这三个仓库里，对你最有价值的不是某个单独仓库，而是三者组合后的方法：

- `paper_search` 给你“搜”
- `ARIS` 给你“研究流程”
- `infiAgent` 给你“长期稳定运行”

你的项目如果继续往前做，最正确的方向不是把它做成单纯的“搜文献工具”，而是做成一个：

“面向扎根理论文献研究的长期运行型研究助手”

它每天自动发现新文献，自动形成编码候选、更新文献表、指出你论文还没覆盖的主题，并把这些变化沉淀成连续研究记忆。
