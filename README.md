# lmem_backfill — 长期记忆历史回填

AstrBot 插件：把群聊历史消息批量喂给 [LivingMemory](https://github.com/lxfight-s-Astrbot-Plugins/astrbot_plugin_livingmemory)，让"失忆"的 bot 一次性补上过去的话题、人物和事件，召回测试立即可用。

## 适用场景

刚装 LivingMemory 时，长期记忆库是空的，bot 对群友聊过的内容毫无印象。本插件从两个数据源抓取历史消息，交给 LivingMemory 的总结管线写入记忆库：

1. **AstrBot 消息库**（`data_v4.db` 的 `platform_message_history` 表，主数据源）；
2. **NapCat `get_group_msg_history`**（上限 200 条，作为补充）。

> 注意：AstrBot 只记录 bot 首次运行之后的消息，NapCat 接口最多返回最近 200 条且忽略翻页参数，**bot 运行之前的群聊历史无法获取**。

## 使用

1. 上传安装后，在插件配置中按群名关键字设置 `group_keyword`；
2. 打开 `auto_start`（保存配置会触发热重载，即相当于远程触发）；
3. 回填完成后**关掉 `auto_start`**，避免每次重载都跑一遍；
4. 已回填过的群会跳过（状态记录在 `plugin_data/lmem_backfill/state.json`），如需重跑打开 `force_rerun`。

## 配置项

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `auto_start` | bool | false | 加载后自动回填一次 |
| `force_rerun` | bool | false | 忽略完成记录强制重跑 |
| `group_keyword` | string | - | 按群名关键字匹配目标群 |
| `count` | int | 500 | 回填消息条数上限 |
| `since` | string | - | 只处理该日期之后的消息（YYYY-MM-DD） |

## 实现要点

- 消息段（@/图片/表情/语音等）统一转纯文本，分块（20 条/块）送入 LivingMemory 总结；
- 模块级全局任务锁防止热重载时重复回填；
- 回填过程逐块落盘状态，中断后可续跑。

## 依赖

- AstrBot ≥ 4.29，已安装并启用 LivingMemory 插件
- 平台适配器需为 aiocqhttp（NapCat），否则仅有 AstrBot 消息库一个数据源
