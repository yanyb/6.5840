本项目是MIT 6.5840（原6.824）分布式系统课程的实验实现，完成了Lab1到Lab4的实验内容。

## 📋 完成情况

| 实验 | 状态 | 说明 |
|------|------|------|
| Lab1: MapReduce | ✅ 已完成 |
| Lab2: Key/Value Server | ✅ 已完成 |
| Lab3: Raft | ✅ 已完成  |
| Lab4: Fault-tolerant Key/Value Service | ✅ 已完成 |
| Lab5: Sharded Key/Value Service | 🔄 进行中 |


## ⚠️ 当前问题：Lab5 - 网络问题与配置变更的区分
### 问题描述
难以准确区分以下两种情况：
1. **网络问题**：RPC调用失败或超时必须重试，如果读的旧配置会死循环
2. **分片配置变更**：当leave group,关闭了group
