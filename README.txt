德州扑克 v3.9 Redis 持久化版

基于 v3.8 网络稳定版增量升级，保持原有游戏规则、桌面/手机布局不变。

v3.9 新增：
1. 接入 Upstash Redis（@upstash/redis），用于持久化房间状态。
2. Render/Node.js 服务重启后，可从 Redis 恢复房间、玩家座位、筹码、牌局、公共牌、盲注、当前行动等状态。
3. 玩家仍使用原有 token 自动恢复座位；服务器重启后旧 socket ID 会失效，但不会因此生成新身份。
4. 断线自动重连、Wi-Fi/流量切换、切后台恢复等 v3.8 机制全部保留。
5. 结果展示、自动发公共牌、边池、大小盲、庄位轮转等原有规则全部保留。
6. 未配置 Redis 环境变量时仍可启动，自动退回纯内存模式；配置后才启用持久化。

Render 环境变量：
UPSTASH_REDIS_REST_URL
UPSTASH_REDIS_REST_TOKEN

注意：Token 只放 Render Environment，不要写入 index.html、GitHub 或 ZIP。

Upstash Free 计划适合当前朋友之间的小规模游戏；具体额度以 Upstash 当前控制台为准。
