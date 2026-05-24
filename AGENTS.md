# AGENTS.md

C++17 Redis proxy，CMake，依赖 libco/cpp_util。除用户要求，不改 thirdparty/submodule。

边界：src/、include/redis_proxy/ 放代理业务（server/worker/session/backend/RESP/buffer/config/rules）；util/ 只放通用基础设施（endpoint/socket_utils/fd_notifier），不得掺业务。保持 redis_proxy 命名空间、include 风格和类边界。

运行：ProxyServer 监听分发 fd；Worker 管线程/协程/backend pool；ClientSession 解析/校验/提交批次/回写；BackendPool/Channel 管连接、队列、回复和失败。

命令：安全优先，显式白名单，未知默认拒绝。新增允许命令只改 CommandRules::Default()，写清 argc/read/write，并补 command_rules 测试。危险/阻塞/事务/PubSub/脚本/认证/管理/模块/集群复制类默认拒绝；不改为黑名单/外部配置，除非更新设计并确认。

改动小而聚焦；协议、缓冲区、调度、命令校验等共享行为必须补测试。勿重构无关模块，勿回滚用户改动。

验证：cmake -S . -B build；cmake --build build -j；cd build && ctest --output-on-failure；或 make test。性能改动跑 bench。
