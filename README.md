# gohangout-input-pulsar

依赖 github.com/apache/pulsar-client-go/pulsar 写的https://github.com/childe/gohangout input 插件 
## 配置
```
inputs:
    - '/mnt/c/work/gohangout/pulsar_input.so':
        codec: json
        serviceUrl: "pulsar://dasds:6650"
        topic: "persistent://dasdasd/dasd/dnlog"
        subscriptionName: "zcola"

outputs:
    - Stdout: {}
```
# gohangout-input-pulsar
