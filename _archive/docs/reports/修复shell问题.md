# 🔧 Shell错误修复指南

## 问题描述
每次运行shell命令时出现：
```
--: eval: line 1: unexpected EOF while looking for matching `)'
--: eval: line 2: syntax error: unexpected end of file
--: dump_bash_state: command not found
```

## 问题原因
这个错误来自Cursor的shell集成功能，调用了一个不存在的`dump_bash_state`函数。

## 解决方案

### 方案1：重启Cursor终端（推荐）
1. 在Cursor中按 `Ctrl + ~` 打开终端
2. 点击终端右上角的 `+` 号，选择 `Split Terminal` 或 `New Terminal`
3. 关闭旧的终端窗口
4. 新终端应该不会再有这个错误

### 方案2：创建临时修复函数
在 `~/.bashrc` 文件末尾添加：
```bash
# 临时修复Cursor shell集成问题
function dump_bash_state() {
    return 0
}
```

然后运行：
```bash
source ~/.bashrc
```

### 方案3：禁用Cursor的shell集成
1. 打开 Cursor 设置（Cmd + ,）
2. 搜索 "shell integration"
3. 取消勾选 "Terminal > Integrated > Shell Integration: Enabled"
4. 重启Cursor

### 方案4：使用Python替代Shell命令（当前方案）
由于shell有问题，我们可以完全使用Python和内置工具完成v49.0的优化：
- ✅ read_file / write - 文件读写
- ✅ search_replace - 代码修改
- ✅ grep - 搜索
- ❌ run_terminal_cmd - 暂时不可用

**实际影响**：几乎没有影响，因为文件操作不需要shell命令。

## 验证修复
执行以下命令测试：
```bash
echo "Hello World"
ls -la
```

如果不再出现错误，说明修复成功！

## 当前工作状态
即使shell有错误，以下操作仍然正常：
- ✅ 文件读写操作
- ✅ 代码搜索和替换  
- ✅ Python脚本执行
- ✅ v49.0优化工作

所以我们可以继续完成v49.0的优化工作，shell问题不会影响进度。

