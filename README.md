# Proxy Node Crawler 

自动爬取GitHub上的代理节点配置，每5小时更新一次。

## 支持的协议

- ✅ **VLESS** (Reality/Xhttp/Vision/ENC)
- ✅ **NaiveProxy**
- ✅ **AnyTLS**
- ✅ **ShadowTLS**
- ✅ **Hysteria2**
- ✅ **TUIC**

## 功能特性

- 🔄 每5小时自动更新（通过GitHub Actions）
- 🔍 智能搜索GitHub仓库
- 📝 自动解析多种配置文件格式
- 📊 生成订阅链接和JSON格式输出
- 🛡️ 支持GitHub Token提高API限流

## 使用方法

### 自动运行
项目会自动每5小时运行一次，无需手动操作。

### 手动触发
1. 进入仓库的 Actions 标签
2. 选择 "Proxy Node Crawler" 工作流
3. 点击 "Run workflow" 按钮

### 本地运行
```bash
# 克隆仓库
git clone https://github.com/yourusername/proxy-node-crawler.git
cd proxy-node-crawler

# 安装依赖
pip install -r requirements.txt

# 设置环境变量（可选）
export GITHUB_TOKEN=your_github_token

# 运行爬虫
python -m crawler.main
