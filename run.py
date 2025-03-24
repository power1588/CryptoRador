#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
CryptoRador - 加密货币异常行情监测工具
入口脚本，用于从项目根目录直接启动程序
"""

import os
import sys

# 将项目根目录添加到Python路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# 导入主程序
from src.main import main

if __name__ == "__main__":
    # 启动程序
    main() 