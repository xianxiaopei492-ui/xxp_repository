import os
import sys
# 添加当前目录到Python路径，确保可以导入您的模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def load_config_from_env():
    """
    从环境变量加载配置（生产环境推荐）
    Returns:
        dict: 配置字典
    """
    config = {
        'app_id': os.getenv('LINGXING_APP_ID', 'ak_89uM2PNqPSPFJ'),
        'app_secret': os.getenv('LINGXING_APP_SECRET', 'COQo5uhVIR8eAPTN3Vy/ig=='),
        'db_config': {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', 'd15c76a0875e73c0'),
            'database': os.getenv('DB_NAME', 'lingxing_orders'),
            'port': int(os.getenv('DB_PORT', '3306')),
            'connect_timeout': int(os.getenv('DB_CONNECT_TIMEOUT', '15')),
            'read_timeout': int(os.getenv('DB_READ_TIMEOUT', '45')),
            'write_timeout': int(os.getenv('DB_WRITE_TIMEOUT', '45'))
        },
        'feishu_config':{
            'APP_ID': "cli_a9bc132c7af81bc7",
            'APP_SECRET' :"0xpxP8mp9Iu5kpymCGQ5FeAujRhAYAfB"
        }
    }

    return config