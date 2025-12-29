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
        # 零星平台配置
        'app_id': os.getenv('LINGXING_APP_ID', 'ak_89uM2PNqPSPFJ'),
        'app_secret': os.getenv('LINGXING_APP_SECRET', 'COQo5uhVIR8eAPTN3Vy/ig=='),

        # 数据库配置
        'db_config': {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', 'd15c76a0875e73c0'),
            'database': os.getenv('DB_NAME', 'lingxing_orders'),
            'port': int(os.getenv('DB_PORT', '3306')),
            'charset': os.getenv('DB_CHARSET', 'utf8mb4'),
            'connect_timeout': int(os.getenv('DB_CONNECT_TIMEOUT', '15')),
            'read_timeout': int(os.getenv('DB_READ_TIMEOUT', '45')),
            'write_timeout': int(os.getenv('DB_WRITE_TIMEOUT', '45'))
        },

        # 飞书配置
        'cancel_orders_config': {
            'APP_ID': os.getenv('FEISHU_APP_ID', 'cli_a9bc132c7af81bc7'),
            'APP_SECRET': os.getenv('FEISHU_APP_SECRET', '0xpxP8mp9Iu5kpymCGQ5FeAujRhAYAfB'),
            'APP_TOKEN': os.getenv('FEISHU_APP_TOKEN', 'VfzjbXYvwaXbUKs2ADKcwxL4no6'),
            'CANCEL_ORDERS_TABLE_ID': os.getenv('FEISHU_CANCEL_ORDERS_TABLE_ID', 'tblTbja1535i09YW')
        },
        'sales_summary_config': {
            'APP_ID': os.getenv('FEISHU_APP_ID', 'cli_a9bc132c7af81bc7'),
            'APP_SECRET': os.getenv('FEISHU_APP_SECRET', '0xpxP8mp9Iu5kpymCGQ5FeAujRhAYAfB'),
            'APP_TOKEN': os.getenv('FEISHU_APP_TOKEN', 'TTMLbFmFFaC4Vqs7CLMc0ER7n1f'),
            'CANCEL_ORDERS_TABLE_ID': os.getenv('FEISHU_CANCEL_ORDERS_TABLE_ID', 'tblRJsFNEVnpM677')
        },
        'warehouse_config': {
            'APP_ID': os.getenv('FEISHU_APP_ID', 'cli_a9bc132c7af81bc7'),
            'APP_SECRET': os.getenv('FEISHU_APP_SECRET', '0xpxP8mp9Iu5kpymCGQ5FeAujRhAYAfB'),
            'APP_TOKEN': os.getenv('FEISHU_APP_TOKEN', 'QB2wbNJwsa9q9ysz5KecvUfmnHc'),
            'WAREHOUSE_TABLE_ID': os.getenv('FEISHU_CANCEL_ORDERS_TABLE_ID', 'tbls99XRbM3FKgKs')
        },
        'inventory_config': {
            'APP_ID': os.getenv('FEISHU_APP_ID', 'cli_a9bc132c7af81bc7'),
            'APP_SECRET': os.getenv('FEISHU_APP_SECRET', '0xpxP8mp9Iu5kpymCGQ5FeAujRhAYAfB'),
            'APP_TOKEN': os.getenv('FEISHU_APP_TOKEN', 'Zoktbe2qWaXRwxsdhNlciFUunsf'),
            'INVENTORY_TABLE_ID': os.getenv('FEISHU_CANCEL_ORDERS_TABLE_ID', 'tblWuUVtBWRr6AKR')
        }
    }

    return config




