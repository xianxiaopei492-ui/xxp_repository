
"""
订单状态每日自动更新脚本
功能：每天定时从零星平台API获取最新订单状态，更新orders表和platform_info表
"""

import os
import sys
import logging
import time
from datetime import datetime, timedelta
from dataoperator import DataOperator
from config import  load_config_from_env
from main import LingXingAPI  

# 添加当前目录到Python路径，确保可以导入您的模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 配置日志系统
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/var/log/daily_order_update.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DailyOrderUpdater:
    """每日订单状态更新器"""

    def __init__(self, app_id, app_secret, db_config):
        """
        初始化更新器

        Args:
            app_id: 零星平台APP_ID
            app_secret: 零星平台APP_SECRET
            db_config: 数据库连接配置
        """
       

        self.api_client = LingXingAPI(app_id, app_secret)
        self.db_config = db_config
        self.data_operator = None

    def connect_database(self):
        """连接数据库"""
        try:
            self.data_operator = DataOperator(self.db_config)
            self.data_operator.connect_db()
            logger.info("数据库连接成功")
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False

    def disconnect_database(self):
        """断开数据库连接"""
        if self.data_operator:
            self.data_operator.disconnect_db()
        logger.info("数据库连接已关闭")

    def get_yesterday_time_range(self):
        """
        获取昨天的时间范围（用于查询昨天更新的订单）

        Returns:
            tuple: (start_time, end_time) 时间戳
        """
        # 获取昨天日期
        yesterday = datetime.now() - timedelta(days=1)
        start_time = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)
        end_time = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)

        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp())

        logger.info(f"查询时间范围: {start_time} 到 {end_time}")
        return start_timestamp, end_timestamp

    def get_recent_days_time_range(self, days=1):
        """
        获取最近N天的时间范围

        Args:
            days: 查询最近多少天

        Returns:
            tuple: (start_time, end_time) 时间戳
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)

        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp())

        logger.info(f"查询最近{days}天时间范围: {start_time} 到 {end_time}")
        return start_timestamp, end_timestamp

    def fetch_updated_orders(self, days_to_check=1):
        """
        获取需要更新的订单数据
        Args:
            days_to_check: 检查最近多少天的订单
        Returns:
            list: 订单数据列表
        """
        try:
            # 获取时间范围
            start_time, end_time = self.get_recent_days_time_range(days_to_check)

            # 构建API请求参数
            api_path = "/pb/mp/order/v2/list"
            base_biz_body = {
                "start_time": start_time,
                "end_time": end_time,
                "date_type": "update_time",  # 按更新时间查询
                "platform_code": [10024],  # 指定平台
            }

            logger.info("开始获取订单更新数据...")
            total_processed = self.api_client.fetch_and_process_order_data_batch(
                api_path, base_biz_body, self.db_config, delay=1
            )

            logger.info(f"订单数据获取完成，共处理 {total_processed} 条记录")
            return total_processed > 0

        except Exception as e:
            logger.error(f"获取订单数据失败: {e}")
            return False

    def validate_order_status_consistency(self):
        """
        验证订单状态一致性（可选功能）
        确保orders表和platform_info表的order_status字段一致
        """
        try:
            if not self.data_operator or not self.data_operator.conn:
                logger.warning("数据库未连接，跳过一致性验证")
                return True

            # 检查两个表的订单状态是否一致
            check_sql = """
            SELECT 
                COUNT(*) as total_orders,
                SUM(CASE WHEN o.order_status = p.order_status THEN 1 ELSE 0 END) as matching_orders
            FROM orders o
            INNER JOIN platform_info p ON o.global_order_no = p.global_order_no
            WHERE o.update_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            """

            self.data_operator.cursor.execute(check_sql)
            result = self.data_operator.cursor.fetchone()
            if result and result[0] > 0:
                consistency_rate = (result[1] / result[0]) * 100
                logger.info(f"订单状态一致性检查: {consistency_rate:.2f}% ({result[1]}/{result[0]})")
                if consistency_rate < 95:
                    logger.warning("订单状态一致性较低，建议检查数据同步逻辑")

            return True

        except Exception as e:
            logger.error(f"订单状态一致性检查失败: {e}")
            return False

    def cleanup_old_data(self, days_to_keep=90):
        """
        清理旧数据（可选功能）

        Args:
            days_to_keep: 保留多少天的数据
        """
        try:
            if not self.data_operator or not self.data_operator.conn:
                return False

            # 清理90天前的订单数据（根据业务需求调整）
            cleanup_sql = """
            DELETE FROM orders 
            WHERE update_time < DATE_SUB(NOW(), INTERVAL %s DAY)
            AND order_status IN ('TRADE_FINISHED', 'TRADE_CLOSED')
            """

            self.data_operator.cursor.execute(cleanup_sql, (days_to_keep,))
            deleted_rows = self.data_operator.cursor.rowcount

            if deleted_rows > 0:
                logger.info(f"清理了 {deleted_rows} 条 {days_to_keep} 天前的已完成/已关闭订单")
                self.data_operator.conn.commit()

            return True

        except Exception as e:
            logger.error(f"数据清理失败: {e}")
            if self.data_operator.conn:
                self.data_operator.conn.rollback()
            return False

    def run_daily_update(self, days_to_check=1, enable_cleanup=False):
        """
        执行每日更新任务

        Args:
            days_to_check: 检查最近多少天的订单
            enable_cleanup: 是否启用数据清理

        Returns:
            bool: 任务执行是否成功
        """
        logger.info("=" * 60)
        logger.info("开始执行每日订单状态更新任务")
        logger.info("=" * 60)

        start_time = time.time()
        success = False

        try:
            # 1. 连接数据库
            if not self.connect_database():
                return False

            # 2. 获取并更新订单数据
            update_success = self.fetch_updated_orders(days_to_check)

            if not update_success:
                logger.error("订单数据更新失败")
                return False

            # 3. 验证数据一致性（可选）
            self.validate_order_status_consistency()

            # # 4. 清理旧数据（可选）
            # if enable_cleanup:
            #     self.cleanup_old_data(days_to_keep=90)

            # 5. 计算执行时间
            execution_time = time.time() - start_time
            logger.info(f"每日订单更新任务执行完成，耗时: {execution_time:.2f} 秒")

            success = True

        except Exception as e:
            logger.error(f"每日更新任务执行失败: {e}")
            success = False

        finally:
            # 确保数据库连接被关闭
            self.disconnect_database()

        return success





def main():
    """主函数"""
    try:
        logger.info("=" * 50)
        logger.info("每日订单状态更新系统启动")
        logger.info("=" * 50)

        # 加载配置
        config = load_config_from_env()

        # 创建更新器实例
        updater = DailyOrderUpdater(
            app_id=config['app_id'],
            app_secret=config['app_secret'],
            db_config=config['db_config']
        )

        # 执行每日更新（检查最近1天的订单，启用数据清理）
        success = updater.run_daily_update(
            days_to_check=1 # 检查最近1天的订单
            # enable_cleanup=True  # 启用数据清理
        )

        if success:
            logger.info("✅ 每日订单更新任务执行成功")
            sys.exit(0)  # 成功退出码
        else:
            logger.error("❌ 每日订单更新任务执行失败")
            sys.exit(1)  # 失败退出码

    except Exception as e:
        logger.error(f"💥 系统执行出现未预期错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()