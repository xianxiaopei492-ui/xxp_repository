"""
订单状态每日自动更新脚本
功能：每天定时从零星平台API获取最新订单状态，更新
新增功能：每日更新库存、仓库、店铺信息
"""
import os
import sys
import logging
import time
from datetime import datetime, timedelta
from dataoperator import DataOperator
from config import load_config_from_env
from api_use import LingXingAPI
import  traceback
import  json
import  pymysql
from utils import extract_store_name, extract_from_json
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
    def update_store_info(self):
        """
        更新店铺信息表
        Returns:
            bool: 更新是否成功
        """
        try:
            logger.info("开始更新店铺信息...")
            # 调用店铺信息API
            success = self.api_client.getstoreList(self.db_config)
            if success:
                logger.info("✅ 店铺信息更新成功")
            else:
                logger.error("❌ 店铺信息更新失败")
            return success
        except Exception as e:
            logger.error(f"更新店铺信息失败: {e}")
            return False
    def update_warehouse_info(self):
        """
        更新仓库信息表
        Returns:
            bool: 更新是否成功
        """
        try:
            logger.info("开始更新仓库信息...")
            # 调用仓库信息API
            self.api_client.getwarehouseList(self.db_config, type=3)
            logger.info("✅ 仓库信息更新完成")
            return True
        except Exception as e:
            logger.error(f"更新仓库信息失败: {e}")
            return False
    def update_inventory_info(self):
        """
        更新库存信息表
        Returns:
            bool: 更新是否成功
        """
        try:
            logger.info("开始更新库存信息...")
            # 获取所有仓库ID
            warehouse_ids = self.api_client.getwarehouseids(self.db_config)
            if not warehouse_ids:
                logger.warning("未获取到仓库ID，跳过库存更新")
                return False
            # 将仓库ID列表转换为逗号分隔的字符串
            wid_str = ",".join([str(wid) for wid in warehouse_ids])
            logger.info(f"获取到 {len(warehouse_ids)} 个仓库，开始更新库存...")
            # 调用库存信息API
            success = self.api_client.getinvetoryList(self.db_config, str=wid_str)
            if success:
                logger.info("✅ 库存信息更新成功")
            else:
                logger.error("❌ 库存信息更新失败")
            return success
        except Exception as e:
            logger.error(f"更新库存信息失败: {e}")
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
    def _update_daily_sales(self, days_back=7, enable_cleanup=False):
        """
        内部方法：更新每日销量数据（不包含数据库连接管理）
        Args:
            days_back: 获取最近多少天的数据
            enable_cleanup: 是否启用数据清理
        Returns:
            bool: 更新是否成功
        """
        try:
            overall_success = True
            # 更新销量统计数据（按不同维度分别更新）
            update_tasks = [
                {"name": "SKU维度销量", "data_type": "4"},
                {"name": "店铺维度销量", "data_type": "6"},
                {"name": "ASIN维度销量", "data_type": "1"}
            ]
            for task in update_tasks:
                logger.info(f"开始更新 {task['name']} 数据...")
                task_success = self.update_sales_statistics(
                    days_back=days_back,
                    result_type="1",  # 销量
                    date_unit="4",  # 按日统计
                    data_type=task['data_type']
                )
                if not task_success:
                    logger.warning(f"{task['name']} 更新失败，但继续执行其他任务")
                    overall_success = False
                else:
                    logger.info(f"✅ {task['name']} 更新完成")
                # 任务间短暂延迟，避免API限流
                time.sleep(2)
            # 可选：清理旧数据
            if enable_cleanup:
                cleanup_success = self.cleanup_old_sales_data(days_to_keep=90)
                if not cleanup_success:
                    logger.warning("销量数据清理失败")
            return overall_success
        except Exception as e:
            logger.error(f"销量数据更新失败: {e}")
            return False
    def _generate_update_report(self, task_results, execution_time, overall_success):
        """
        生成更新任务报告
        Args:
            task_results: 各任务执行结果字典
            execution_time: 总执行时间
            overall_success: 整体是否成功
        """
        logger.info("=" * 60)
        logger.info("每日数据更新任务报告")
        logger.info("=" * 60)
        success_count = 0
        total_count = 0
        for task_name, result in task_results.items():
            total_count += 1
            status_icon = "✅" if result is True else "⚠️" if result == "跳过" else "❌"
            status_text = "成功" if result is True else "跳过" if result == "跳过" else "失败"
            logger.info(f"{status_icon} {task_name}: {status_text}")
            if result is True:
                success_count += 1
        success_rate = (success_count / total_count) * 100 if total_count > 0 else 0
        logger.info("-" * 40)
        logger.info(f"任务完成情况: {success_count}/{total_count} ({success_rate:.1f}%)")
        logger.info(f"总执行时间: {execution_time:.2f} 秒")
        if overall_success:
            logger.info("🎉 所有关键任务执行成功")
        else:
            logger.info("⚠️  部分任务执行失败，但非关键任务不影响整体流程")
        logger.info("=" * 60)
    def update_sales_statistics(self, days_back=30, result_type="1", date_unit="4", data_type="4", sids=None):
        """
        更新销量统计数据
        Args:
            days_back: 获取最近多少天的数据（默认30天）
            result_type: 汇总类型 1销量 2订单量 3销售额
            date_unit: 统计时间指标 1年 2月 3周 4日
            data_type: 统计数据维度 1ASIN 2父体 3MSKU 4SKU 5SPU 6店铺
            sids: 店铺ID列表，多个使用英文逗号分隔
        Returns:
            bool: 更新是否成功
        """
        try:
            # 计算日期范围（确保不超过90天限制）
            days_back = min(days_back, 90)  # API限制最大90天
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days_back)
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            print(f"开始更新销量统计数据，时间范围: {start_date_str} 到 {end_date_str}")
            print(f"统计参数 - 汇总类型: {result_type}, 时间单位: {date_unit}, 数据维度: {data_type}")
            if sids:
                print(f"指定店铺ID: {sids}")
            # 调用销量数据获取方法
            success = self.api_client.get_sales_by_date_range(
                db_config=self.db_config,
                start_date=start_date_str,
                end_date=end_date_str,
                result_type=result_type,
                date_unit=date_unit,
                data_type=data_type,
                sids=sids
            )
            if success:
                print("销量统计数据更新成功")
                # 记录更新统计信息
                self._log_sales_update_summary(start_date_str, end_date_str)
            else:
                print("销量统计数据更新失败")
            return success
        except Exception as e:
            print(f"更新销量统计数据失败: {e}")
            print(f"详细错误信息: {traceback.format_exc()}")
            return False
    def _log_sales_update_summary(self, start_date, end_date):
        """
        记录销量数据更新摘要信息（适配sales_code）
        """
        try:
            if not self.data_operator or not self.data_operator.conn:
                self.connect_database()
            # 查询本次更新的数据统计
            summary_sql = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT sales_code) as sales_code,
                SUM(volume_total) as total_volume,
                AVG(volume_total) as avg_volume,
                MIN(create_time) as earliest_record,
                MAX(create_time) as latest_record
            FROM sales_info 
            WHERE create_time >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
            AND create_time <= NOW()
            """
            self.data_operator.cursor.execute(summary_sql)
            result = self.data_operator.cursor.fetchone()
            if result and result[0] > 0:
                print(f"销量更新摘要 - 时间段: {start_date} 至 {end_date}")
                print(f"  新增记录数: {result[0]} 条")
                print(f"  唯一SKU数量: {result[1]} 个")
                print(f"  总销量/销售额: {result[2]:.2f}")
                print(f"  平均销量/销售额: {result[3]:.2f}")
                print(f"  最早记录时间: {result[4]}")
                print(f"  最晚记录时间: {result[5]}")
            else:
                print("未找到本次更新的销量记录")
        except Exception as e:
            print(f"生成销量更新摘要失败: {e}")
    def cleanup_old_sales_data(self, days_to_keep=90):
        """
        清理旧的销量数据
        """
        try:
            if not self.data_operator or not self.data_operator.conn:
                self.connect_database()
            # 先统计要删除的数据量
            count_sql = "SELECT COUNT(*) FROM sales_info WHERE create_time < DATE_SUB(NOW(), INTERVAL %s DAY)"
            self.data_operator.cursor.execute(count_sql, (days_to_keep,))
            count_result = self.data_operator.cursor.fetchone()
            if count_result and count_result[0] > 0:
                print(f"准备清理 {count_result[0]} 条 {days_to_keep} 天前的销量数据")
                # 执行删除
                delete_sql = "DELETE FROM sales_info WHERE create_time < DATE_SUB(NOW(), INTERVAL %s DAY)"
                self.data_operator.cursor.execute(delete_sql, (days_to_keep,))
                deleted_rows = self.data_operator.cursor.rowcount
                self.data_operator.conn.commit()
                print(f"成功清理 {deleted_rows} 条旧销量数据")
                # 优化表空间
                optimize_sql = "OPTIMIZE TABLE sales_info"
                self.data_operator.cursor.execute(optimize_sql)
                print("表空间优化完成")
                return True
            else:
                print(f"没有需要清理的旧销量数据（保留 {days_to_keep} 天）")
                return True
        except Exception as e:
            print(f"清理旧销量数据失败: {e}")
            if self.data_operator.conn:
                self.data_operator.conn.rollback()
            return False
    def rebuild_orders_merge_table(self):
        """
        重建订单合并宽表 orders_merge
        将订单、物流、商品和店铺信息联合为一个宽表供前端展示
        """
        try:
            print("开始重建订单合并宽表...")
            if not self.data_operator or not self.data_operator.conn:
                self.connect_database()
            # 重建合并表的SQL语句
            sql = """
            DROP TABLE IF EXISTS orders_merge;
            CREATE TABLE orders_merge AS
            SELECT 
                o.global_order_no,
                o.reference_no,
                o.store_id,
                o.order_from_name,
                o.delivery_type,
                o.split_type,
                o.order_status,
                -- 将时间字段改为DATETIME类型
                CASE 
                    WHEN o.global_purchase_time IS NOT NULL AND o.global_purchase_time != 0 
                    THEN FROM_UNIXTIME(o.global_purchase_time, '%Y-%m-%d %H:%i:%s')
                    ELSE NULL 
                END AS global_purchase_time,
                
                CASE 
                    WHEN o.global_payment_time IS NOT NULL AND o.global_payment_time != 0 
                    THEN FROM_UNIXTIME(o.global_payment_time, '%Y-%m-%d %H:%i:%s')
                    ELSE NULL 
                END AS global_payment_time,
                
                CASE 
                    WHEN o.global_review_time IS NOT NULL AND o.global_review_time != 0 
                    THEN FROM_UNIXTIME(o.global_review_time, '%Y-%m-%d %H:%i:%s')
                    ELSE NULL 
                END AS global_review_time,
                
                CASE 
                    WHEN o.global_distribution_time IS NOT NULL AND o.global_distribution_time != 0 
                    THEN FROM_UNIXTIME(o.global_distribution_time, '%Y-%m-%d %H:%i:%s')
                    ELSE NULL 
                END AS global_distribution_time,
                
                CASE 
                    WHEN o.global_print_time IS NOT NULL AND o.global_print_time != 0 
                    THEN FROM_UNIXTIME(o.global_print_time, '%Y-%m-%d %H:%i:%s')
                    ELSE NULL 
                END AS global_print_time,
                
                CASE 
                    WHEN o.global_mark_time IS NOT NULL AND o.global_mark_time != 0 
                    THEN FROM_UNIXTIME(o.global_mark_time, '%Y-%m-%d %H:%i:%s')
                    ELSE NULL 
                END AS global_mark_time,
                
                CASE 
                    WHEN o.global_delivery_time IS NOT NULL AND o.global_delivery_time != 0 
                    THEN FROM_UNIXTIME(o.global_delivery_time, '%Y-%m-%d %H:%i:%s')
                    ELSE NULL 
                END AS global_delivery_time,
                
                o.amount_currency,
                
                CASE 
                    WHEN o.global_latest_ship_time IS NOT NULL AND o.global_latest_ship_time != 0 
                    THEN FROM_UNIXTIME(o.global_latest_ship_time, '%Y-%m-%d %H:%i:%s')
                    ELSE NULL 
                END AS global_latest_ship_time,
                
                CASE 
                    WHEN o.global_cancel_time IS NOT NULL AND o.global_cancel_time != 0 
                    THEN FROM_UNIXTIME(o.global_cancel_time, '%Y-%m-%d %H:%i:%s')
                    ELSE NULL 
                END AS global_cancel_time,
                
                CASE 
                    WHEN o.update_time IS NOT NULL AND o.update_time != 0 
                    THEN FROM_UNIXTIME(o.update_time, '%Y-%m-%d %H:%i:%s')
                    ELSE NULL 
                END AS update_time,
                
                o.order_tag,
                o.pending_order_tag,
                o.exception_order_tag,
                o.wid,
                o.warehouse_name,
                o.original_global_order_no,
                o.supplier_id,
                o.is_delete,
                o.order_custom_fields,
                
                CASE 
                    WHEN o.global_create_time IS NOT NULL AND o.global_create_time != 0 
                    THEN FROM_UNIXTIME(o.global_create_time, '%Y-%m-%d %H:%i:%s')
                    ELSE NULL 
                END AS global_create_time,
                
                l.logistics_type_id,
                l.logistics_type_name,
                l.logistics_provider_id,
                l.logistics_provider_name,
                l.actual_carrier,
                l.waybill_no,
                l.pre_weight,
                l.pre_fee_weight,
                l.pre_fee_weight_unit,
                l.pre_pkg_length,
                l.pre_pkg_height,
                l.pre_pkg_width,
                l.weight,
                l.pkg_fee_weight,
                l.pkg_fee_weight_unit,
                l.pkg_length,
                l.pkg_width,
                l.pkg_height,
                l.weight_unit,
                l.pkg_size_unit,
                l.cost_currency_code,
                
                CASE 
                    WHEN l.pre_cost_amount IS NOT NULL
                    THEN CAST(REPLACE(REPLACE(l.pre_cost_amount, '-￥', ''), '￥', '') AS DECIMAL(10,2))
                    ELSE NULL 
                END AS pre_cost_amount,
                
                l.cost_amount,
                
                CASE 
                    WHEN l.logistics_time IS NOT NULL AND l.logistics_time != 0 
                    THEN FROM_UNIXTIME(l.logistics_time, '%Y-%m-%d %H:%i:%s')
                    ELSE NULL 
                END AS logistics_time,
                
                l.tracking_no,
                l.mark_no,
                i.global_item_no,
                i.item_id,
                i.platform_order_no,
                i.order_item_no,
                i.item_from_name,
                i.msku,
                i.local_sku,
                i.product_no,
                i.local_product_name,
                i.is_bundled,
                i.title,
                i.variant_attr,
                i.unit_price_amount,
                i.item_price_amount,
                i.quantity,
                i.platform_status,
                i.item_type,
                i.stock_cost_amount,
                i.wms_outbound_cost_amount,
                i.stock_deduct_id,
                i.stock_deduct_name,
                i.cg_price_amount,
                i.shipping_amount,
                i.wms_shipping_price_amount,
                i.customer_shipping_amount,
                i.discount_amount,
                i.customer_tip_amount,
                i.tax_amount,
                i.sales_revenue_amount,
                i.transaction_fee_amount,
                i.other_amount,
                i.customized_url,
                i.platform_subsidy_amount,
                i.cod_amount,
                i.gift_wrap_amount,
                i.platform_tax_amount,
                i.points_granted_amount,
                i.other_fee,
                
                CASE 
                    WHEN i.delivery_time IS NOT NULL AND i.delivery_time != 0 
                    THEN FROM_UNIXTIME(i.delivery_time, '%Y-%m-%d %H:%i:%s')
                    ELSE NULL 
                END AS delivery_time,
                
                i.source_name,
                i.data_json,
                i.item_custom_fields,
                s.sid AS store_sid,
                s.store_name AS store_full_name,
                s.platform_code AS store_platform_code,
                s.platform_name AS store_platform_name,
                s.currency AS store_currency,
                s.is_sync AS store_is_sync,
                s.status AS store_status,
                s.country_code AS store_country_code
            FROM orders o
            LEFT JOIN logistics_info l ON o.global_order_no = l.global_order_no
            LEFT JOIN item_info i ON o.global_order_no = i.global_order_no
            LEFT JOIN store_info s ON o.store_id = s.store_id;
            ALTER TABLE orders_merge ADD PRIMARY KEY (global_item_no);
            -- 添加索引以提高查询性能
            CREATE INDEX idx_orders_merge_global_item_no ON orders_merge(global_item_no);
            CREATE INDEX idx_orders_merge_global_order_no ON orders_merge(global_order_no);
            CREATE INDEX idx_orders_merge_store_id ON orders_merge(store_id);
            """
            # 执行SQL语句
            statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
            for statement in statements:
                try:
                    self.data_operator.cursor.execute(statement)
                    print(f"执行SQL成功: {statement[:100]}...")
                except Exception as e:
                    print(f"执行SQL失败: {e}")
                    print(f"失败语句: {statement}")
                    # 继续执行其他语句，不中断整个流程
            self.data_operator.conn.commit()
            print("订单合并宽表重建成功")
            return True
        except Exception as e:
            print(f"重建订单合并宽表失败: {e}")
            if self.data_operator.conn:
                self.data_operator.conn.rollback()
            return False

    def rebuild_sales_summary_daily(self):
        """
        重建销量汇总表 sales_summary_daily - 兼容MySQL 5.7版本
        """
        try:
            print("开始重建销量汇总表...")

            if not self.data_operator or not self.data_operator.conn:
                self.connect_database()

            # 确保使用字典游标
            if not hasattr(self.data_operator.cursor, 'description') or not self.data_operator.cursor.description:
                # 重新创建字典游标
                self.data_operator.cursor.close()
                self.data_operator.cursor = self.data_operator.conn.cursor(pymysql.cursors.DictCursor)

            # 分步骤执行的SQL语句 - 兼容MySQL 5.7
            sql_steps = [
                # 1. 创建表
                """
                CREATE TABLE IF NOT EXISTS sales_summary_daily (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    sku VARCHAR(255) NOT NULL,
                    store_name VARCHAR(255),
                    platform_name VARCHAR(255),
                    recent_3d_sales DECIMAL(15,2) DEFAULT 0,
                    recent_7d_sales DECIMAL(15,2) DEFAULT 0,
                    recent_15d_sales DECIMAL(15,2) DEFAULT 0,
                    recent_30d_sales DECIMAL(15,2) DEFAULT 0,
                    total_sales DECIMAL(15,2) DEFAULT 0,
                    last_sale_date DATE,
                    summary_date DATE NOT NULL,
                    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_sku_store_date (sku, store_name, summary_date)
                )
                """,

                # 2. 清空表
                "TRUNCATE TABLE sales_summary_daily",

                # 3. 创建临时表存储解析后的数据
                """
                CREATE TEMPORARY TABLE temp_sales_data (
                    sales_id INT,
                    sku VARCHAR(255),
                    store_name VARCHAR(255),
                    platform_name VARCHAR(255),
                    sale_date DATE,
                    daily_sales DECIMAL(15,2),
                    volume_total DECIMAL(15,2)
                )
                """
            ]

            # 执行前三个步骤
            for i, statement in enumerate(sql_steps, 1):
                try:
                    self.data_operator.cursor.execute(statement)
                    print(f"✅ 步骤{i}执行成功")
                except Exception as e:
                    print(f"❌ 步骤{i}执行失败: {e}")
                    if i == 3:  # 临时表创建失败，可能是已存在
                        try:
                            self.data_operator.cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_sales_data")
                            self.data_operator.cursor.execute(statement)
                            print(f"✅ 重新创建临时表成功")
                        except Exception as e2:
                            print(f"❌ 重新创建临时表失败: {e2}")

            # 4. 使用Python处理JSON数据（兼容MySQL 5.7的方法）
            try:
                # 获取所有需要处理的数据
                select_sql = """
                SELECT 
                    sales_id,
                    sku,
                    store_name,
                    platform_name,
                    date_collect,
                    volume_total
                FROM sales_info 
                WHERE date_collect IS NOT NULL 
                AND date_collect != '{}'
                AND date_collect != ''
                """

                self.data_operator.cursor.execute(select_sql)
                rows = self.data_operator.cursor.fetchall()

                summary_date = datetime.now().date()
                processed_count = 0

                print(f"🔍 找到 {len(rows)} 条需要处理的销售记录")

                for row in rows:
                    try:
                        # 安全地获取字段值 - 处理元组和字典两种情况
                        if isinstance(row, dict):
                            sales_id = row.get('sales_id')
                            sku_data = row.get('sku')
                            store_data = row.get('store_name')
                            platform_data = row.get('platform_name')
                            date_collect_str = row.get('date_collect')
                            volume_total = row.get('volume_total')
                        else:
                            # 如果是元组，按顺序获取
                            sales_id = row[0] if len(row) > 0 else None
                            sku_data = row[1] if len(row) > 1 else None
                            store_data = row[2] if len(row) > 2 else None
                            platform_data = row[3] if len(row) > 3 else None
                            date_collect_str = row[4] if len(row) > 4 else None
                            volume_total = row[5] if len(row) > 5 else None

                        if not date_collect_str:
                            continue

                        # 处理JSON字符串
                        try:
                            if isinstance(date_collect_str, str):
                                # 确保是有效的JSON
                                date_collect_str = date_collect_str.replace("'", '"')
                                date_collect = json.loads(date_collect_str)
                            else:
                                date_collect = date_collect_str
                        except (json.JSONDecodeError, TypeError) as e:
                            print(f"❌ JSON解析失败: {e}, 数据: {str(date_collect_str)[:100]}")
                            continue

                        if not isinstance(date_collect, dict):
                            continue

                        # 处理每个日期-销量对
                        for date_str, sales_str in date_collect.items():
                            try:
                                # 转换日期
                                sale_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                                sales_value = float(sales_str) if sales_str else 0

                                # 提取SKU和店铺信息
                                sku = extract_from_json(sku_data, sales_id, 'SKU')
                                store_name = extract_store_name(extract_from_json(store_data, '未知店铺', '店铺'))
                                platform_name = extract_from_json(platform_data, '未知平台', '平台')

                                # 插入临时表
                                insert_temp_sql = """
                                INSERT INTO temp_sales_data 
                                (sales_id, sku, store_name, platform_name, sale_date, daily_sales, volume_total)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """

                                self.data_operator.cursor.execute(insert_temp_sql, (
                                    sales_id, sku, store_name, platform_name,
                                    sale_date, sales_value, float(volume_total or 0)
                                ))
                                processed_count += 1

                            except Exception as e:
                                print(f"❌ 处理日期 {date_str} 失败: {e}")
                                continue

                    except Exception as e:
                        # 安全地获取sales_id用于错误报告
                        sales_id_val = '未知'
                        try:
                            if isinstance(row, dict):
                                sales_id_val = row.get('sales_id', '未知')
                            else:
                                sales_id_val = row[0] if len(row) > 0 else '未知'
                        except:
                            pass

                        print(f"❌ 处理记录 {sales_id_val} 失败: {e}")
                        continue

                print(f"✅ 成功解析 {processed_count} 条日期销售记录")

                # 5. 从临时表汇总数据并插入到目标表
                summary_sql = """
                INSERT INTO sales_summary_daily 
                (sku, store_name, platform_name, recent_3d_sales, recent_7d_sales, 
                 recent_15d_sales, recent_30d_sales, total_sales, last_sale_date, summary_date)
                SELECT 
                    sku,
                    store_name,
                    platform_name,
                    COALESCE(SUM(CASE WHEN sale_date >= CURDATE() - INTERVAL 3 DAY THEN daily_sales ELSE 0 END), 0) as recent_3d_sales,
                    COALESCE(SUM(CASE WHEN sale_date >= CURDATE() - INTERVAL 7 DAY THEN daily_sales ELSE 0 END), 0) as recent_7d_sales,
                    COALESCE(SUM(CASE WHEN sale_date >= CURDATE() - INTERVAL 15 DAY THEN daily_sales ELSE 0 END), 0) as recent_15d_sales,
                    COALESCE(SUM(CASE WHEN sale_date >= CURDATE() - INTERVAL 30 DAY THEN daily_sales ELSE 0 END), 0) as recent_30d_sales,
                    MAX(volume_total) as total_sales,
                    MAX(sale_date) as last_sale_date,
                    CURDATE() as summary_date
                FROM temp_sales_data
                WHERE sale_date >= CURDATE() - INTERVAL 30 DAY
                GROUP BY sku, store_name, platform_name
                HAVING recent_30d_sales > 0
                """

                self.data_operator.cursor.execute(summary_sql)
                inserted_rows = self.data_operator.cursor.rowcount
                print(f"✅ 成功汇总并插入 {inserted_rows} 条记录到销量汇总表")

                # 6. 清理临时表
                self.data_operator.cursor.execute("DROP TEMPORARY TABLE IF EXISTS temp_sales_data")

                self.data_operator.conn.commit()

            except Exception as e:
                print(f"❌ 数据处理失败: {e}")
                import traceback
                print(f"详细错误: {traceback.format_exc()}")
                self.data_operator.conn.rollback()
                return False

            print("✅ 销量汇总表重建成功")
            return True

        except Exception as e:
            print(f"❌❌ 重建销量汇总表失败: {e}")
            import traceback
            print(f"详细错误: {traceback.format_exc()}")
            if self.data_operator.conn:
                self.data_operator.conn.rollback()
            return False




    def run_daily_update(self, days_to_check=1, enable_cleanup=False,
                         update_orders=True, update_inventory=True, update_warehouse=True,
                         update_store=True, update_sales=True, sales_days_back=7,
                         rebuild_merge_table=True, rebuild_sales_summary=True):
        """
        执行每日更新任务（整合销量数据更新）
        Args:
            days_to_check: 检查最近多少天的订单
            enable_cleanup: 是否启用数据清理
            update_orders: 是否更新订单数据
            update_inventory: 是否更新库存信息
            update_warehouse: 是否更新仓库信息
            update_store: 是否更新店铺信息
            update_sales: 是否更新销量数据
            sales_days_back: 销量数据回溯天数
            rebuild_merge_table: 是否重建订单合并宽表
            rebuild_sales_summary: 是否重建销量汇总表
        Returns:
            bool: 任务执行是否成功
        """
        logger.info("=" * 60)
        logger.info("开始执行每日数据更新任务")
        logger.info("=" * 60)
        # 打印当前配置
        logger.info("📋 任务配置:")
        logger.info(f"  更新订单数据: {update_orders}")
        logger.info(f"  更新库存信息: {update_inventory}")
        logger.info(f"  更新仓库信息: {update_warehouse}")
        logger.info(f"  更新店铺信息: {update_store}")
        logger.info(f"  更新销量数据: {update_sales}")
        logger.info(f"  重建合并宽表: {rebuild_merge_table}")
        logger.info(f"  重建销量汇总: {rebuild_sales_summary}")
        logger.info(f"  数据清理: {enable_cleanup}")
        start_time = time.time()
        overall_success = True
        task_results = {}
        try:
            # 1. 连接数据库
            if not self.connect_database():
                return False
            # 2. 获取并更新订单数据（新增参数控制）
            if update_orders:
                logger.info("开始更新订单数据...")
                order_success = self.fetch_updated_orders(days_to_check)
                task_results["订单数据"] = order_success
                if not order_success:
                    logger.error("订单数据更新失败")
                    overall_success = False
                else:
                    logger.info("✅ 订单数据更新成功")
            else:
                logger.info("跳过订单数据更新")
                task_results["订单数据"] = "跳过"
            # 3. 更新仓库信息
            if update_warehouse:
                logger.info("开始更新仓库信息...")
                warehouse_success = self.update_warehouse_info()
                task_results["仓库信息"] = warehouse_success
                if not warehouse_success:
                    logger.warning("仓库信息更新失败，但继续执行其他任务")
                    overall_success = overall_success and False  # 非关键任务，不强制失败
                else:
                    logger.info("✅ 仓库信息更新成功")
            else:
                logger.info("跳过仓库信息更新")
                task_results["仓库信息"] = "跳过"
            # 4. 更新店铺信息
            if update_store:
                logger.info("开始更新店铺信息...")
                store_success = self.update_store_info()
                task_results["店铺信息"] = store_success
                if not store_success:
                    logger.warning("店铺信息更新失败，但继续执行其他任务")
                    overall_success = overall_success and False
                else:
                    logger.info("✅ 店铺信息更新成功")
            else:
                logger.info("跳过店铺信息更新")
                task_results["店铺信息"] = "跳过"
            # 5. 更新库存信息（需要先有仓库信息）
            if update_inventory:
                logger.info("开始更新库存信息...")
                inventory_success = self.update_inventory_info()
                task_results["库存信息"] = inventory_success
                if not inventory_success:
                    logger.warning("库存信息更新失败，但继续执行其他任务")
                    overall_success = overall_success and False
                else:
                    logger.info("✅ 库存信息更新成功")
            else:
                logger.info("跳过库存信息更新")
                task_results["库存信息"] = "跳过"
            # 6. 更新销量数据
            if update_sales:
                logger.info("开始更新销量数据...")
                sales_success = self._update_daily_sales(sales_days_back, enable_cleanup)
                task_results["销量数据"] = sales_success
                if not sales_success:
                    logger.warning("销量数据更新失败，但继续执行其他任务")
                    overall_success = overall_success and False
                else:
                    logger.info("✅ 销量数据更新成功")
            else:
                logger.info("跳过销量数据更新")
                task_results["销量数据"] = "跳过"
            # 7. 重建订单合并宽表（供前端展示）
            if rebuild_merge_table:
                logger.info("开始重建订单合并宽表...")
                merge_success = self.rebuild_orders_merge_table()
                task_results["订单合并宽表"] = merge_success
                if not merge_success:
                    logger.warning("订单合并宽表重建失败，但继续执行其他任务")
                    overall_success = overall_success and False
                else:
                    logger.info("✅ 订单合并宽表重建成功")
            else:
                logger.info("跳过订单合并宽表重建")
                task_results["订单合并宽表"] = "跳过"
            # 8. 重建销量汇总表（新增功能）
            if rebuild_sales_summary:
                logger.info("开始重建销量汇总表...")
                sales_summary_success = self.rebuild_sales_summary_daily()
                task_results["销量汇总表"] = sales_summary_success
                if not sales_summary_success:
                    logger.warning("销量汇总表重建失败，但继续执行其他任务")
                    overall_success = overall_success and False
                else:
                    logger.info("✅ 销量汇总表重建成功")
            else:
                logger.info("跳过销量汇总表重建")
                task_results["销量汇总表"] = "跳过"
            # 9. 验证数据一致性（可选）
            logger.info("开始验证数据一致性...")
            consistency_success = self.validate_order_status_consistency()
            task_results["数据一致性"] = consistency_success
            if not consistency_success:
                logger.warning("数据一致性验证失败")
                overall_success = overall_success and False
            else:
                logger.info("✅ 数据一致性验证完成")
            # 10. 清理旧数据
            if enable_cleanup:
                logger.info("开始清理旧数据...")
                cleanup_success = self.cleanup_old_data()
                task_results["数据清理"] = cleanup_success
                if not cleanup_success:
                    logger.warning("数据清理失败")
                    overall_success = overall_success and False
                else:
                    logger.info("✅ 数据清理完成")
            else:
                logger.info("跳过数据清理")
                task_results["数据清理"] = "跳过"
            # 11. 计算执行时间并生成报告
            execution_time = time.time() - start_time
            self._generate_update_report(task_results, execution_time, overall_success)
        except Exception as e:
            logger.error(f"每日更新任务执行失败: {e}")
            overall_success = False
            logger.error(f"详细错误: {traceback.format_exc()}")
        finally:
            # 确保数据库连接被关闭
            self.disconnect_database()
        return overall_success
def main():
    try:
        logger.info("=" * 60)
        logger.info("每日数据更新系统启动")
        logger.info("=" * 60)
        # 加载配置
        config = load_config_from_env()
        # 创建更新器实例
        updater = DailyOrderUpdater(
            app_id=config['app_id'],
            app_secret=config['app_secret'],
            db_config=config['db_config']
        )
        # 执行整合后的每日更新任务
        success = updater.run_daily_update(
            days_to_check=1,  # 检查最近1天的订单
            enable_cleanup=False,  # 是否启用数据清理
            update_orders=False,  # 更新订单信息
            update_inventory=False,  # 更新库存信息
            update_warehouse=False,  # 更新仓库信息
            update_store=False,  # 更新店铺信息
            update_sales=True,  # 更新销量数据
            sales_days_back=7,  # 销量数据回溯7天
            rebuild_merge_table=False,  # 重建订单合并宽表
            rebuild_sales_summary=True  # 新增：重建销量汇总表
        )
        if success:
            logger.info("✅ 每日数据更新任务执行成功")
            sys.exit(0)
        else:
            logger.warning("⚠️ 部分数据更新任务执行失败")
            sys.exit(1)
    except Exception as e:
        logger.error(f"💥 系统执行出现未预期错误: {e}")
        logger.error(f"详细错误: {traceback.format_exc()}")
        sys.exit(1)
if __name__ == "__main__":
    main()
