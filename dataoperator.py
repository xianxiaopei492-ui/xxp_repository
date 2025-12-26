import pymysql
import json
import hashlib


class DataOperator:
    def __init__(self, db_config):
        """
        初始化数据库连接配置
        db_config: 字典，包含数据库连接信息
        """
        self.db_config = db_config
        self.conn = None
        self.cursor = None

    def connect_db(self):
        """连接数据库（增加超时控制）"""
        try:
            # 从配置中获取超时参数，若未设置则使用合理默认值
            connect_timeout = self.db_config.get('connect_timeout', 10)  # 连接超时默认10秒
            read_timeout = self.db_config.get('read_timeout', 30)  # 读取超时默认30秒
            write_timeout = self.db_config.get('write_timeout', 30)  # 写入超时默认30秒

            self.conn = pymysql.connect(
                host=self.db_config['host'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['database'],
                port=self.db_config.get('port', 3306),
                charset=self.db_config.get('charset', 'utf8mb4'),
                # 新增的超时参数
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                write_timeout=write_timeout
            )
            self.cursor = self.conn.cursor()
            print("数据库连接成功")
        except Exception as e:
            print(f"数据库连接失败: {e}")
            raise

    def disconnect_db(self):
        """断开数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("数据库连接已关闭")

    def serialize_value(self, value):
        """
        序列化值以确保数据库兼容性[1,6](@ref)
        处理字典、列表等复杂类型，转换为JSON字符串
        """
        if value is None:
            return None
        elif isinstance(value, (dict, list)):
            # 字典/列表序列化为JSON字符串[1,6](@ref)
            try:
                return json.dumps(value, ensure_ascii=False, separators=(',', ':'))
            except Exception as e:
                print(f"JSON序列化错误: {e}, 值: {value}")
                return str(value)
        elif isinstance(value, (int, float, str, bool)):
            # 基本类型直接返回
            return value
        else:
            # 其他类型转换为字符串
            return str(value)

    def insert_orders(self, order_list):
        """
        批量插入订单数据到各个表
        order_list: API返回的订单列表
        """
        if not self.conn:
            self.connect_db()

        try:
            # 遍历每个订单进行处理
            for order_data in order_list:
                self._process_single_order(order_data)

            # 提交所有事务
            self.conn.commit()
            print(f"成功插入 {len(order_list)} 个订单的完整数据")

        except Exception as e:
            self.conn.rollback()
            print(f"数据插入失败，已回滚: {e}")
            raise

    def insert_stores_table(self, store_list):
        """
        批量插入店铺数据到各个表
        order_list: API返回的订单列表
        """
        if not self.conn:
            self.connect_db()

        try:
            # 遍历每个订单进行处理
            for store_data in store_list:
                self._process_stores(store_data)
            # 提交所有事务
            self.conn.commit()
            print(f"成功插入 {len(store_list)} 个订单的完整数据")

        except Exception as e:
            self.conn.rollback()
            print(f"数据插入失败，已回滚: {e}")
            raise

    def _process_stores(self, store_data):
        """
            插入或更新店铺信息到 store_info 表
            使用 ON DUPLICATE KEY UPDATE 实现存在时更新，不存在时插入
            """
        sql = """
            INSERT INTO store_info (
                store_id, sid, store_name, platform_code, platform_name, 
                currency, is_sync, status, country_code
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                sid = VALUES(sid),
                store_name = VALUES(store_name),
                platform_code = VALUES(platform_code),
                platform_name = VALUES(platform_name),
                currency = VALUES(currency),
                is_sync = VALUES(is_sync),
                status = VALUES(status),
                country_code = values (country_code)
            """

        # 准备要插入的数据值元组
        values = (
            self.serialize_value(store_data['store_id']),
            self.serialize_value(store_data.get('sid', '')),  # 使用get方法避免KeyError
            self.serialize_value(store_data['store_name']),
            self.serialize_value(store_data['platform_code']),
            self.serialize_value(store_data['platform_name']),
            self.serialize_value(store_data.get('currency', 'USD')),  # 默认USD
            self.serialize_value(store_data.get('is_sync', 1)),  # 默认开启同步
            self.serialize_value(store_data.get('status', 0)),  # 默认状态正常
            self.serialize_value(store_data.get('country_code', '')),
        )

        try:
            self.cursor.execute(sql, values)
            # 可以根据需要返回插入ID或影响的行数
            # return self.cursor.lastrowid
            print("店铺信息插入/更新成功")
            return True
        except Exception as e:
            print(f"插入店铺信息失败: {e}")
            return False

    def _process_single_order(self, order_data):
        """处理单个订单的完整数据插入"""
        global_order_no = order_data['global_order_no']

        # 1. 插入主订单表
        self._insert_orders_table(order_data)

        # 2. 插入买家信息表
        self._insert_buyers_info(order_data)

        # 3. 插入地址信息表
        self._insert_address_info(order_data)

        # 4. 插入商品信息表
        self._insert_item_info(order_data)

        # 5. 插入平台信息表
        self._insert_platform_info(order_data)

        # 6. 插入支付信息表
        self._insert_payment_info(order_data)

        # 7. 插入物流信息表
        self._insert_logistics_info(order_data)

        print(f"订单 {global_order_no} 数据处理完成")

    def _insert_orders_table(self, order_data):
        """插入主订单表"""
        sql = """
           INSERT INTO orders (
        global_order_no, reference_no, store_id, order_from_name, delivery_type, 
        split_type, order_status, global_purchase_time, global_payment_time, global_review_time,
        global_distribution_time, global_print_time, global_mark_time, global_delivery_time,
        amount_currency, remark, global_latest_ship_time, global_cancel_time, update_time,
        order_tag, pending_order_tag, exception_order_tag, wid, warehouse_name,
        original_global_order_no, supplier_id, is_delete, order_custom_fields, global_create_time
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        reference_no = VALUES(reference_no),
        store_id = VALUES(store_id),
        order_from_name = VALUES(order_from_name),
        delivery_type = VALUES(delivery_type),
        split_type = VALUES(split_type),
        order_status = VALUES(order_status),
        global_purchase_time = VALUES(global_purchase_time),
        global_payment_time = VALUES(global_payment_time),
        global_review_time = VALUES(global_review_time),
        global_distribution_time = VALUES(global_distribution_time),
        global_print_time = VALUES(global_print_time),
        global_mark_time = VALUES(global_mark_time),
        global_delivery_time = VALUES(global_delivery_time),
        amount_currency = VALUES(amount_currency),
        remark = VALUES(remark),
        global_latest_ship_time = VALUES(global_latest_ship_time),
        global_cancel_time = VALUES(global_cancel_time),
        update_time = VALUES(update_time),
        order_tag = VALUES(order_tag),
        pending_order_tag = VALUES(pending_order_tag),
        exception_order_tag = VALUES(exception_order_tag),
        wid = VALUES(wid),
        warehouse_name = VALUES(warehouse_name),
        original_global_order_no = VALUES(original_global_order_no),
        supplier_id = VALUES(supplier_id),
        is_delete = VALUES(is_delete),
        order_custom_fields = VALUES(order_custom_fields),
        global_create_time = VALUES(global_create_time),
        data_updatetime = CURRENT_TIMESTAMP
        """

        # 使用序列化函数处理所有值[1,6](@ref)
        values = (
            self.serialize_value(order_data['global_order_no']),
            self.serialize_value(order_data['reference_no']),
            self.serialize_value(order_data['store_id']),
            self.serialize_value(order_data['order_from_name']),
            self.serialize_value(order_data['delivery_type']),
            self.serialize_value(order_data['split_type']),
            self.serialize_value(order_data['status']),
            self.serialize_value(order_data['global_purchase_time']),
            self.serialize_value(order_data['global_payment_time']),
            self.serialize_value(order_data['global_review_time']),
            self.serialize_value(order_data['global_distribution_time']),
            self.serialize_value(order_data['global_print_time']),
            self.serialize_value(order_data['global_mark_time']),
            self.serialize_value(order_data['global_delivery_time']),
            self.serialize_value(order_data['amount_currency']),
            self.serialize_value(order_data['remark']),
            self.serialize_value(order_data['global_latest_ship_time']),
            self.serialize_value(order_data['global_cancel_time']),
            self.serialize_value(order_data['update_time']),
            self.serialize_value(order_data['order_tag']),
            self.serialize_value(order_data['pending_order_tag']),
            self.serialize_value(order_data['exception_order_tag']),
            self.serialize_value(order_data['wid']),
            self.serialize_value(order_data['warehouse_name']),
            self.serialize_value(order_data['original_global_order_no']),
            self.serialize_value(order_data['supplier_id']),
            self.serialize_value(order_data['is_delete']),
            self.serialize_value(order_data.get('order_custom_fields')),  # 使用序列化处理[1](@ref)
            self.serialize_value(order_data['global_create_time'])
        )

        self.cursor.execute(sql, values)

    def _insert_buyers_info(self, order_data):
        """插入买家信息表"""
        buyers_info = order_data.get('buyers_info', {})
        sql = """
        INSERT IGNORE INTO buyers_info (
            global_order_no, buyer_no, buyer_email, buyer_name, buyer_note
        ) VALUES (%s, %s, %s, %s, %s)
        on DUPLICATE KEY UPDATE 
        buyer_no = VALUES(buyer_no),
        buyer_email = VALUES(buyer_email),
        buyer_name = VALUES(buyer_name),
        buyer_note = VALUES(buyer_note),
        data_updatetime = CURRENT_TIMESTAMP
        """

        # 使用序列化函数处理所有值[1](@ref)
        values = (
            self.serialize_value(order_data['global_order_no']),
            self.serialize_value(buyers_info.get('buyer_no', '')),
            self.serialize_value(buyers_info.get('buyer_email', '')),
            self.serialize_value(buyers_info.get('buyer_name', '')),
            self.serialize_value(buyers_info.get('buyer_note', ''))
        )

        self.cursor.execute(sql, values)

    def _insert_address_info(self, order_data):
        """插入地址信息表"""
        address_info = order_data.get('address_info', {})
        sql = """
           INSERT INTO address_info (
        global_order_no, receiver_name, receiver_mobile, receiver_tel, receiver_country_code,
        city, state_or_region, address_line1, address_line2, address_line3,
        district, postal_code, doorplate_no, company_name
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        receiver_name = VALUES(receiver_name),
        receiver_mobile = VALUES(receiver_mobile),
        receiver_tel = VALUES(receiver_tel),
        receiver_country_code = VALUES(receiver_country_code),
        city = VALUES(city),
        state_or_region = VALUES(state_or_region),
        address_line1 = VALUES(address_line1),
        address_line2 = VALUES(address_line2),
        address_line3 = VALUES(address_line3),
        district = VALUES(district),
        postal_code = VALUES(postal_code),
        doorplate_no = VALUES(doorplate_no),
        company_name = VALUES(company_name),
        data_updatetime = CURRENT_TIMESTAMP
        """

        # 使用序列化函数处理所有值[1](@ref)
        values = (
            self.serialize_value(order_data['global_order_no']),
            self.serialize_value(address_info.get('receiver_name', '')),
            self.serialize_value(address_info.get('receiver_mobile', '')),
            self.serialize_value(address_info.get('receiver_tel', '')),
            self.serialize_value(address_info.get('receiver_country_code', '')),
            self.serialize_value(address_info.get('city', '')),
            self.serialize_value(address_info.get('state_or_region', '')),
            self.serialize_value(address_info.get('address_line1', '')),
            self.serialize_value(address_info.get('address_line2', '')),
            self.serialize_value(address_info.get('address_line3', '')),
            self.serialize_value(address_info.get('district', '')),
            self.serialize_value(address_info.get('postal_code', '')),
            self.serialize_value(address_info.get('doorplate_no', '')),
            self.serialize_value(address_info.get('company_name'))
        )

        self.cursor.execute(sql, values)

    def _insert_item_info(self, order_data):
        """插入商品信息表（使用executemany批量插入）"""
        items = order_data.get('item_info', [])
        if not items:
            return

        sql = """
                INSERT INTO item_info (
            global_order_no, global_item_no, item_id, platform_order_no, order_item_no,
            item_from_name, msku, local_sku, product_no, local_product_name, is_bundled,
            title, variant_attr, unit_price_amount, item_price_amount, quantity, remark,
            platform_status, item_type, stock_cost_amount, wms_outbound_cost_amount,
            stock_deduct_id, stock_deduct_name, cg_price_amount, shipping_amount,
            wms_shipping_price_amount, customer_shipping_amount, discount_amount,
            customer_tip_amount, tax_amount, sales_revenue_amount, transaction_fee_amount,
            other_amount, customized_url, platform_subsidy_amount, cod_amount,
            gift_wrap_amount, platform_tax_amount, points_granted_amount, other_fee,
            delivery_time, source_name, data_json, item_custom_fields, is_delete
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            item_id = VALUES(item_id),
            platform_order_no = VALUES(platform_order_no),
            order_item_no = VALUES(order_item_no),
            item_from_name = VALUES(item_from_name),
            msku = VALUES(msku),
            local_sku = VALUES(local_sku),
            product_no = VALUES(product_no),
            local_product_name = VALUES(local_product_name),
            is_bundled = VALUES(is_bundled),
            title = VALUES(title),
            variant_attr = VALUES(variant_attr),
            unit_price_amount = VALUES(unit_price_amount),
            item_price_amount = VALUES(item_price_amount),
            quantity = VALUES(quantity),
            remark = VALUES(remark),
            platform_status = VALUES(platform_status),
            item_type = VALUES(item_type),
            stock_cost_amount = VALUES(stock_cost_amount),
            wms_outbound_cost_amount = VALUES(wms_outbound_cost_amount),
            stock_deduct_id = VALUES(stock_deduct_id),
            stock_deduct_name = VALUES(stock_deduct_name),
            cg_price_amount = VALUES(cg_price_amount),
            shipping_amount = VALUES(shipping_amount),
            wms_shipping_price_amount = VALUES(wms_shipping_price_amount),
            customer_shipping_amount = VALUES(customer_shipping_amount),
            discount_amount = VALUES(discount_amount),
            customer_tip_amount = VALUES(customer_tip_amount),
            tax_amount = VALUES(tax_amount),
            sales_revenue_amount = VALUES(sales_revenue_amount),
            transaction_fee_amount = VALUES(transaction_fee_amount),
            other_amount = VALUES(other_amount),
            customized_url = VALUES(customized_url),
            platform_subsidy_amount = VALUES(platform_subsidy_amount),
            cod_amount = VALUES(cod_amount),
            gift_wrap_amount = VALUES(gift_wrap_amount),
            platform_tax_amount = VALUES(platform_tax_amount),
            points_granted_amount = VALUES(points_granted_amount),
            other_fee = VALUES(other_fee),
            delivery_time = VALUES(delivery_time),
            source_name = VALUES(source_name),
            data_json = VALUES(data_json),
            item_custom_fields = VALUES(item_custom_fields),
            is_delete = VALUES(is_delete),
            data_updatetime = CURRENT_TIMESTAMP
        """

        batch_data = []
        for item in items:
            # 使用序列化函数处理所有值[1,6](@ref)
            data = (
                self.serialize_value(order_data['global_order_no']),
                self.serialize_value(item.get('globalItemNo')),
                self.serialize_value(item.get('id')),
                self.serialize_value(item.get('platform_order_no')),
                self.serialize_value(item.get('order_item_no')),
                self.serialize_value(item.get('item_from_name')),
                self.serialize_value(item.get('msku')),
                self.serialize_value(item.get('local_sku')),
                self.serialize_value(item.get('product_no')),
                self.serialize_value(item.get('local_product_name')),
                self.serialize_value(item.get('is_bundled', 0)),
                self.serialize_value(item.get('title')),
                self.serialize_value(item.get('variant_attr')),
                self.serialize_value(
                    float(item.get('unit_price_amount', 0)) if item.get('unit_price_amount') not in [None,
                                                                                                     ''] else 0.0),
                self.serialize_value(
                    float(item.get('item_price_amount', 0)) if item.get('item_price_amount') not in [None,
                                                                                                     ''] else 0.0),
                self.serialize_value(item.get('quantity', 0)),
                self.serialize_value(item.get('remark', '')),
                self.serialize_value(item.get('platform_status')),
                self.serialize_value(item.get('type')),
                self.serialize_value(
                    float(item.get('stock_cost_amount', 0)) if item.get('stock_cost_amount') not in [None,
                                                                                                     ''] else 0.0),
                self.serialize_value(
                    float(item.get('wms_outbound_cost_amount', 0)) if item.get('wms_outbound_cost_amount') not in [None,
                                                                                                                   ''] else 0.0),
                self.serialize_value(item.get('stock_deduct_id')),
                self.serialize_value(item.get('stock_deduct_name')),
                self.serialize_value(
                    float(item.get('cg_price_amount', 0)) if item.get('cg_price_amount') not in [None, ''] else 0.0),
                self.serialize_value(
                    float(item.get('shipping_amount', 0)) if item.get('shipping_amount') not in [None, ''] else 0.0),
                self.serialize_value(
                    float(item.get('wms_shipping_price_amount', 0)) if item.get('wms_shipping_price_amount') not in [
                        None, ''] else 0.0),
                self.serialize_value(
                    float(item.get('customer_shipping_amount', 0)) if item.get('customer_shipping_amount') not in [None,
                                                                                                                   ''] else 0.0),
                self.serialize_value(
                    float(item.get('discount_amount', 0)) if item.get('discount_amount') not in [None, ''] else 0.0),
                self.serialize_value(
                    float(item.get('customer_tip_amount', 0)) if item.get('customer_tip_amount') not in [None,
                                                                                                         ''] else 0.0),
                self.serialize_value(
                    float(item.get('tax_amount', 0)) if item.get('tax_amount') not in [None, ''] else 0.0),
                self.serialize_value(
                    float(item.get('sales_revenue_amount', 0)) if item.get('sales_revenue_amount') not in [None,
                                                                                                           ''] else 0.0),
                self.serialize_value(
                    float(item.get('transaction_fee_amount', 0)) if item.get('transaction_fee_amount') not in [None,
                                                                                                               ''] else 0.0),
                self.serialize_value(
                    float(item.get('other_amount', 0)) if item.get('other_amount') not in [None, ''] else 0.0),
                self.serialize_value(item.get('customized_url')),
                self.serialize_value(
                    float(item.get('platform_subsidy_amount', 0)) if item.get('platform_subsidy_amount') not in [None,
                                                                                                                 ''] else 0.0),
                self.serialize_value(
                    float(item.get('cod_amount', 0)) if item.get('cod_amount') not in [None, ''] else 0.0),
                self.serialize_value(
                    float(item.get('gift_wrap_amount', 0)) if item.get('gift_wrap_amount') not in [None, ''] else 0.0),
                self.serialize_value(
                    float(item.get('platform_tax_amount', 0)) if item.get('platform_tax_amount') not in [None,
                                                                                                         ''] else 0.0),
                self.serialize_value(
                    float(item.get('points_granted_amount', 0)) if item.get('points_granted_amount') not in [None,
                                                                                                             ''] else 0.0),
                self.serialize_value(
                    float(item.get('other_fee', 0)) if item.get('other_fee') not in [None, ''] else 0.0),
                self.serialize_value(item.get('delivery_time')),
                self.serialize_value(item.get('source_name')),
                self.serialize_value(item.get('data_json')),  # 可能包含字典[6](@ref)
                self.serialize_value(item.get('item_custom_fields')),  # 可能包含字典[1](@ref)
                self.serialize_value(item.get('is_delete', 0))
            )
            batch_data.append(data)

        # 使用executemany批量插入[2](@ref)
        self.cursor.executemany(sql, batch_data)

    def _insert_platform_info(self, order_data):
        """插入平台信息表"""
        platforms = order_data.get('platform_info', [])
        if not platforms:
            return

        sql = """
        INSERT IGNORE INTO platform_info (
            global_order_no, order_from, platform_order_no, platform_order_name,
            platform_code, store_country_code,order_status ,payment_status, shipping_status,
            purchase_time, payment_time, latest_ship_time, cancel_time, delivery_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        order_from = VALUES(order_from),
        platform_order_name = VALUES(platform_order_name),
        platform_code = VALUES(platform_code),
        store_country_code = VALUES(store_country_code),
        order_status = VALUES(order_status),
        payment_status = VALUES(payment_status),
        shipping_status = VALUES(shipping_status),
        purchase_time = VALUES(purchase_time),
        payment_time = VALUES(payment_time),
        latest_ship_time = VALUES(latest_ship_time),
        cancel_time = VALUES(cancel_time),
        delivery_time = VALUES(delivery_time),
        data_updatetime = CURRENT_TIMESTAMP 
        """

        batch_data = []
        for platform in platforms:
            # 使用序列化函数处理所有值[1](@ref)
            data = (
                self.serialize_value(order_data['global_order_no']),
                self.serialize_value(platform.get('order_from')),
                self.serialize_value(platform.get('platform_order_no')),
                self.serialize_value(platform.get('platform_order_name')),
                self.serialize_value(platform.get('platform_code')),
                self.serialize_value(platform.get('store_Country_code')),
                self.serialize_value(platform.get('status')),
                self.serialize_value(platform.get('payment_status')),
                self.serialize_value(platform.get('shipping_status')),
                self.serialize_value(platform.get('purchase_time')),
                self.serialize_value(platform.get('payment_time')),
                self.serialize_value(platform.get('latest_ship_time')),
                self.serialize_value(platform.get('cancel_time')),
                self.serialize_value(platform.get('delivery_time'))
            )
            batch_data.append(data)

        self.cursor.executemany(sql, batch_data)

    def _insert_payment_info(self, order_data):
        """插入支付信息表"""
        payments = order_data.get('payment_info', [])
        if not payments:
            return

        sql = """
            INSERT INTO payment_info (
        global_order_no, platform_order_no, payment_method, transaction_no,
        currency, payment_amount, payment_time
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        payment_method = VALUES(payment_method),
        transaction_no = VALUES(transaction_no),
        currency = VALUES(currency),
        payment_amount = VALUES(payment_amount),
        payment_time = VALUES(payment_time),
        data_updatetime = CURRENT_TIMESTAMP
        """

        batch_data = []
        for payment in payments:
            # 使用序列化函数处理所有值[1](@ref)
            data = (
                self.serialize_value(order_data['global_order_no']),
                self.serialize_value(payment.get('platform_order_no')),
                self.serialize_value(payment.get('payment_method')),
                self.serialize_value(payment.get('transaction_no')),
                self.serialize_value(payment.get('currency')),
                self.serialize_value(
                    float(payment.get('payment_amount', 0)) if payment.get('payment_amount') not in [None,
                                                                                                     ''] else 0.0),
                self.serialize_value(payment.get('payment_time'))
            )
            batch_data.append(data)

        self.cursor.executemany(sql, batch_data)

    def _insert_logistics_info(self, order_data):
        """插入物流信息表"""
        logistics = order_data.get('logistics_info', {})
        if not logistics:
            return

        sql = """
            INSERT INTO logistics_info (
        global_order_no, logistics_type_id, logistics_type_name,
        logistics_provider_id, logistics_provider_name, actual_carrier, waybill_no,
        pre_weight, pre_fee_weight, pre_fee_weight_unit, pre_pkg_length, pre_pkg_height,
        pre_pkg_width, weight, pkg_fee_weight, pkg_fee_weight_unit, pkg_length,
        pkg_width, pkg_height, weight_unit, pkg_size_unit, cost_currency_code,
        pre_cost_amount, cost_amount, logistics_time, tracking_no, mark_no
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        logistics_type_id = VALUES(logistics_type_id),
        logistics_type_name = VALUES(logistics_type_name),
        logistics_provider_id = VALUES(logistics_provider_id),
        logistics_provider_name = VALUES(logistics_provider_name),
        actual_carrier = VALUES(actual_carrier),
        waybill_no = VALUES(waybill_no),
        pre_weight = VALUES(pre_weight),
        pre_fee_weight = VALUES(pre_fee_weight),
        pre_fee_weight_unit = VALUES(pre_fee_weight_unit),
        pre_pkg_length = VALUES(pre_pkg_length),
        pre_pkg_height = VALUES(pre_pkg_height),
        pre_pkg_width = VALUES(pre_pkg_width),
        weight = VALUES(weight),
        pkg_fee_weight = VALUES(pkg_fee_weight),
        pkg_fee_weight_unit = VALUES(pkg_fee_weight_unit),
        pkg_length = VALUES(pkg_length),
        pkg_width = VALUES(pkg_width),
        pkg_height = VALUES(pkg_height),
        weight_unit = VALUES(weight_unit),
        pkg_size_unit = VALUES(pkg_size_unit),
        cost_currency_code = VALUES(cost_currency_code),
        pre_cost_amount = VALUES(pre_cost_amount),
        cost_amount = VALUES(cost_amount),
        logistics_time = VALUES(logistics_time),
        tracking_no = VALUES(tracking_no),
        mark_no = VALUES(mark_no),
        data_updatetime = CURRENT_TIMESTAMP
        """

        # 使用序列化函数处理所有值[1](@ref)
        values = (
            self.serialize_value(order_data['global_order_no']),
            self.serialize_value(logistics.get('logistics_type_id')),
            self.serialize_value(logistics.get('logistics_type_name')),
            self.serialize_value(logistics.get('logistics_provider_id')),
            self.serialize_value(logistics.get('logistics_provider_name')),
            self.serialize_value(logistics.get('actual_carrier')),
            self.serialize_value(logistics.get('waybill_no')),
            self.serialize_value(
                float(logistics.get('pre_weight', 0)) if logistics.get('pre_weight') not in [None, ''] else 0.0),
            self.serialize_value(
                float(logistics.get('pre_fee_weight', 0)) if logistics.get('pre_fee_weight') not in [None,
                                                                                                     ''] else 0.0),
            self.serialize_value(logistics.get('pre_fee_weight_unit')),
            self.serialize_value(
                float(logistics.get('pre_pkg_length', 0)) if logistics.get('pre_pkg_length') not in [None,
                                                                                                     ''] else 0.0),
            self.serialize_value(
                float(logistics.get('pre_pkg_height', 0)) if logistics.get('pre_pkg_height') not in [None,
                                                                                                     ''] else 0.0),
            self.serialize_value(
                float(logistics.get('pre_pkg_width', 0)) if logistics.get('pre_pkg_width') not in [None, ''] else 0.0),
            self.serialize_value(
                float(logistics.get('weight', 0)) if logistics.get('weight') not in [None, ''] else 0.0),
            self.serialize_value(
                float(logistics.get('pkg_fee_weight', 0)) if logistics.get('pkg_fee_weight') not in [None,
                                                                                                     ''] else 0.0),
            self.serialize_value(logistics.get('pkg_fee_weight_unit')),
            self.serialize_value(
                float(logistics.get('pkg_length', 0)) if logistics.get('pkg_length') not in [None, ''] else 0.0),
            self.serialize_value(
                float(logistics.get('pkg_width', 0)) if logistics.get('pkg_width') not in [None, ''] else 0.0),
            self.serialize_value(
                float(logistics.get('pkg_height', 0)) if logistics.get('pkg_height') not in [None, ''] else 0.0),
            self.serialize_value(logistics.get('weight_unit')),
            self.serialize_value(logistics.get('pkg_size_unit')),
            self.serialize_value(logistics.get('cost_currency_code')),
            self.serialize_value(logistics.get('pre_cost_amount')),
            self.serialize_value(
                float(logistics.get('cost_amount', 0)) if logistics.get('cost_amount') not in [None, ''] else 0.0),
            self.serialize_value(logistics.get('logistics_time')),
            self.serialize_value(logistics.get('tracking_no')),
            self.serialize_value(logistics.get('mark_no'))
        )

        self.cursor.execute(sql, values)


    def insert_warehouse_table(self, warehouse_list):
        """
        批量插入仓库数据到warehouse_info表
        warehouse_list: API返回的仓库列表
        """
        if not self.conn:
            self.connect_db()

        try:
            # 遍历每个仓库数据进行处理
            for warehouse_data in warehouse_list:
                self._process_warehouse(warehouse_data)

            # 提交所有事务
            self.conn.commit()
            print(f"成功插入/更新 {len(warehouse_list)} 个仓库数据")

        except Exception as e:
            self.conn.rollback()
            print(f"仓库数据插入失败，已回滚: {e}")
            raise


    def _process_warehouse(self, warehouse_data):
        """
        插入或更新仓库信息到 warehouse_info 表
        使用 ON DUPLICATE KEY UPDATE 实现存在时更新，不存在时插入
        """
        sql = """
            INSERT INTO warehouse_info (
                wid, w_type, w_sub_type, w_name, is_delete, country_code, 
                wp_id, wp_name, t_warehouse_name, t_warehouse_code, 
                t_country_area_name, t_status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                w_type = VALUES(w_type),
                w_sub_type = VALUES(w_sub_type),
                w_name = VALUES(w_name),
                is_delete = VALUES(is_delete),
                country_code = VALUES(country_code),
                wp_id = VALUES(wp_id),
                wp_name = VALUES(wp_name),
                t_warehouse_name = VALUES(t_warehouse_name),
                t_warehouse_code = VALUES(t_warehouse_code),
                t_country_area_name = VALUES(t_country_area_name),
                t_status = VALUES(t_status),
                data_updatime = CURRENT_TIMESTAMP
        """

        # 准备要插入的数据值元组
        values = (
            self.serialize_value(warehouse_data.get('wid')),
            self.serialize_value(warehouse_data.get('type')),  # 映射到 w_type
            self.serialize_value(warehouse_data.get('sub_type')),  # 映射到 w_sub_type
            self.serialize_value(warehouse_data.get('name')),  # 映射到 w_name
            self.serialize_value(warehouse_data.get('is_delete', 0)),
            self.serialize_value(warehouse_data.get('country_code', '')),
            self.serialize_value(warehouse_data.get('wp_id')),
            self.serialize_value(warehouse_data.get('wp_name', '')),
            self.serialize_value(warehouse_data.get('t_warehouse_name', '')),
            self.serialize_value(warehouse_data.get('t_warehouse_code', '')),
            self.serialize_value(warehouse_data.get('t_country_area_name', '')),
            self.serialize_value(warehouse_data.get('t_status', 1)),
        )

        try:
            self.cursor.execute(sql, values)
            print(f"仓库信息插入/更新成功: wid={warehouse_data.get('wid')}")
            return True
        except Exception as e:
            print(f"插入仓库信息失败: {e}, 数据: {values}")
            return False



    def get_warehouse_ids(self):
        """
        查询所有仓库wid信息
        """
        if not self.conn:
            self.connect_db()

        sql = "SELECT wid FROM warehouse_info "
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            wid_list = [row[0] for row in result] if result else []
            return wid_list
        except Exception as e:
            print(f"查询仓库信息失败: {e}")
            return None

    def insert_inventory_table(self, inventory_list):
        """
        批量插入库存数据到inventory_info表
        inventory_list: API返回的库存列表
        """
        if not self.conn:
            self.connect_db()

        try:
            # 遍历每个库存数据进行处理
            for inventory_data in inventory_list:
                self._process_single_inventory(inventory_data)

            # 提交所有事务
            self.conn.commit()
            print(f"成功插入/更新 {len(inventory_list)} 条库存数据")

        except Exception as e:
            self.conn.rollback()
            print(f"库存数据插入失败，已回滚: {e}")
            raise

    def _process_single_inventory(self, inventory_data):
        """处理单个库存记录"""
        sql = """
        INSERT INTO inventory_info (
            wid, product_id, sku, seller_id, fnsku, product_total, product_valid_num,
            product_bad_num, product_qc_num, product_lock_num, good_lock_num, bad_lock_num,
            stock_cost_total, quantity_receive, stock_cost, product_onway, transit_head_cost,
            average_age, qty_sellable, qty_reserved, qty_onway, qty_pending,
            box_qty_sellable, box_qty_reserved, box_qty_onway, box_qty_pending,
            age_0_15_days, age_16_30_days, age_31_90_days, age_above_91_days,
            available_inventory_box_qty, purchase_price, price, head_stock_price, stock_price
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            product_id = VALUES(product_id),
            seller_id = VALUES(seller_id),
            fnsku = VALUES(fnsku),
            product_total = VALUES(product_total),
            product_valid_num = VALUES(product_valid_num),
            product_bad_num = VALUES(product_bad_num),
            product_qc_num = VALUES(product_qc_num),
            product_lock_num = VALUES(product_lock_num),
            good_lock_num = VALUES(good_lock_num),
            bad_lock_num = VALUES(bad_lock_num),
            stock_cost_total = VALUES(stock_cost_total),
            quantity_receive = VALUES(quantity_receive),
            stock_cost = VALUES(stock_cost),
            product_onway = VALUES(product_onway),
            transit_head_cost = VALUES(transit_head_cost),
            average_age = VALUES(average_age),
            qty_sellable = VALUES(qty_sellable),
            qty_reserved = VALUES(qty_reserved),
            qty_onway = VALUES(qty_onway),
            qty_pending = VALUES(qty_pending),
            box_qty_sellable = VALUES(box_qty_sellable),
            box_qty_reserved = VALUES(box_qty_reserved),
            box_qty_onway = VALUES(box_qty_onway),
            box_qty_pending = VALUES(box_qty_pending),
            age_0_15_days = VALUES(age_0_15_days),
            age_16_30_days = VALUES(age_16_30_days),
            age_31_90_days = VALUES(age_31_90_days),
            age_above_91_days = VALUES(age_above_91_days),
            available_inventory_box_qty = VALUES(available_inventory_box_qty),
            purchase_price = VALUES(purchase_price),
            price = VALUES(price),
            head_stock_price = VALUES(head_stock_price),
            stock_price = VALUES(stock_price),
            data_updatime = CURRENT_TIMESTAMP
        """

        # 处理third_inventory数据
        third_inventory = inventory_data.get('third_inventory', {})

        # 处理stock_age_list数据
        stock_age_list = inventory_data.get('stock_age_list', [])
        age_mapping = {
            '0-15天库龄': 'age_0_15_days',
            '16-30天库龄': 'age_16_30_days',
            '31-90天库龄': 'age_31_90_days',
            '91天以上库龄': 'age_above_91_days'
        }

        # 初始化库龄字段为0
        age_data = {field: 0 for field in age_mapping.values()}

        # 填充库龄数据
        for age_item in stock_age_list:
            age_name = age_item.get('name', '')
            age_qty = age_item.get('qty', 0)
            if age_name in age_mapping:
                age_data[age_mapping[age_name]] = age_qty

        # 准备插入的值
        values = (
            self.serialize_value(inventory_data.get('wid')),
            self.serialize_value(inventory_data.get('product_id')),
            self.serialize_value(inventory_data.get('sku')),
            self.serialize_value(inventory_data.get('seller_id', '0')),
            self.serialize_value(inventory_data.get('fnsku', '')),
            self.serialize_value(inventory_data.get('product_total', 0)),
            self.serialize_value(inventory_data.get('product_valid_num', 0)),
            self.serialize_value(inventory_data.get('product_bad_num', 0)),
            self.serialize_value(inventory_data.get('product_qc_num', 0)),
            self.serialize_value(inventory_data.get('product_lock_num', 0)),
            self.serialize_value(inventory_data.get('good_lock_num', 0)),
            self.serialize_value(inventory_data.get('bad_lock_num', 0)),
            self.serialize_value(
                float(inventory_data.get('stock_cost_total', 0))
                if inventory_data.get('stock_cost_total') not in [None, ''] else 0.0
            ),
            self.serialize_value(
                float(inventory_data.get('quantity_receive', 0))
                if inventory_data.get('quantity_receive') not in [None, ''] else 0.0
            ),
            self.serialize_value(
                float(inventory_data.get('stock_cost', 0))
                if inventory_data.get('stock_cost') not in [None, ''] else 0.0
            ),
            self.serialize_value(inventory_data.get('product_onway', 0)),
            self.serialize_value(
                float(inventory_data.get('transit_head_cost', 0))
                if inventory_data.get('transit_head_cost') not in [None, ''] else 0.0
            ),
            self.serialize_value(inventory_data.get('average_age', 0)),
            # third_inventory字段
            self.serialize_value(third_inventory.get('qty_sellable', 0)),
            self.serialize_value(third_inventory.get('qty_reserved', 0)),
            self.serialize_value(third_inventory.get('qty_onway', 0)),
            self.serialize_value(third_inventory.get('qty_pending', 0)),
            self.serialize_value(third_inventory.get('box_qty_sellable', 0)),
            self.serialize_value(third_inventory.get('box_qty_reserved', 0)),
            self.serialize_value(third_inventory.get('box_qty_onway', 0)),
            self.serialize_value(third_inventory.get('box_qty_pending', 0)),
            # stock_age_list字段
            age_data['age_0_15_days'],
            age_data['age_16_30_days'],
            age_data['age_31_90_days'],
            age_data['age_above_91_days'],
            # 其他字段
            self.serialize_value(inventory_data.get('available_inventory_box_qty', 0)),
            self.serialize_value(
                float(inventory_data.get('purchase_price', 0))
                if inventory_data.get('purchase_price') not in [None, ''] else 0.0
            ),
            self.serialize_value(
                float(inventory_data.get('price', 0))
                if inventory_data.get('price') not in [None, ''] else 0.0
            ),
            self.serialize_value(
                float(inventory_data.get('head_stock_price', 0))
                if inventory_data.get('head_stock_price') not in [None, ''] else 0.0
            ),
            self.serialize_value(
                float(inventory_data.get('stock_price', 0))
                if inventory_data.get('stock_price') not in [None, ''] else 0.0
            )
        )

        try:
            self.cursor.execute(sql, values)
            print(f"库存信息插入/更新成功: wid={inventory_data.get('wid')}, sku={inventory_data.get('sku')}")
            return True
        except Exception as e:
            print(f"插入库存信息失败: {e}, 数据: {inventory_data}")
            return False

    def insert_sales_info(self, sales_data):
        """
        插入销量统计信息到sales_info表
        """
        if not self.conn:
            self.connect_db()

            # 将sku字段转换为JSON字符串并计算MD5
        sku_json = json.dumps(sales_data.get('sku', []), sort_keys=True, separators=(',', ':'))
        sales_code = hashlib.md5(sku_json.encode('utf-8')).hexdigest()

        sql = """
          INSERT INTO sales_info (
              sku, spu, spu_name, msku, mskuld, sku_and_product_name, product_name, develop_name,
              sid, platform_code, platform_name, site_code, site_name, store_name,
              attribute, parent_asin, platform_product_id, platform_product_title,
              currency_code, icon, pic_url, date_collect, volume_total, sales_code
          ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
          ON DUPLICATE KEY UPDATE
              spu = VALUES(spu),
              spu_name = VALUES(spu_name),
              msku = VALUES(msku),
              mskuld = VALUES(mskuld),
              sku_and_product_name = VALUES(sku_and_product_name),
              product_name = VALUES(product_name),
              develop_name = VALUES(develop_name),
              sid = VALUES(sid),
              platform_code = VALUES(platform_code),
              platform_name = VALUES(platform_name),
              site_code = VALUES(site_code),
              site_name = VALUES(site_name),
              store_name = VALUES(store_name),
              attribute = VALUES(attribute),
              parent_asin = VALUES(parent_asin),
              platform_product_id = VALUES(platform_product_id),
              platform_product_title = VALUES(platform_product_title),
              currency_code = VALUES(currency_code),
              icon = VALUES(icon),
              pic_url = VALUES(pic_url),
              date_collect = VALUES(date_collect),
              volume_total = VALUES(volume_total),
              update_time = CURRENT_TIMESTAMP
          """

        try:
            # 准备数据
            values = (
                self.serialize_value(sales_data.get('sku', [])),
                self.serialize_value(sales_data.get('spu', [])),
                self.serialize_value(sales_data.get('spu_name', [])),
                self.serialize_value(sales_data.get('msku', [])),
                self.serialize_value(sales_data.get('mskuId', [])),
                self.serialize_value(sales_data.get('skuAndProductName', [])),
                self.serialize_value(sales_data.get('product_name', [])),
                self.serialize_value(sales_data.get('develop_name', [])),
                self.serialize_value(sales_data.get('sid', [])),
                self.serialize_value(sales_data.get('platform_code', [])),
                self.serialize_value(sales_data.get('platform_name', [])),
                self.serialize_value(sales_data.get('site_code', [])),
                self.serialize_value(sales_data.get('site_name', [])),
                self.serialize_value(sales_data.get('store_name', [])),
                self.serialize_value(sales_data.get('attribute', [])),
                self.serialize_value(sales_data.get('parentAsin', [])),
                self.serialize_value(sales_data.get('platform_product_id', [])),
                self.serialize_value(sales_data.get('platform_product_title', [])),
                self.serialize_value(sales_data.get('currency_code', '')),
                self.serialize_value(sales_data.get('icon', '')),
                self.serialize_value(sales_data.get('pic_url', '')),
                self.serialize_value(sales_data.get('date_collect', {})),
                self.serialize_value(
                    float(sales_data.get('volumeTotal', 0))
                    if sales_data.get('volumeTotal') not in [None, ''] else 0.0
                ),
                sales_code  # 新增的sales_code字段
            )

            self.cursor.execute(sql, values)
            self.conn.commit()
            print(f"销量信息插入/更新成功，sales_code: {sales_code}")
            return True

        except Exception as e:
            print(f"插入销量信息失败: {e}")
            self.conn.rollback()
            return False

