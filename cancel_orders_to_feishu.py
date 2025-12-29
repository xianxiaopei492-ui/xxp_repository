import pymysql
import requests
import time
import re
from datetime import datetime, date
from config import  load_config_from_env
from utils import extract_store_name

config = load_config_from_env()

# ================== 配置部分 ==================
APP_ID = config['cancel_orders_config']['APP_ID']
APP_SECRET = config['cancel_orders_config']['APP_SECRET']
APP_Token = config['cancel_orders_config']['APP_TOKEN'] # app_token
CANCEL_ORDERS_TABLE_ID = config['cancel_orders_config']['CANCEL_ORDERS_TABLE_ID']  # 新的取消订单表ID

MYSQL_CONFIG = config['db_config']

MYSQL_TABLE = "orders_merge"

# 修正后的取消订单表字段定义 - 全部使用文本类型
CANCEL_ORDERS_FIELDS = [
    {
        "field_name": "对应日期",
        "type": 1  # 文本类型
    },
    {
        "field_name": "平台订单号",
        "type": 1  # 文本类型
    },
    {
        "field_name": "对应店铺id",
        "type": 1  # 文本类型
    },
    {
        "field_name": "对应店铺名称",
        "type": 1  # 文本类型
    },
    {
        "field_name": "订单状态",
        "type": 1,  # 文本类型
    },
    {
        "field_name": "取消时间",
        "type": 1  # 文本类型
    },
    {
        "field_name": "对应负责人",
        "type": 1  # 文本类型
    },
    {
        "field_name": "是否跟进",
        "type": 3,  # 单选类型
        "property": {
            "options": [
                {"name": "是"},
                {"name": "否"}
            ]
        }
    }
]


# ================== 飞书API工具函数 ==================
def get_tenant_access_token():
    """获取访问令牌"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={
        "app_id": APP_ID,
        "app_secret": APP_SECRET
    }).json()

    if resp.get("code") != 0:
        raise Exception(f"获取token失败: {resp}")

    return resp["tenant_access_token"]


def get_existing_fields_with_details(token, table_id):
    """获取表格中现有的字段详细信息"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_Token}/tables/{table_id}/fields"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, headers=headers)
        result = response.json()

        if result.get("code") == 0:
            fields = result.get("data", {}).get("items", [])
            field_details = {}
            for field in fields:
                field_name = field.get("field_name")
                field_details[field_name] = {
                    "type": field.get("type"),
                    "property": field.get("property", {}),
                    "field_id": field.get("field_id")
                }
            return field_details
        else:
            print(f"❌ 获取字段详情失败: {result.get('msg')}")
            return {}
    except Exception as e:
        print(f"❌ 获取字段详情异常: {e}")
        return {}


def create_cancel_orders_fields(token):
    """创建取消订单表的字段"""
    print("🔧 开始创建取消订单表字段...")

    # 获取现有字段的详细信息
    existing_fields = get_existing_fields_with_details(token, CANCEL_ORDERS_TABLE_ID)

    created_count = 0
    has_error = False

    for field_def in CANCEL_ORDERS_FIELDS:
        field_name = field_def["field_name"]

        if field_name in existing_fields:
            existing_type = existing_fields[field_name]["type"]
            expected_type = field_def["type"]

            print(f"✅ 字段已存在: {field_name} (当前类型: {existing_type}, 期望类型: {expected_type})")

            # 如果类型不匹配，尝试更新字段
            if existing_type != expected_type:
                print(f"🔄 字段类型不匹配，尝试更新字段: {field_name}")
                if update_field_type(token, field_def, CANCEL_ORDERS_TABLE_ID, existing_fields[field_name]["field_id"]):
                    print(f"✅ 成功更新字段类型: {field_name}")
                else:
                    print(f"❌ 更新字段类型失败: {field_name}")
                    has_error = True
            continue

        if create_field(token, field_def, CANCEL_ORDERS_TABLE_ID):
            created_count += 1
        else:
            has_error = True
        time.sleep(0.5)

    print(f"🎯 字段创建完成: 新增 {created_count} 个字段")
    return not has_error


def update_field_type(token, field_def, table_id, field_id):
    """更新字段类型"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_Token}/tables/{table_id}/fields/{field_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "field_name": field_def["field_name"],
        "type": field_def["type"]
    }

    try:
        print(f"🔄 正在更新字段类型: {field_def['field_name']} -> 类型 {field_def['type']}")
        response = requests.put(url, headers=headers, json=payload, timeout=10)
        result = response.json()

        if result.get("code") == 0:
            print(f"✅ 成功更新字段类型: {field_def['field_name']}")
            return True
        else:
            error_msg = result.get("msg", "未知错误")
            error_code = result.get("code")
            print(f"❌ 更新字段类型失败 [{field_def['field_name']}]: {error_msg} (错误码: {error_code})")
            return False
    except Exception as e:
        print(f"❌ 更新字段类型异常 [{field_def['field_name']}]: {e}")
        return False


def create_field(token, field_def, table_id):
    """创建字段"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_Token}/tables/{table_id}/fields"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # 简化payload，只包含必要字段
    payload = {
        "field_name": field_def["field_name"],
        "type": field_def["type"]
    }

    # 只有在有property且不为空时才添加
    if "property" in field_def and field_def["property"]:
        payload["property"] = field_def["property"]

    try:
        print(f"🔄 正在创建字段: {field_def['field_name']} (类型: {field_def['type']})")
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        result = response.json()

        if result.get("code") == 0:
            print(f"✅ 成功创建字段: {field_def['field_name']}")
            return True
        else:
            error_msg = result.get("msg", "未知错误")
            error_code = result.get("code")
            print(f"❌ 创建字段失败 [{field_def['field_name']}]: {error_msg} (错误码: {error_code})")
            return False
    except Exception as e:
        print(f"❌ 创建字段异常 [{field_def['field_name']}]: {e}")
        return False





def fetch_cancel_orders_data(date_filter=None):
    """从MySQL读取取消订单数据"""
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 基础查询语句
        sql = """
        SELECT 
            global_cancel_time,
            order_status,
            platform_order_no,
            store_id,
            store_full_name
        FROM orders_merge 
        WHERE order_status = 7
        AND global_cancel_time IS NOT NULL 
        AND global_cancel_time != ''
        """

        # 添加日期过滤条件
        if date_filter:
            sql += f" AND global_cancel_time > '{date_filter}'"
            print(f"🔍 使用日期过滤条件: > {date_filter}")

        cursor.execute(sql)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        print(f"✅ 读取到 {len(rows)} 条取消订单记录 (order_status=7)")
        return rows

    except Exception as e:
        print(f"❌ 读取取消订单数据失败: {e}")
        return []


def filter_and_validate_cancel_orders(rows):
    """筛选和验证取消订单数据"""
    if not rows:
        return []

    valid_records = []
    invalid_count = 0

    for row in rows:
        # 验证order_status是否为7
        if row.get('order_status') != 7:
            invalid_count += 1
            continue

        # 验证必要字段是否存在
        required_fields = ['platform_order_no', 'store_id', 'global_cancel_time']
        if not all(row.get(field) for field in required_fields):
            invalid_count += 1
            continue

        valid_records.append(row)

    if invalid_count > 0:
        print(f"⚠️  过滤掉 {invalid_count} 条无效记录，保留 {len(valid_records)} 条有效记录")

    return valid_records


def convert_to_cancel_orders_format(rows):
    """将MySQL数据转换为取消订单格式"""
    records = []

    for row in rows:
        cancel_time = row.get('global_cancel_time', '')

        # 统一日期时间处理逻辑
        if cancel_time:
            try:
                # 如果是datetime对象，直接格式化
                if isinstance(cancel_time, (datetime, date)):
                    date_part = cancel_time.strftime('%Y-%m-%d')
                    time_part = cancel_time.strftime('%Y-%m-%d %H:%M:%S')
                # 如果是字符串，尝试解析
                elif isinstance(cancel_time, str):
                    # 清理字符串中的异常字符
                    cancel_time = cancel_time.strip()

                    # 尝试解析常见日期格式
                    parsed = False
                    for fmt in ['%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S',
                                '%Y-%m-%d', '%Y/%m/%d']:
                        try:
                            dt = datetime.strptime(cancel_time, fmt)
                            date_part = dt.strftime('%Y-%m-%d')
                            time_part = dt.strftime('%Y-%m-%d %H:%M:%S')
                            parsed = True
                            break
                        except ValueError:
                            continue

                    if not parsed:
                        # 如果所有格式都失败，尝试智能解析
                        if ' ' in cancel_time:
                            date_str, time_str = cancel_time.split(' ', 1)
                            date_part = date_str.replace('/', '-')
                            # 确保时间部分有完整的格式
                            if ':' in time_str:
                                time_parts = time_str.split(':')
                                if len(time_parts) == 2:
                                    time_str += ':00'  # 添加秒部分
                            time_part = f"{date_part} {time_str}"
                        else:
                            date_part = cancel_time.replace('/', '-')
                            time_part = f"{date_part} 00:00:00"
                else:
                    # 其他类型直接转换为字符串
                    date_part = str(cancel_time)
                    time_part = str(cancel_time)

            except Exception as e:
                print(f"⚠️ 日期处理警告: {e}, 使用原始值: {cancel_time}")
                date_part = str(cancel_time)
                time_part = str(cancel_time)
        else:
            date_part = ''
            time_part = ''

        # 确保日期格式统一为YYYY-MM-DD
        date_part = date_part.replace('/', '-') if date_part else ''

        # 提取店铺名称
        full_store_name = row.get('store_full_name', '')
        store_name = extract_store_name(full_store_name)

        record = {
            "对应日期": date_part,
            "平台订单号": row.get('platform_order_no', ''),
            "对应店铺id": row.get('store_id', ''),
            "对应店铺名称": store_name,
            "订单状态": "申请取消",
            "取消时间": time_part,
            "对应负责人": "",
            "是否跟进": ""
        }

        records.append(record)

    return records


def batch_insert_cancel_orders(token, records, batch_size=50):
    """批量插入取消订单记录"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_Token}/tables/{CANCEL_ORDERS_TABLE_ID}/records/batch_create"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    total = len(records)
    success_count = 0

    if total == 0:
        print("✅ 没有需要插入的取消订单记录")
        return 0

    print(f"📊 开始批量插入 {total} 条取消订单记录，每批 {batch_size} 条")

    # 先测试插入少量数据
    test_records = records[:5] if len(records) > 5 else records
    print(f"🔍 测试插入前 {len(test_records)} 条记录...")

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total + batch_size - 1) // batch_size

        print(f"📦 处理第 {batch_num}/{total_batches} 批，共 {len(batch)} 条记录")

        # 数据清理和验证
        batch_records = []
        for record in batch:
            cleaned_record = {}
            for key, value in record.items():
                # 处理空值
                if value is None:
                    cleaned_record[key] = ""
                else:
                    # 确保所有值都是字符串
                    cleaned_record[key] = str(value).strip()

            # 打印第一条记录用于调试
            if not batch_records:
                print(f"🔍 第一条记录样例: {cleaned_record}")

            batch_records.append({"fields": cleaned_record})

        payload = {"records": batch_records}

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=30)
                result = response.json()

                if result.get("code") == 0:
                    batch_success = len(result.get("data", {}).get("records", []))
                    success_count += batch_success
                    print(f"✅ 第 {batch_num} 批成功插入: {batch_success} 条")
                    break  # 成功，跳出重试循环
                else:
                    error_msg = result.get("msg", "未知错误")
                    error_code = result.get("code")
                    print(f"❌ 第 {batch_num} 批失败 (尝试 {attempt + 1}/{max_retries}): {error_msg} (错误码: {error_code})")

                    # 如果是字段转换错误，提供详细诊断
                    if "DatetimeFieldConvFail" in error_msg or error_code == 1254064:
                        print("💡 日期字段转换失败诊断:")
                        print("   - 检查飞书表格中字段的实际类型")
                        print("   - 确保日期格式为 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")
                        print("   - 确认字段不是日期类型而是文本类型")

                        # 获取字段详情进行诊断
                        field_details = get_existing_fields_with_details(token, CANCEL_ORDERS_TABLE_ID)
                        for field_name in ["对应日期", "取消时间"]:
                            if field_name in field_details:
                                detail = field_details[field_name]
                                print(f"   - {field_name}: 类型 {detail['type']}, 属性 {detail.get('property', {})}")

                    # 如果是字段不存在错误
                    if "FieldNameNotFound" in error_msg:
                        print("💡 字段不存在诊断:")
                        print("   - 检查字段名是否正确")
                        print("   - 确认字段已创建")

                    # 等待后重试
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 2
                        print(f"⏳ 等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                        continue

            except Exception as e:
                print(f"❌ 第 {batch_num} 批异常: {e}")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    print(f"⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue

        # 批次间延迟
        if i + batch_size < total:
            time.sleep(2)

    print(f"🎯 取消订单数据插入完成: 成功 {success_count}/{total} 条")
    return success_count


def check_existing_cancel_orders(token, platform_order_nos):
    """检查已存在的取消订单记录（基于平台订单号）"""
    existing_orders = set()
    page_token = None

    # 飞书API每次最多返回100条记录，需要分页获取
    while True:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_Token}/tables/{CANCEL_ORDERS_TABLE_ID}/records"
        headers = {"Authorization": f"Bearer {token}"}

        params = {"page_size": 100}
        if page_token:
            params["page_token"] = page_token

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            result = response.json()

            if result.get("code") == 0:
                records = result.get("data", {}).get("items", [])
                if not records:
                    break

                # 提取平台订单号字段值
                for record in records:
                    fields = record.get("fields", {})
                    order_no = fields.get("平台订单号")
                    if order_no and order_no in platform_order_nos:
                        existing_orders.add(order_no)

                # 检查是否有下一页
                page_token = result.get("data", {}).get("page_token")
                if not page_token:
                    break

                time.sleep(0.2)
            else:
                break

        except Exception as e:
            print(f"❌ 检查已存在记录异常: {e}")
            break

    return existing_orders


def filter_duplicate_cancel_orders(records):
    """过滤掉平台订单号已存在的记录"""
    if not records:
        return []

    # 提取所有平台订单号
    platform_order_nos = set()
    for record in records:
        order_no = record.get("平台订单号")
        if order_no:
            platform_order_nos.add(order_no)

    if not platform_order_nos:
        return records

    # 获取访问令牌
    try:
        token = get_tenant_access_token()
        existing_orders = check_existing_cancel_orders(token, platform_order_nos)
    except:
        existing_orders = set()

    # 过滤记录
    unique_records = []
    duplicate_count = 0

    for record in records:
        order_no = record.get("平台订单号")
        if order_no and order_no in existing_orders:
            duplicate_count += 1
        else:
            unique_records.append(record)

    if duplicate_count > 0:
        print(f"🔄 过滤掉 {duplicate_count} 条重复记录，剩余 {len(unique_records)} 条唯一记录")

    return unique_records


def write_cancel_orders_to_feishu():
    """主函数：将取消订单数据写入飞书"""
    print("🚀 开始取消订单数据同步流程")
    print("=" * 50)

    try:
        # 1. 获取访问令牌
        print("1. 获取飞书访问令牌...")
        token = get_tenant_access_token()
        print("✅ Token获取成功")

        # 2. 创建字段
        print("2. 检查/创建字段...")
        fields_created = create_cancel_orders_fields(token)
        if not fields_created:
            print("❌ 字段创建过程中发生错误，流程结束")
            return False
        else:
            print("✅ 字段检查/创建完成")

        # 3. 读取MySQL数据（添加日期过滤）
        print("3. 读取MySQL取消订单数据...")
        today_date = datetime.now().strftime('%Y-%m-%d 00:00:00')
        mysql_rows = fetch_cancel_orders_data(date_filter=today_date)

        if not mysql_rows:
            print("✅ 没有取消订单数据需要同步")
            return True

        # 4. 筛选和验证数据
        print("4. 筛选和验证数据 (order_status=7)...")
        valid_rows = filter_and_validate_cancel_orders(mysql_rows)

        if not valid_rows:
            print("❌ 没有有效的取消订单数据需要同步")
            return False

        # 5. 数据格式转换
        print("5. 转换数据格式...")
        feishu_records = convert_to_cancel_orders_format(valid_rows)
        print(f"✅ 成功转换 {len(feishu_records)} 条记录")

        # 6. 去重检查
        print("6. 执行去重检查...")
        unique_records = filter_duplicate_cancel_orders(feishu_records)

        if not unique_records:
            print("🎉 所有取消订单数据都已存在，无需插入新记录")
            return True

        # 7. 插入数据到飞书
        print("7. 插入数据到飞书多维表格...")
        success_count = batch_insert_cancel_orders(token, unique_records)

        # 8. 结果统计
        print("\n" + "=" * 50)
        if success_count > 0:
            print(f"🎉 取消订单数据同步完成! 成功插入 {success_count} 条记录")
        else:
            print("❌ 取消订单数据同步失败")

        return success_count > 0

    except Exception as e:
        print(f"\n💥 流程执行异常: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # 单独运行取消订单同步
    write_cancel_orders_to_feishu()