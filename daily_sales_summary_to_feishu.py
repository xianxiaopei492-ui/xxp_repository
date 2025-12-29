import pymysql
import requests
import time
import re
from datetime import datetime, date
from config import  load_config_from_env
from utils import extract_store_name
import traceback

config = load_config_from_env()

# ================== 配置部分 ==================
APP_ID = config['sales_summary_config']['APP_ID']
APP_SECRET = config['sales_summary_config']['APP_SECRET']
APP_Token = config['sales_summary_config']['APP_TOKEN'] # app_token
CANCEL_ORDERS_TABLE_ID = config['sales_summary_config']['CANCEL_ORDERS_TABLE_ID']  # 新的销量汇总表ID

MYSQL_CONFIG = config['db_config']

MYSQL_TABLE = "sales_summary_daily"

# 修正后的销量汇总表字段定义 - 全部使用文本类型
CANCEL_ORDERS_FIELDS = [
    {
        "field_name": "sku",
        "type": 1  # 文本类型
    },
    {
        "field_name": "对应店铺名称",
        "type": 1  # 文本类型
    },
    {
        "field_name": "对应平台名称",
        "type": 1  # 文本类型
    },
    {
        "field_name": "近3天销量",
        "type": 1  # 文本类型
    },
    {
        "field_name": "近7天销量",
        "type": 1,  # 文本类型
    },
    {
        "field_name": "近15天销量",
        "type": 1  # 文本类型
    },
    {
        "field_name": "近30天销量",
        "type": 1  # 文本类型
    },
    {
        "field_name": "总销量",
        "type": 1  # 文本类型
    },
    {
        "field_name": "最后销售日期",
        "type": 1  # 文本类型
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
    """创建销量汇总表的字段"""
    print("🔧 开始创建销量汇总表字段...")

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


def clear_feishu_table(token, table_id):
    """简化版的清空表格函数"""
    try:
        print("🗑️  开始清空飞书表格数据...")
        # 获取所有记录ID
        all_records = []
        page_token = None
        while True:
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_Token}/tables/{table_id}/records"
            headers = {"Authorization": f"Bearer {token}"}
            params = {"page_size": 100, "page_token": page_token} if page_token else {"page_size": 100}
            response = requests.get(url, headers=headers, params=params)
            result = response.json()
            if result.get("code") != 0:
                print(f"❌ 获取记录失败: {result.get('msg')}")
                return False

            records = result.get("data", {}).get("items", [])
            if not records:
                break

            all_records.extend(records)
            page_token = result.get("data", {}).get("page_token")
            if not page_token:
                break

            time.sleep(0.5)

        if not all_records:
            print("✅ 表格为空，无需清理")
            return True

        record_ids = [record["record_id"] for record in all_records if record.get("record_id")]
        print(f"📊 找到 {len(record_ids)} 条记录需要删除")

        # 分批删除（每批50条）
        batch_size = 50
        deleted_count = 0

        for i in range(0, len(record_ids), batch_size):
            batch_ids = record_ids[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(record_ids) + batch_size - 1) // batch_size

            print(f"🔄 删除批次 {batch_num}/{total_batches} ({len(batch_ids)} 条记录)")

            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_Token}/tables/{table_id}/records/batch_delete"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            # 构建请求体
            payload = {
                "records": batch_ids
            }
            response = requests.post(url, headers=headers, json=payload)
            result = response.json()
            print("22222")
            print(result)
            if result.get("code") == 0:
                deleted_count += len(batch_ids)
                print(f"✅ 批次 {batch_num} 删除成功")
            else:
                print(f"❌ 批次 {batch_num} 删除失败: {result.get('msg')}")
                # 尝试单条删除
                for record_id in batch_ids:
                    single_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_Token}/tables/{table_id}/records/{record_id}"
                    single_response = requests.delete(single_url, headers={"Authorization": f"Bearer {token}"})
                    single_result = single_response.json()
                    if single_result.get("code") == 0:
                        deleted_count += 1

            time.sleep(1)  # 批次间延迟

        print(f"✅ 清空完成，共删除 {deleted_count} 条记录")
        return True

    except Exception as e:
        print(f"❌ 清空表格失败: {e}")
        return False

def fetch_cancel_orders_data():
    """从MySQL读取销量数据"""
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 基础查询语句
        sql = """
        SELECT 
            sku,
            store_name,
            platform_name,
            recent_3d_sales,
            recent_7d_sales,
            recent_15d_sales,
            recent_30d_sales,
            total_sales,
            last_sale_date
        FROM sales_summary_daily
        WHERE sku IS NOT NULL 
        AND sku != ''
        """

        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        print(f"✅ 读取到 {len(rows)} 条销量汇总记录 ")
        return rows

    except Exception as e:
        print(f"❌ 读取销量汇总数据失败: {e}")
        return []



def convert_to_cancel_orders_format(rows):
    """将MySQL数据转换为销量汇总格式"""
    records = []

    for row in rows:
        last_sale_date = row.get('last_sale_date', '')

        # 统一日期时间处理逻辑
        if last_sale_date:
            try:
                # 如果是datetime对象，直接格式化
                if isinstance(last_sale_date, (datetime, date)):
                    date_part = last_sale_date.strftime('%Y-%m-%d')
                    time_part = last_sale_date.strftime('%Y-%m-%d %H:%M:%S')
                # 如果是字符串，尝试解析
                elif isinstance(last_sale_date, str):
                    # 清理字符串中的异常字符
                    last_sale_date = last_sale_date.strip()

                    # 尝试解析常见日期格式
                    parsed = False
                    for fmt in ['%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S',
                                '%Y-%m-%d', '%Y/%m/%d']:
                        try:
                            dt = datetime.strptime(last_sale_date, fmt)
                            date_part = dt.strftime('%Y-%m-%d')
                            time_part = dt.strftime('%Y-%m-%d %H:%M:%S')
                            parsed = True
                            break
                        except ValueError:
                            continue

                    if not parsed:
                        # 如果所有格式都失败，尝试智能解析
                        if ' ' in last_sale_date:
                            date_str, time_str = last_sale_date.split(' ', 1)
                            date_part = date_str.replace('/', '-')
                        else:
                            date_part = last_sale_date.replace('/', '-')
                else:
                    # 其他类型直接转换为字符串
                    date_part = str(last_sale_date)

            except Exception as e:
                print(f"⚠️ 日期处理警告: {e}, 使用原始值: {last_sale_date}")
                date_part = str(last_sale_date)

        else:
            date_part = ''

        # 确保日期格式统一为YYYY-MM-DD
        date_part = date_part.replace('/', '-') if date_part else ''
        # 提取店铺名称
        full_store_name = row.get('store_full_name', '')
        store_name = extract_store_name(full_store_name)

        record = {
            "sku": row.get('sku', ''),
            "对应店铺名称": row.get('store_name', ''),
            "对应平台名称": row.get('platform_name', ''),
            "近3天销量":  row.get('recent_3d_sales', ''),
            "近7天销量":  row.get('recent_7d_sales', ''),
            "近15天销量":  row.get('recent_15d_sales', ''),
            "近30天销量":  row.get('recent_30d_sales', ''),
            "总销量":  row.get('total_sales', ''),
            "最后销售日期":date_part
        }

        records.append(record)

    return records


def batch_insert_cancel_orders(token, records, batch_size=50):
    """批量插入销量汇总记录"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_Token}/tables/{CANCEL_ORDERS_TABLE_ID}/records/batch_create"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    total = len(records)
    success_count = 0

    if total == 0:
        print("✅ 没有需要插入的销量汇总记录")
        return 0

    print(f"📊 开始批量插入 {total} 条销量汇总记录，每批 {batch_size} 条")

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

    print(f"🎯 销量汇总数据插入完成: 成功 {success_count}/{total} 条")
    return success_count


def write_sales_summary_to_feishu():
    """主函数：将销量汇总数据写入飞书（包含清空表格逻辑）"""
    print("🚀 开始销量汇总数据同步流程")
    print("=" * 50)

    try:
        # 1. 获取访问令牌
        print("1. 获取飞书访问令牌...")
        token = get_tenant_access_token()
        if not token:
            print("❌ Token获取失败，流程结束")
            return False
        print("✅ Token获取成功")

        # 2. 创建字段
        print("2. 检查/创建字段...")
        fields_created = create_cancel_orders_fields(token)
        if not fields_created:
            print("❌ 字段创建过程中发生错误，流程结束")
            return False
        else:
            print("✅ 字段检查/创建完成")

        # 3. 清空飞书表格中的现有数据（保留字段结构）
        print("3. 清空飞书表格中的现有数据...")
        clear_success = clear_feishu_table(token, CANCEL_ORDERS_TABLE_ID)
        if not clear_success:
            print("❌ 清空表格数据失败，但继续执行数据插入...")
            # 不立即返回，尝试继续插入数据
        else:
            print("✅ 表格数据清空完成")

        # 4. 读取MySQL数据
        print("4. 读取MySQL销量汇总数据...")
        mysql_rows = fetch_cancel_orders_data()
        if not mysql_rows:
            print("✅ 没有销量汇总数据需要同步")
            return True

        print(f"📊 从MySQL读取到 {len(mysql_rows)} 条销量汇总记录")

        # 5. 数据格式转换
        print("5. 转换数据格式...")
        feishu_records = convert_to_cancel_orders_format(mysql_rows)
        if not feishu_records:
            print("❌ 数据格式转换失败，没有可插入的记录")
            return False

        print(f"✅ 成功转换 {len(feishu_records)} 条记录")

        # 6. 插入数据到飞书
        print("6. 插入数据到飞书多维表格...")
        success_count = batch_insert_cancel_orders(token, feishu_records)

        # 7. 结果统计
        print("\n" + "=" * 50)
        print("📊 销量汇总数据同步结果报告")
        print("=" * 50)

        if success_count > 0:
            print(f"🎉 销量汇总数据同步完成!")
            print(f"   - 成功插入: {success_count}/{len(feishu_records)} 条记录")
            print(f"   - 表格已清空并重新填充")

            # 显示前几条记录作为样例
            if feishu_records and len(feishu_records) > 0:
                print(f"   - 数据样例:")
                for i, record in enumerate(feishu_records[:3], 1):
                    sku = record.get('sku', '未知')
                    store = record.get('对应店铺名称', '未知')
                    sales_30d = record.get('近30天销量', 0)
                    print(f"     {i}. {sku} - {store}: 30天销量 {sales_30d}")
        else:
            print("❌ 销量汇总数据同步失败，没有成功插入任何记录")
            return False

        return success_count > 0

    except Exception as e:
        print(f"\n💥 流程执行异常: {e}")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # 单独运行销量汇总同步
    write_sales_summary_to_feishu()