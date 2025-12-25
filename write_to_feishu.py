import pymysql
import requests
import time
import json
import decimal
from datetime import datetime, date
from decimal import Decimal

# ================== 配置部分 ==================
APP_ID = "cli_a9bc132c7af81bc7"
APP_SECRET = "0xpxP8mp9Iu5kpymCGQ5FeAujRhAYAfB"
APP_Token = "NYd4bZZ8vagln2szwWec5gbhnoh"  # app_token
TABLE_ID = "tblPr0mGcW0iXlCh"  # table_id

MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "d15c76a0875e73c0",
    "database": "lingxing_orders",
    "charset": "utf8mb4"
}

MYSQL_TABLE = "orders_merge"

# 直接使用英文字段名列表
FIELD_NAMES = [
    "global_order_no", "reference_no", "store_id", "order_from_name",
    "delivery_type", "split_type", "order_status", "global_purchase_time",
    "global_payment_time", "global_review_time", "global_distribution_time",
    "global_print_time", "global_mark_time", "global_delivery_time",
    "amount_currency", "global_latest_ship_time", "global_cancel_time",
    "update_time", "order_tag", "pending_order_tag", "exception_order_tag",
    "wid", "warehouse_name", "original_global_order_no", "supplier_id",
    "is_delete", "order_custom_fields", "global_create_time",
    "logistics_type_id", "logistics_type_name", "logistics_provider_id",
    "logistics_provider_name", "actual_carrier", "waybill_no", "pre_weight",
    "pre_fee_weight", "pre_fee_weight_unit", "pre_pkg_length", "pre_pkg_height",
    "pre_pkg_width", "weight", "pkg_fee_weight", "pkg_fee_weight_unit",
    "pkg_length", "pkg_width", "pkg_height", "weight_unit", "pkg_size_unit",
    "cost_currency_code", "pre_cost_amount", "cost_amount", "logistics_time",
    "tracking_no", "mark_no", "global_item_no", "item_id", "platform_order_no",
    "order_item_no", "item_from_name", "msku", "local_sku", "product_no",
    "local_product_name", "is_bundled", "title", "variant_attr", "unit_price_amount",
    "item_price_amount", "quantity", "platform_status", "item_type", "stock_cost_amount",
    "wms_outbound_cost_amount", "stock_deduct_id", "stock_deduct_name", "cg_price_amount",
    "shipping_amount", "wms_shipping_price_amount", "customer_shipping_amount",
    "discount_amount", "customer_tip_amount", "tax_amount", "sales_revenue_amount",
    "transaction_fee_amount", "other_amount", "customized_url", "platform_subsidy_amount",
    "cod_amount", "gift_wrap_amount", "platform_tax_amount", "points_granted_amount",
    "other_fee", "delivery_time", "source_name", "data_json", "item_custom_fields"
]


# 动态字段类型映射函数
def get_field_type(field_name):
    """根据字段名确定字段类型"""
    # 带_time后缀的字段使用文本类型 (1)
    if field_name.endswith('_time'):
        return 1

    # 数字类型字段关键词识别
    num_keywords = ['amount', 'weight', 'price', 'cost', 'fee', 'quantity',
                    'number', 'count', 'total', 'sum', 'avg', 'average',
                    'max', 'min', 'rate', 'ratio', 'percent', 'percentage']

    for keyword in num_keywords:
        if keyword in field_name.lower():
            return 2  # 数字类型

    # 标识字段使用文本类型
    id_keywords = ['_id', '_no', 'id_', 'no_']
    for keyword in id_keywords:
        if keyword in field_name.lower():
            return 1  # 文本类型

    # 布尔类型字段
    bool_keywords = ['is_', 'has_', 'can_', 'enable', 'disable', 'active']
    for keyword in bool_keywords:
        if keyword in field_name.lower():
            return 1  # 文本类型（飞书没有专门的布尔类型，用文本表示）

    # 默认使用文本类型
    return 1


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


def get_existing_fields(token):
    """获取表格中现有的字段信息（包含类型）"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_Token}/tables/{TABLE_ID}/fields"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, headers=headers)
        result = response.json()

        if result.get("code") == 0:
            fields = result.get("data", {}).get("items", [])
            field_info = {}
            for field in fields:
                field_name = field.get("field_name")
                field_type = field.get("type")
                field_info[field_name] = {
                    "type": field_type,
                    "ui_type": field.get("ui_type"),
                    "field_id": field.get("field_id")
                }
            print(f"✅ 获取到现有字段: {len(field_info)} 个")
            return field_info
        else:
            error_msg = result.get("msg", "未知错误")
            error_code = result.get("code")
            print(f"❌ 获取字段失败: {error_msg} (错误码: {error_code})")
            return {}

    except Exception as e:
        print(f"❌ 获取字段异常: {e}")
        return {}


def create_field(token, field_name, field_type=1):
    """创建新字段"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_Token}/tables/{TABLE_ID}/fields"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # 根据字段类型设置不同的参数
    if field_type == 1:  # 文本类型
        payload = {
            "field_name": field_name,
            "type": 1
        }
    elif field_type == 2:  # 数字类型
        payload = {
            "field_name": field_name,
            "type": 2,
            "property": {
                "formatter": "0.00",  # 保留2位小数
                "precision": 2,
                "decimal_symbol": "."
            }
        }
    else:
        payload = {
            "field_name": field_name,
            "type": 1
        }

    try:
        print(f"正在创建字段: {field_name} (类型: {field_type})")
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        result = response.json()
        if result.get("code") == 0:
            print(f"✅ 成功创建字段: {field_name} (类型: {field_type})")
            return True
        else:
            error_msg = result.get("msg", "未知错误")
            error_code = result.get("code")
            print(f"❌ 创建字段失败 [{field_name}]: {error_msg} (错误码: {error_code})")

            # 根据错误码提供具体建议
            if error_code == 1254040:
                print(f"💡 建议: 表格可能不存在，请检查 TABLE_ID 是否正确")
            elif error_code == 99991400:
                print(f"💡 建议: 应用权限不足，请检查应用是否有多维表格编辑权限")
            elif "date" in error_msg.lower():
                print(f"💡 建议: 日期字段配置可能有问题，检查日期格式")

            return False

    except Exception as e:
        print(f"❌ 创建字段异常 [{field_name}]: {e}")
        return False


def create_missing_fields(token):
    """创建缺失的字段"""
    print("🔍 开始检查并创建缺失字段...")

    # 获取现有字段
    existing_fields = get_existing_fields(token)
    existing_field_names = list(existing_fields.keys())

    # 需要创建的字段（直接使用英文字段名）
    required_fields = FIELD_NAMES

    # 找出缺失的字段
    missing_fields = [field for field in required_fields if field not in existing_field_names]
    if not missing_fields:
        print("✅ 所有字段已存在，无需创建")
        return True

    print(f"📋 需要创建 {len(missing_fields)} 个缺失字段:")
    for field in missing_fields:
        print(f"   - {field}")

    # 批量创建字段
    success_count = 0
    for field_name in missing_fields:
        # 动态确定字段类型
        field_type = get_field_type(field_name)
        if create_field(token, field_name, field_type):
            success_count += 1
        time.sleep(0.5)

    print(f"🎯 字段创建完成: 成功 {success_count}/{len(missing_fields)} 个")
    return success_count == len(missing_fields)


# ================== 新增功能：重复数据检查 ==================
def get_existing_global_item_nos(token, max_records=10000):
    """
    获取飞书多维表格中已存在的global_item_no值
    用于去重检查
    """
    print("🔍 检查飞书表格中已存在的记录...")
    existing_items = set()
    page_token = None
    total_retrieved = 0

    # 飞书API每次最多返回100条记录，需要分页获取
    while total_retrieved < max_records:
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_Token}/tables/{TABLE_ID}/records"
        headers = {"Authorization": f"Bearer {token}"}

        params = {"page_size": 100}  # 每次获取100条
        if page_token:
            params["page_token"] = page_token

        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            result = response.json()

            if result.get("code") == 0:
                records = result.get("data", {}).get("items", [])
                if not records:
                    break

                # 提取global_item_no字段值
                for record in records:
                    fields = record.get("fields", {})
                    global_item_no = fields.get("global_item_no")
                    if global_item_no:
                        existing_items.add(str(global_item_no))

                total_retrieved += len(records)
                print(f"✅ 已检索 {total_retrieved} 条记录，发现 {len(existing_items)} 个唯一global_item_no")

                # 检查是否有下一页
                page_token = result.get("data", {}).get("page_token")
                if not page_token:
                    break

                # 避免请求过快
                time.sleep(0.2)

            else:
                error_msg = result.get("msg", "未知错误")
                error_code = result.get("code")
                print(f"❌ 获取记录失败: {error_msg} (错误码: {error_code})")
                break

        except Exception as e:
            print(f"❌ 获取记录异常: {e}")
            break

    print(f"🎯 去重检查完成: 共发现 {len(existing_items)} 个已存在的global_item_no")
    return existing_items


def filter_duplicate_records(feishu_records, existing_global_item_nos):
    """
    过滤掉global_item_no已存在的记录
    """
    unique_records = []
    duplicate_count = 0

    for record in feishu_records:
        global_item_no = record.get("global_item_no")
        if global_item_no and str(global_item_no) in existing_global_item_nos:
            duplicate_count += 1
        else:
            unique_records.append(record)

    if duplicate_count > 0:
        print(f"🔄 过滤掉 {duplicate_count} 条重复记录，剩余 {len(unique_records)} 条唯一记录")

    return unique_records


# ================== 数据转换函数 ==================
class CustomJSONEncoder(json.JSONEncoder):
    """自定义JSON编码器"""

    def default(self, obj):
        if isinstance(obj, (decimal.Decimal, Decimal)):
            return float(obj)
        elif isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        else:
            return super(CustomJSONEncoder, self).default(obj)


def safe_json_dumps(data):
    """安全序列化"""
    return json.dumps(data, cls=CustomJSONEncoder, ensure_ascii=False)


def convert_value_for_feishu(value, field_name=None):
    """值类型转换，根据字段类型处理 - 增强调试版"""
    if value is None:
        print(f"⚠️  字段 {field_name} 的值为 None")
        return None

    # 动态获取字段类型
    field_type = get_field_type(field_name) if field_name else 1

    # 特别监控有问题的字段
    debug_fields = ["cost_currency_code", "weight_unit","amount_currency","pre_fee_weight_unit","pkg_fee_weight_unit"]
    is_debug = field_name in debug_fields

    if is_debug:
        print(f"🔍 调试 {field_name}: 原始值={value}, 类型={type(value)}, 目标字段类型={field_type}")

    try:
        # 数字类型处理
        if field_type == 2:
            if isinstance(value, (decimal.Decimal, Decimal)):
                result = float(value)
            elif isinstance(value, (int, float)):
                result = value
            elif isinstance(value, str):
                try:
                    result = float(value.strip())
                except:
                    if is_debug:
                        print(f"❌ {field_name} 字符串转数字失败: {value}")
                    return None
            else:
                if is_debug:
                    print(f"❌ {field_name} 不支持的数字类型: {type(value)}")
                return None

        # 文本类型处理
        else:
            if isinstance(value, (decimal.Decimal, Decimal)):
                result = str(value)
            elif isinstance(value, bytes):
                result = value.decode('utf-8', errors='ignore')
            elif value is None:
                result = ""
            else:
                result = str(value)

        if is_debug:
            print(f"✅ {field_name} 转换成功: {value} -> {result}")

        return result

    except Exception as e:
        print(f"❌ 转换值失败 [{field_name}]: {value} -> {e}")
        return None


# ================== 增强错误检测的批量插入函数 ==================
def analyze_feishu_error(error_code, error_msg, batch_data_sample=None):
    """分析飞书API错误并返回具体原因和建议"""
    error_analysis = {
        "code": error_code,
        "message": error_msg,
        "possible_causes": [],
        "suggestions": []
    }

    # 基于常见错误码的分析 [1,3](@ref)
    if error_code == 99991400:
        error_analysis["possible_causes"] = ["APP_ID或APP_SECRET错误", "应用权限不足"]
        error_analysis["suggestions"] = [
            "检查APP_ID和APP_SECRET是否正确",
            "在飞书开放平台确认应用已开通多维表格相关权限",
            "确认应用已发布版本"
        ]
    elif error_code == 1254040:
        error_analysis["possible_causes"] = ["表格不存在或无法访问"]
        error_analysis["suggestions"] = [
            "检查APP_Token和TABLE_ID是否正确",
            "确认应用有该表格的访问权限",
            "在飞书多维表格中确认表格存在"
        ]
    elif error_code == 1254020 or "field" in error_msg.lower():
        error_analysis["possible_causes"] = ["字段不存在或字段类型不匹配", "数据格式错误"]
        error_analysis["suggestions"] = [
            "检查字段名是否正确拼写",
            "确认字段已创建且类型匹配",
            "检查数据值是否符合字段类型要求"
        ]
    elif "rate limit" in error_msg.lower() or "too many" in error_msg.lower():
        error_analysis["possible_causes"] = ["API调用频率超限"]
        error_analysis["suggestions"] = [
            "减少批量大小或增加请求间隔",
            "飞书API频率限制为50次/秒，请控制调用频率 [6](@ref)"
        ]
    elif "date" in error_msg.lower() or "time" in error_msg.lower():
        error_analysis["possible_causes"] = ["日期时间格式错误"]
        error_analysis["suggestions"] = [
            "确认日期时间字段格式为YYYY-MM-DD HH:mm:ss",
            "检查时间值是否在合理范围内"
        ]
    elif "number" in error_msg.lower() or "numeric" in error_msg.lower():
        error_analysis["possible_causes"] = ["数字格式错误"]
        error_analysis["suggestions"] = [
            "检查数字字段是否包含非数字字符",
            "确认数字值在合理范围内"
        ]
    else:
        error_analysis["possible_causes"] = ["未知错误，需要进一步排查"]
        error_analysis["suggestions"] = [
            "查看飞书官方API文档错误码说明",
            "检查网络连接和认证信息",
            "尝试减少批量大小重新执行"
        ]

    return error_analysis


def batch_insert_records(token, records, batch_size=500):  # 改为500条/批
    """批量插入记录 - 增强错误检测版"""
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_Token}/tables/{TABLE_ID}/records/batch_create"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    total = len(records)
    success_count = 0

    if total == 0:
        print("✅ 没有需要插入的新记录")
        return 0

    print(f"📊 开始批量插入 {total} 条记录，每批 {batch_size} 条")

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (total + batch_size - 1) // batch_size

        print(f"\n📦 处理第 {batch_num}/{total_batches} 批，共 {len(batch)} 条记录")

        # 转换批次数据
        batch_records = []
        for j, record in enumerate(batch):
            processed_fields = {}

            for key, value in record.items():
                processed_value = convert_value_for_feishu(value, key)
                if processed_value is not None:
                    processed_fields[key] = processed_value

            if processed_fields:
                batch_records.append({"fields": processed_fields})

        if not batch_records:
            print(f"⚠️  第 {batch_num} 批没有有效数据")
            continue

        payload = {"records": batch_records}

        max_retries = 3
        for attempt in range(max_retries):
            try:
                json_payload = safe_json_dumps(payload)
                response = requests.post(url, headers=headers, data=json_payload.encode('utf-8'), timeout=60)
                result = response.json()

                if result.get("code") == 0:
                    batch_success = len(result.get("data", {}).get("records", []))
                    success_count += batch_success
                    print(f"✅ 第 {batch_num} 批成功插入: {batch_success} 条")

                    # 显示进度
                    progress = min(i + len(batch), total)
                    print(f"📈 总体进度: {progress}/{total} ({progress / total * 100:.1f}%)")
                    break  # 成功，跳出重试循环

                else:
                    error_msg = result.get("msg", "未知错误")
                    error_code = result.get("code")

                    # 详细错误分析
                    error_analysis = analyze_feishu_error(error_code, error_msg,
                                                          batch_records[0] if batch_records else None)

                    print(f"❌ 第 {batch_num} 批失败 (尝试 {attempt + 1}/{max_retries}):")
                    print(f"   错误码: {error_code}")
                    print(f"   错误信息: {error_msg}")
                    print(f"   可能原因: {', '.join(error_analysis['possible_causes'])}")
                    print(f"   建议: {', '.join(error_analysis['suggestions'])}")

                    # 如果是频率限制错误，等待后重试
                    if "rate limit" in error_msg.lower() or "too many" in error_msg.lower():
                        wait_time = (attempt + 1) * 5  # 指数退避：5, 10, 15秒
                        print(f"⏳ 遇到频率限制，等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                        continue
                    else:
                        # 非重试错误，直接跳出
                        break

            except requests.exceptions.Timeout:
                print(f"❌ 第 {batch_num} 批请求超时 (尝试 {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 3
                    print(f"⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                else:
                    print("❌ 重试次数已用尽，跳过本批次")
                    break

            except requests.exceptions.ConnectionError:
                print(f"❌ 第 {batch_num} 批网络连接错误 (尝试 {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 5
                    print(f"⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                else:
                    print("❌ 重试次数已用尽，跳过本批次")
                    break

            except Exception as e:
                print(f"❌ 第 {batch_num} 批异常: {e}")
                break

        # 批次间延迟，避免触发频率限制
        if i + batch_size < total:  # 不是最后一批
            delay_seconds = 2  # 500条批次用2秒延迟
            print(f"⏳ 等待 {delay_seconds} 秒后处理下一批...")
            time.sleep(delay_seconds)

    print(f"\n🎯 批量插入完成: 成功 {success_count}/{total} 条")
    return success_count


# ================== MySQL数据读取 ==================
def fetch_mysql_data(limit=None):
    """从MySQL读取数据"""
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 只选择指定的字段
        selected_fields = ','.join(FIELD_NAMES)
        sql = f"SELECT {selected_fields} FROM {MYSQL_TABLE}"

        if limit:
            sql += f" LIMIT {limit}"

        cursor.execute(sql)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"❌ 读取MySQL数据失败: {e}")
        return []


def convert_mysql_to_feishu_format(rows):
    """将MySQL数据转换为飞书格式"""
    records = []

    for i, row in enumerate(rows):
        fields = {}
        for field_name in FIELD_NAMES:
            value = row.get(field_name)
            fields[field_name] = value
        records.append(fields)

    return records


# ================== 优化后的主流程 ==================
def main():
    """主执行函数 - 增强错误检测版"""
    print("🚀 开始飞书多维表格数据同步流程（增强错误检测版）")
    print("=" * 60)
    print(f"数据源: {MYSQL_TABLE}")
    print(f"目标表: {TABLE_ID}")
    print(f"批量大小: 500条/批")  # 更新为500条
    print(f"去重字段: global_item_no")

    try:
        # 1. 获取访问令牌
        print("\n1. 获取飞书访问令牌...")
        token = get_tenant_access_token()
        print("✅ Token获取成功")

        # 2. 创建缺失字段
        print("\n2. 检查并创建缺失字段...")
        fields_created = create_missing_fields(token)

        # 3. 读取MySQL数据
        print("\n3. 读取MySQL数据...")
        mysql_rows = fetch_mysql_data()
        print(f"✅ 读取到 {len(mysql_rows)} 条MySQL记录")

        if not mysql_rows:
            print("❌ 未读取到数据，流程结束")
            return False

        # 4. 数据格式转换
        print("\n4. 转换数据格式...")
        feishu_records = convert_mysql_to_feishu_format(mysql_rows)
        print(f"✅ 成功转换 {len(feishu_records)} 条记录")

        # 5. 去重检查（新增功能）
        print("\n5. 执行去重检查...")
        existing_global_item_nos = get_existing_global_item_nos(token)
        unique_records = filter_duplicate_records(feishu_records, existing_global_item_nos)

        if not unique_records:
            print("🎉 所有数据都已存在，无需插入新记录")
            return True

        # 6. 插入数据到飞书
        print("\n6. 插入数据到飞书多维表格...")
        success_count = batch_insert_records(token, unique_records, batch_size=500)  # 改为500条

        # 7. 结果统计
        print("\n" + "=" * 60)
        if success_count > 0:
            print(f"🎉 数据同步完成! 成功插入 {success_count}/{len(unique_records)} 条唯一记录")
            if len(feishu_records) > len(unique_records):
                duplicate_count = len(feishu_records) - len(unique_records)
                print(f"🔍 自动跳过 {duplicate_count} 条重复记录")
        else:
            print("❌ 数据同步失败")

        return success_count > 0

    except Exception as e:
        print(f"\n💥 流程执行异常: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    main()