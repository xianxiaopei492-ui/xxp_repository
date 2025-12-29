import  re
import json
def extract_store_name(full_store_name):
    """使用正则表达式提取店铺名称（去掉[保健][美国]等前缀）"""
    if not full_store_name:
        return ""

    # 匹配格式如：[保健][美国]80YML SALE，提取80YML SALE
    pattern = r'\[.*?\]\[.*?\](.*)'
    match = re.search(pattern, full_store_name)

    if match:
        return match.group(1).strip()
    else:
        # 如果没有匹配到预期格式，返回原字符串
        return full_store_name

def extract_from_json(data, default_value, field_name):
    """
    安全地从JSON数据中提取值
    """
    try:
        if data is None:
            return default_value

        if isinstance(data, str) and data.startswith('['):
            try:
                data_list = json.loads(data)
                return data_list[0] if data_list else default_value
            except:
                return default_value
        elif isinstance(data, list):
            return data[0] if data else default_value
        else:
            return str(data) if data else default_value
    except Exception as e:
        print(f"❌ 提取{field_name}失败: {e}")
        return default_value