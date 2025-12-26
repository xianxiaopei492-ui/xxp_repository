import time
import json
import hashlib
import base64
import pandas as pd
import requests
import math
import traceback
from Crypto.Cipher import AES
from dataoperator import DataOperator  # 修改导入
from config import  load_config_from_env





class LingXingAPI:
    """零星开放平台API客户端"""

    def __init__(self, app_id: str, app_secret: str, base_url: str = "https://openapi.lingxing.com"):
        self.APP_ID = app_id
        self.APP_SECRET = app_secret
        self.BASE_URL = base_url

    # ========= AES 工具 =========
    @staticmethod
    def pkcs5_pad(s: str) -> str:
        pad_len = 16 - len(s) % 16
        return s + chr(pad_len) * pad_len

    def aes_encrypt(self, text: str, key: str) -> str:
        cipher = AES.new(key.encode("utf-8"), AES.MODE_ECB)
        padded = self.pkcs5_pad(text).encode("utf-8")
        encrypted = cipher.encrypt(padded)
        return base64.b64encode(encrypted).decode("utf-8")

    # ========= 生成 sign =========
    def generate_sign(self, params: dict) -> str:
        filtered = {k: v for k, v in params.items() if v != ""}
        items = sorted(filtered.items(), key=lambda x: x[0])
        s = "&".join(f"{k}={v}" for k, v in items)
        md5_str = hashlib.md5(s.encode("utf-8")).hexdigest().upper()
        return self.aes_encrypt(md5_str, self.APP_ID)

    # ========= 获取 access_token =========
    def get_access_token(self) -> str:
        url = f"{self.BASE_URL}/api/auth-server/oauth/access-token"
        data = {
            "appId": self.APP_ID,
            "appSecret": self.APP_SECRET
        }

        try:
            resp = requests.post(url, data=data, timeout=10)
            resp.raise_for_status()
            result = resp.json()

            if (result.get("code") in [200, '200']) and "data" in result:
                return result["data"]["access_token"]
            else:
                raise RuntimeError(f"获取token失败: {result}")

        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"网络请求失败: {e}")
        except (KeyError, ValueError) as e:
            raise RuntimeError(f"响应数据解析失败: {e}")

    # ========= 业务 POST 请求 =========
    def api_post(self, api_path: str, biz_body: dict) -> dict:
        access_token = self.get_access_token()
        timestamp = str(int(time.time()))

        biz_for_sign = {}
        for k, v in biz_body.items():
            if isinstance(v, (dict, list)):
                biz_for_sign[k] = json.dumps(v, separators=(",", ":"), ensure_ascii=False)
            else:
                biz_for_sign[k] = v

        sign_params = {
            **biz_for_sign,
            "access_token": access_token,
            "app_key": self.APP_ID,
            "timestamp": timestamp,
        }

        sign = self.generate_sign(sign_params)

        query = {
            "access_token": access_token,
            "app_key": self.APP_ID,
            "timestamp": timestamp,
            "sign": sign,
        }

        headers = {"Content-Type": "application/json"}
        url = self.BASE_URL + api_path

        try:
            resp = requests.post(url, params=query, json=biz_body, headers=headers, timeout=30)
            resp.raise_for_status()
            result = resp.json()

            # 添加调试信息
            print(f"API请求: {url}")
            print(f"请求参数: {biz_body}")
            print(f"响应状态码: {resp.status_code}")
            print(f"响应内容: {result}")

            return result
        except requests.exceptions.RequestException as e:
            print(f"API请求失败: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"响应JSON解析失败: {e}")
            return None


    def fetch_and_process_order_data_batch(self, api_path, base_biz_body, db_config, max_retries=3, delay=1):
        """
        从分页API获取数据并实时分批处理
        Args:
            api_path: API路径
            base_biz_body: 基础请求体参数
            db_config: 数据库配置
            max_retries: 最大重试次数
            delay: 请求延迟
        Returns:
            int: 成功处理的总记录数
        """
        total_processed = 0
        current_offset = 0
        page_size = 500
        request_attempt = 0
        # 初始化数据处理器
        data_operator = DataOperator(db_config)

        try:
            # 连接数据库（在整个处理过程中保持连接）
            data_operator.connect_db()
            print("数据库连接成功，开始分批处理数据...")

            # 1. 首先获取数据总量
            print("正在获取数据总量...")
            biz_body = base_biz_body.copy()
            biz_body.update({"offset": 0, "length": 20})

            try:
                initial_result = self.api_post(api_path, biz_body)
                total_expected = int(initial_result["data"]["total"])
                print(f"数据总量为: {total_expected}")
            except Exception as e:
                print(f"获取数据总量失败: {e}")
                return total_processed

            # 2. 计算总页数
            total_pages = math.ceil(total_expected / page_size)
            print(f"开始分页请求，共需处理 {total_pages} 页数据...")

            # 3. 分批获取和处理数据
            while current_offset < total_expected and request_attempt < max_retries:
                current_page = (current_offset // page_size) + 1
                print(f"正在处理第 {current_page}/{total_pages} 页, Offset: {current_offset}")

                try:
                    # 构建当前页的请求参数
                    biz_body = base_biz_body.copy()
                    biz_body.update({
                        "offset": current_offset,
                        "length": page_size,
                    })

                    # 获取当前批次数据
                    result = self.api_post(api_path, biz_body)
                    current_batch = result["data"]["list"]
                    batch_size = len(current_batch)

                    if batch_size == 0:
                        print("当前页未返回数据，退出循环。")
                        break

                    # 实时处理当前批次数据
                    print(f"  第 {current_page} 页获取成功，本页 {batch_size} 条数据，开始插入数据库...")

                    try:
                        # 使用数据处理器插入当前批次
                        data_operator.insert_orders(current_batch)
                        print(f"  ✓ 第 {current_page} 页数据插入成功")
                        total_processed += batch_size
                    except Exception as e:
                        print(f"  ✗ 第 {current_page} 页数据插入失败: {e}")
                        # 插入失败时回滚事务
                        data_operator.conn.rollback()

                    # 更新偏移量
                    current_offset += batch_size

                    # 重置连续失败计数
                    request_attempt = 0

                    # 请求间隔，避免给API造成压力
                    time.sleep(delay)

                except Exception as e:
                    request_attempt += 1
                    print(f"  第 {current_page} 页请求失败，正在进行第 {request_attempt} 次重试。错误信息: {e}")
                    if request_attempt >= max_retries:
                        print("重试次数已达上限，停止处理。")
                        break
                    time.sleep(delay * 2)

            print(f"所有数据处理完成。预期数据量: {total_expected}，实际成功处理: {total_processed}")
            return total_processed

        except Exception as e:
            print(f"处理过程中发生错误: {e}")
            return total_processed
        finally:
            # 确保数据库连接被关闭
            data_operator.disconnect_db()

    def fetch_and_process_store_data_batch(self, api_path, base_biz_body, db_config, max_retries=3, delay=1):
        """
        从分页API获取store数据并实时分批处理
        """
        total_processed = 0
        current_offset = 0
        page_size = 50  # 调整为大于20的值，避免API限制
        request_attempt = 0
        data_operator = DataOperator(db_config)
        try:
            data_operator.connect_db()
            print("数据库连接成功，开始分批处理数据...")
            # 1. 首先获取数据总量
            print("正在获取数据总量...")
            biz_body = base_biz_body.copy()
            # 验证并设置初始参数
            try:
                biz_body.update({"offset": 0, "length": 20})
            except ValueError as e:
                print(f"参数验证失败: {e}")
                # 使用API允许的最小值
                biz_body.update({"offset": 0, "length": 20})
                print("已自动调整参数为允许的最小值")

            try:
                initial_result = self.api_post(api_path, biz_body)
                # 增强响应结构检查
                if not initial_result:
                    print("API返回空响应")
                    return total_processed
                # 检查API返回的code字段
                api_code = initial_result.get("code")
                if api_code is None:
                    print("API响应缺少code字段")
                    # 尝试检查其他可能的成功标识
                    if initial_result.get("status") == "success" or initial_result.get("success"):
                        print("检测到其他成功标识，继续处理")
                    else:
                        return total_processed
                elif str(api_code) not in ['0', '200', '1000']:  # 根据实际API调整成功码
                    error_msg = initial_result.get("msg", "未知错误")
                    print(f"API返回错误: {error_msg} (代码: {api_code})")

                if "data" not in initial_result or initial_result["data"] is None:
                    print("API返回数据为空")
                    return total_processed

                # 安全地获取total值
                data = initial_result["data"]
                total_str = data.get("total")
                if total_str is None:
                    print("API返回数据缺少total字段")
                    # 尝试从其他字段获取总数
                    total_str = data.get("count", data.get("size", "0"))
                try:
                    total_expected = int(total_str)
                    print(f"数据总量为: {total_expected}")
                except (ValueError, TypeError) as e:
                    print(f"转换total为整数失败: {e}, 原始值: {total_str}")
                    total_expected = 0

            except Exception as e:
                print(f"获取数据总量失败: {e}")
                return total_processed

            # 2. 计算总页数（确保page_size不小于20）
            page_size = max(page_size, 20)  # 确保不小于API最小值
            total_pages = math.ceil(total_expected / page_size) if total_expected > 0 else 0
            print(f"开始分页请求，共需处理 {total_pages} 页数据，每页 {page_size} 条...")
            # 3. 分批获取和处理数据
            while current_offset < total_expected and request_attempt < max_retries:
                current_page = (current_offset // page_size) + 1
                print(f"正在处理第 {current_page}/{total_pages} 页, Offset: {current_offset}")
                try:
                    # 构建当前页的请求参数
                    biz_body = base_biz_body.copy()
                    biz_body.update({
                        "offset": current_offset,
                        "length": page_size,
                    })
                    # 获取当前批次数据
                    result = self.api_post(api_path, biz_body)
                    # 调试信息
                    print(f"API请求参数: offset={current_offset}, length={page_size}")
                    print(f"API响应: {result}")
                    # 增强响应检查
                    if not result:
                        print("API返回空响应，跳过本页")
                        request_attempt += 1
                        time.sleep(delay * 2)
                        continue
                    # 检查API响应码
                    result_code = result.get("code")
                    if result_code is None:
                        print("API响应缺少code字段，尝试检查数据...")
                        # 如果数据存在，尝试继续处理
                        if "data" in result and result["data"]:
                            print("尽管缺少code字段，但存在数据，继续处理")
                        else:
                            request_attempt += 1
                            continue
                    elif str(result_code) not in ['0', '200', '1000']:
                        error_msg = result.get("msg", "未知错误")
                        print(f"API返回错误: {error_msg} (代码: {result_code})")

                    if "data" not in result or result["data"] is None:
                        print("API返回数据为空，跳过本页")
                        request_attempt += 1
                        continue

                    current_batch = result["data"]["list"]

                    batch_size = len(current_batch) if current_batch else 0

                    if batch_size == 0:
                        print("当前页未返回数据，退出循环。")
                        break
                    # 实时处理当前批次数据
                    print(f"  第 {current_page} 页获取成功，本页 {batch_size} 条数据，开始插入数据库...")
                    try:

                        data_operator.insert_stores_table(current_batch)
                        print(f"  ✓ 第 {current_page} 页数据插入成功")
                        total_processed += batch_size
                    except Exception as e:
                        print(f"  ✗✗ 第 {current_page} 页数据插入失败: {e}")
                        data_operator.conn.rollback()
                        request_attempt += 1
                        continue

                    # 更新偏移量
                    current_offset += batch_size
                    request_attempt = 0  # 重置重试计数
                    time.sleep(delay)

                except ValueError as e:
                    # 参数验证错误
                    print(f"参数错误: {e}")
                    request_attempt += 1
                    if request_attempt >= max_retries:
                        print("参数错误重试次数已达上限，停止处理。")
                        break
                    time.sleep(delay * 2)

                except Exception as e:
                    request_attempt += 1
                    print(f"  第 {current_page} 页请求失败，正在进行第 {request_attempt} 次重试。错误信息: {e}")
                    if request_attempt >= max_retries:
                        print("重试次数已达上限，停止处理。")
                        break
                    time.sleep(delay * 2)

            print(f"所有数据处理完成。预期数据量: {total_expected}，实际成功处理: {total_processed}")
            return total_processed

        except Exception as e:
            print(f"处理过程中发生错误: {e}")

            traceback.print_exc()
            return total_processed
        finally:
            data_operator.disconnect_db()

    def fetch_and_process_invetory_data_batch(self, api_path, base_biz_body, db_config, max_retries=3, delay=1):
        """
        从分页API获取invetory数据并实时分批处理
        """
        total_processed = 0
        current_offset = 0
        page_size = 50  # 调整为大于20的值，避免API限制
        request_attempt = 0
        data_operator = DataOperator(db_config)
        try:
            data_operator.connect_db()
            print("数据库连接成功，开始分批处理数据...")
            # 1. 首先获取数据总量
            print("正在获取数据总量...")
            biz_body = base_biz_body.copy()
            # 验证并设置初始参数
            try:
                biz_body.update({"offset": 0, "length": 20})
            except ValueError as e:
                print(f"参数验证失败: {e}")
                # 使用API允许的最小值
                biz_body.update({"offset": 0, "length": 20})
                print("已自动调整参数为允许的最小值")

            try:
                initial_result = self.api_post(api_path, biz_body)
                # 增强响应结构检查
                if not initial_result:
                    print("API返回空响应")
                    return total_processed
                # 检查API返回的code字段
                api_code = initial_result.get("code")
                if api_code is None:
                    print("API响应缺少code字段")
                    # 尝试检查其他可能的成功标识
                    if initial_result.get("status") == "success" or initial_result.get("success"):
                        print("检测到其他成功标识，继续处理")
                    else:
                        return total_processed
                elif str(api_code) not in ['0', '200', '1000']:  # 根据实际API调整成功码
                    error_msg = initial_result.get("msg", "未知错误")
                    print(f"API返回错误: {error_msg} (代码: {api_code})")

                if "data" not in initial_result or initial_result["data"] is None:
                    print("API返回数据为空")
                    return total_processed

                # 安全地获取total值
                data = initial_result
                total_str = data.get("total")
                if total_str is None:
                    print("API返回数据缺少total字段")
                    # 尝试从其他字段获取总数
                    total_str = data.get("count", data.get("size", "0"))
                try:
                    total_expected = int(total_str)
                    print(f"数据总量为: {total_expected}")
                except (ValueError, TypeError) as e:
                    print(f"转换total为整数失败: {e}, 原始值: {total_str}")
                    total_expected = 0

            except Exception as e:
                print(f"获取数据总量失败: {e}")
                return total_processed

            # 2. 计算总页数（确保page_size不小于20）
            page_size = max(page_size, 20)  # 确保不小于API最小值
            total_pages = math.ceil(total_expected / page_size) if total_expected > 0 else 0
            print(f"开始分页请求，共需处理 {total_pages} 页数据，每页 {page_size} 条...")
            # 3. 分批获取和处理数据
            while current_offset < total_expected and request_attempt < max_retries:
                current_page = (current_offset // page_size) + 1
                print(f"正在处理第 {current_page}/{total_pages} 页, Offset: {current_offset}")
                try:
                    # 构建当前页的请求参数
                    biz_body = base_biz_body.copy()
                    biz_body.update({
                        "offset": current_offset,
                        "length": page_size,
                    })
                    # 获取当前批次数据
                    result = self.api_post(api_path, biz_body)
                    # 调试信息
                    print(f"API请求参数: offset={current_offset}, length={page_size}")
                    print(f"API响应: {result}")
                    # 增强响应检查
                    if not result:
                        print("API返回空响应，跳过本页")
                        request_attempt += 1
                        time.sleep(delay * 2)
                        continue
                    # 检查API响应码
                    result_code = result.get("code")
                    if result_code is None:
                        print("API响应缺少code字段，尝试检查数据...")
                        # 如果数据存在，尝试继续处理
                        if "data" in result and result["data"]:
                            print("尽管缺少code字段，但存在数据，继续处理")
                        else:
                            request_attempt += 1
                            continue
                    elif str(result_code) not in ['0', '200', '1000']:
                        error_msg = result.get("msg", "未知错误")
                        print(f"API返回错误: {error_msg} (代码: {result_code})")

                    if "data" not in result or result["data"] is None:
                        print("API返回数据为空，跳过本页")
                        request_attempt += 1
                        continue

                    current_batch = result["data"]

                    batch_size = len(current_batch) if current_batch else 0

                    if batch_size == 0:
                        print("当前页未返回数据，退出循环。")
                        break
                    # 实时处理当前批次数据
                    print(f"  第 {current_page} 页获取成功，本页 {batch_size} 条数据，开始插入数据库...")
                    try:

                        data_operator.insert_inventory_table(current_batch)
                        print(f"  ✓ 第 {current_page} 页数据插入成功")
                        total_processed += batch_size
                    except Exception as e:
                        print(f"  ✗✗ 第 {current_page} 页数据插入失败: {e}")
                        data_operator.conn.rollback()
                        request_attempt += 1
                        continue

                    # 更新偏移量
                    current_offset += batch_size
                    request_attempt = 0  # 重置重试计数
                    time.sleep(delay)

                except ValueError as e:
                    # 参数验证错误
                    print(f"参数错误: {e}")
                    request_attempt += 1
                    if request_attempt >= max_retries:
                        print("参数错误重试次数已达上限，停止处理。")
                        break
                    time.sleep(delay * 2)

                except Exception as e:
                    request_attempt += 1
                    print(f"  第 {current_page} 页请求失败，正在进行第 {request_attempt} 次重试。错误信息: {e}")
                    if request_attempt >= max_retries:
                        print("重试次数已达上限，停止处理。")
                        break
                    time.sleep(delay * 2)

            print(f"所有数据处理完成。预期数据量: {total_expected}，实际成功处理: {total_processed}")
            return total_processed

        except Exception as e:
            print(f"处理过程中发生错误: {e}")

            traceback.print_exc()
            return total_processed
        finally:
            data_operator.disconnect_db()




    def get_orders_by_time_range(self, db_config, start_time, end_time, date_type="update_time",
                                 platform_codes=[10024]):
        """
        获取指定时间范围内的订单数据并存入数据库

        Args:
            db_config: 数据库配置字典
            start_time: 开始时间（时间戳或可转换为时间戳的字符串）
            end_time: 结束时间（时间戳或可转换为时间戳的字符串）
            date_type: 时间类型，默认为"update_time"
            platform_codes: 平台代码列表，默认为[10024]
        Returns:
            bool: 处理成功返回True，否则False
        """
        try:
            # 转换时间格式（如果传入的是字符串）
            if isinstance(start_time, str):
                start_time = int(pd.to_datetime(start_time, format='%Y%m%d %H:%M:%S').timestamp())
            if isinstance(end_time, str):
                end_time = int(pd.to_datetime(end_time, format='%Y%m%d %H:%M:%S').timestamp())

            print(f"获取订单数据，时间范围: {start_time} 到 {end_time}")
            print(f"时间类型: {date_type}, 平台代码: {platform_codes}")

            # API路径和请求参数
            api_path = "/pb/mp/order/v2/list"
            base_biz_body = {
                "start_time": start_time,
                "end_time": end_time,
                "date_type": date_type,
                "platform_code": platform_codes,
            }

            # 使用分批处理方式
            total_processed = self.fetch_and_process_order_data_batch(
                api_path, base_biz_body, db_config, delay=1
            )

            if total_processed > 0:
                print(f"成功处理 {total_processed} 条订单数据")
                return True
            else:
                print("未处理任何数据，请检查网络连接或API参数。")
                return False

        except Exception as e:
            print(f"处理订单数据失败: {e}")
            return False

    def getstoreList(self, db_config,  platform_codes=[10024]):
            """
            获取指定平台的店铺数据并存入数据库
            Args:
                db_config: 数据库配置字典
                platform_codes: 平台代码列表，默认为[10024]
            Returns:
                bool: 处理成功返回True，否则False
            """
            try:
                # API路径和请求参数
                api_path = "/pb/mp/shop/v2/getSellerList"
                base_biz_body = {
                    "platform_code": platform_codes,
                    "is_sync":1,
                    "status":1
                }

                # 使用分批处理方式
                total_processed = self.fetch_and_process_store_data_batch(
                    api_path, base_biz_body, db_config, delay=1
                )

                if total_processed > 0:
                    print(f"成功处理 {total_processed} 条店铺数据")
                    return True
                else:
                    print("未处理任何数据，请检查网络连接或API参数。")
                    return False

            except Exception as e:
                print(f"处理订单数据失败: {e}")
                return False

    def getwarehouseList(self, db_config, type=3):
        """
        获取指定平台的仓库数据并存入数据库
        Args:
            db_config: 数据库配置字典
            type: 仓库类型
        Returns:
            bool: 处理成功返回True，否则False
        """
        warehouselist = []
        data_operator = DataOperator(db_config)
        result = []
        try:
            # API路径和请求参数
            api_path = "/erp/sc/data/local_inventory/warehouse"
            biz_body = {
                "type": type
            }
            try:
                result = self.api_post(api_path, biz_body)
                # 增强响应结构检查
                if not result:
                    print("API返回空响应")
                    return warehouselist
                # 检查API返回的code字段
                api_code = result.get("code")
                if api_code is None:
                    print("API响应缺少code字段")
                    # 尝试检查其他可能的成功标识
                    if result.get("status") == "success" or result.get("success"):
                        print("检测到其他成功标识，继续处理")
                    else:
                        return warehouselist
                elif str(api_code) not in ['0', '200', '1000']:  # 根据实际API调整成功码
                    error_msg = result.get("msg", "未知错误")
                    print(f"API返回错误: {error_msg} (代码: {api_code})")

                if "data" not in result or result["data"] is None:
                    print("API返回数据为空")
                    return warehouselist

            except Exception as e:
                # 参数验证错误
                print(f"api连接错误: {e}")
            data_operator.connect_db()
            print("数据库连接成功，开始处理数据...")
            current_datas = result["data"]
            print(f"仓库数组长度为：{len(current_datas)}")
            data_operator.insert_warehouse_table(current_datas)
        except Exception as e:
            print(f"处理仓库数据失败: {e}")
            return False
        finally:
            data_operator.disconnect_db()
            
    def getwarehouseids(self, db_config):
        """
        获取指定平台的仓库数据并存入数据库
        Args:
            db_config: 数据库配置字典
            type: 仓库类型
        Returns:
            bool: 处理成功返回True，否则False
        """
        warehouseids = []
        data_operator = DataOperator(db_config)
        data_operator.connect_db()
        try:
            warehouseids = data_operator.get_warehouse_ids()
        except Exception as e:
            print("获取仓库id数组失败")
        data_operator.disconnect_db()
        return  warehouseids

    def getinvetoryList(self, db_config, str=''):
            """
            获取指定仓库的库存数据并存入数据库
            Args:
                db_config: 数据库配置字典
                str: 仓库id字符串，默认为空
            Returns:
                bool: 处理成功返回True，否则False
            """
            try:
                # API路径和请求参数
                api_path = "/erp/sc/routing/data/local_inventory/inventoryDetails"
                base_biz_body = {
                    "wid": str,
                }

                # 使用分批处理方式
                total_processed = self.fetch_and_process_invetory_data_batch(
                    api_path, base_biz_body, db_config, delay=1
                )

                if total_processed > 0:
                    print(f"成功处理 {total_processed} 条库存数据")
                    return True
                else:
                    print("未处理任何数据，请检查网络连接或API参数。")
                    return False

            except Exception as e:
                print(f"处理订单数据失败: {e}")
                return False

    def get_sales_by_date_range(self, db_config, start_date, end_date, result_type="1", date_unit="4",
                                data_type="4", sids=None, max_retries=3, delay=1):
        """
        获取指定时间范围内的销量数据并存入数据库

        Args:
            db_config: 数据库配置字典
            start_date: 开始日期（格式：YYYY-MM-DD）
            end_date: 结束日期（格式：YYYY-MM-DD）
            result_type: 汇总类型 1销量 2订单量 3销售额
            date_unit: 统计时间指标 1年 2月 3周 4日
            data_type: 统计数据维度 1ASIN 2父体 3MSKU 4SKU 5SPU 6店铺
            sids: 店铺ID列表，多个使用英文逗号分隔
            max_retries: 最大重试次数
            delay: 请求延迟

        Returns:
            bool: 处理成功返回True，否则False
        """
        try:
            # 验证日期范围不超过90天
            from datetime import datetime
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            days_diff = (end_dt - start_dt).days

            if days_diff > 90:
                print(f"错误: 时间范围超过90天限制: {days_diff}天")
                return False

            print(f"获取销量数据，时间范围: {start_date} 到 {end_date}")
            print(f"统计参数 - 汇总类型: {result_type}, 时间单位: {date_unit}, 数据维度: {data_type}")

            if sids:
                print(f"指定店铺ID: {sids}")

            # API路径和请求参数
            api_path = "/basicOpen/platformStatisticsV2/saleStat/pageList"
            base_biz_body = {
                "start_date": start_date,
                "end_date": end_date,
                "result_type": result_type,
                "date_unit": date_unit,
                "data_type": data_type
            }

            # 添加可选参数
            if sids:
                base_biz_body["sids"] = sids

            # 使用分批处理方式
            total_processed = self.fetch_and_process_sales_data_batch(
                api_path, base_biz_body, db_config, max_retries, delay
            )

            if total_processed > 0:
                print(f"成功处理 {total_processed} 条销量数据")
                return True
            else:
                print("警告: 未处理任何销量数据")
                return False

        except Exception as e:
            print(f"处理销量数据失败: {e}")
            import traceback
            print(f"详细错误: {traceback.format_exc()}")
            return False

    def fetch_and_process_sales_data_batch(self, api_path, base_biz_body, db_config, max_retries=3, delay=1):
        """
        从分页API获取销量数据并实时分批处理

        Args:
            api_path: API路径
            base_biz_body: 基础请求体参数
            db_config: 数据库配置
            max_retries: 最大重试次数
            delay: 请求延迟

        Returns:
            int: 成功处理的总记录数
        """
        total_processed = 0
        current_page = 1
        page_size = 100  # 每页大小，可根据API限制调整
        request_attempt = 0

        # 初始化数据处理器
        data_operator = DataOperator(db_config)

        try:
            # 连接数据库
            data_operator.connect_db()
            print("数据库连接成功，开始分批处理销量数据...")

            # 1. 首先获取数据总量
            print("正在获取销量数据总量...")
            biz_body = base_biz_body.copy()
            biz_body.update({
                "page": 1,
                "length": 20  # 初始请求获取少量数据以获取总数
            })

            try:
                initial_result = self.api_post(api_path, biz_body)

                if not initial_result or initial_result.get("code") != 0:
                    print(f"获取数据总量失败: {initial_result}")
                    return total_processed

                total_expected = initial_result.get("total", 0)
                print(f"销量数据总量为: {total_expected}")

            except Exception as e:
                print(f"获取数据总量失败: {e}")
                return total_processed

            # 2. 计算总页数
            total_pages = (total_expected + page_size - 1) // page_size if total_expected > 0 else 0
            print(f"开始分页请求，共需处理 {total_pages} 页数据，每页 {page_size} 条...")

            # 3. 分批获取和处理数据
            while current_page <= total_pages and request_attempt < max_retries:
                print(f"正在处理第 {current_page}/{total_pages} 页")

                try:
                    # 构建当前页的请求参数
                    biz_body = base_biz_body.copy()
                    biz_body.update({
                        "page": current_page,
                        "length": page_size
                    })

                    # 获取当前批次数据
                    result = self.api_post(api_path, biz_body)

                    # 检查API响应
                    if not result or result.get("code") != 0:
                        error_msg = result.get("message", "未知错误") if result else "空响应"
                        print(f"警告: 第 {current_page} 页API请求失败: {error_msg}")
                        request_attempt += 1
                        time.sleep(delay * 2)
                        continue

                    data_list = result.get("data", [])
                    batch_size = len(data_list)

                    if batch_size == 0:
                        print("当前页未返回数据，退出循环")
                        break

                    # 处理当前批次数据
                    print(f"第 {current_page} 页获取成功，本页 {batch_size} 条数据，开始插入数据库...")

                    batch_success_count = self._process_sales_batch_data(data_operator, data_list)
                    total_processed += batch_success_count

                    if batch_success_count == batch_size:
                        print(f"第 {current_page} 页数据插入成功 ({batch_success_count}/{batch_size})")
                        request_attempt = 0  # 重置重试计数
                        current_page += 1  # 处理下一页
                    else:
                        print(f"警告: 第 {current_page} 页数据部分插入失败 ({batch_success_count}/{batch_size})")
                        request_attempt += 1

                    # 请求间隔，避免API限流
                    time.sleep(delay)

                except Exception as e:
                    request_attempt += 1
                    print(f"错误: 第 {current_page} 页处理失败，正在进行第 {request_attempt} 次重试。错误: {e}")

                    if request_attempt >= max_retries:
                        print("错误: 重试次数已达上限，停止处理")
                        break

                    time.sleep(delay * 2)

            print(f"销量数据处理完成。预期数据量: {total_expected}，实际成功处理: {total_processed}")
            return total_processed

        except Exception as e:
            print(f"处理销量数据过程中发生错误: {e}")
            import traceback
            print(f"详细错误: {traceback.format_exc()}")
            return total_processed

        finally:
            # 确保数据库连接被关闭
            data_operator.disconnect_db()

    def _process_sales_batch_data(self, data_operator, data_list):
        """
        处理单批销量数据

        Args:
            data_operator: 数据库操作对象
            data_list: 单批数据列表

        Returns:
            int: 成功处理的数据条数
        """
        success_count = 0

        for data in data_list:
            try:
                # 预处理数据，处理JSON字符串字段
                processed_data = self._preprocess_sales_data(data)

                # 插入数据库
                if data_operator.insert_sales_info(processed_data):
                    success_count += 1
                else:
                    print("警告: 单条销量数据插入失败")

            except Exception as e:
                print(f"错误: 处理单条销量数据失败: {e}")
                continue

        return success_count

    def _preprocess_sales_data(self, data):
        """
        预处理销量数据，处理JSON字符串等特殊字段
        """
        import json
        import hashlib

        processed_data = data.copy()

        # 处理JSON字符串字段
        json_fields_to_process = ['msku', 'date_collect']

        for field in json_fields_to_process:
            if field in processed_data and isinstance(processed_data[field], str):
                try:
                    processed_data[field] = json.loads(processed_data[field])
                except:
                    if field == 'msku':
                        processed_data[field] = [processed_data[field]] if processed_data[field] else []
                    elif field == 'date_collect':
                        processed_data[field] = {}

        # 确保所有数组字段都是列表类型
        array_fields = [
            'sku', 'spu', 'spu_name', 'msku', 'mskuId', 'sid',
            'skuAndProductName', 'product_name', 'develop_name',
            'platform_code', 'platform_name', 'attribute',
            'parentAsin', 'site_code', 'site_name', 'store_name',
            'platform_product_id', 'platform_product_title'
        ]

        for field in array_fields:
            if field in processed_data and not isinstance(processed_data[field], list):
                if processed_data[field] is None:
                    processed_data[field] = []
                else:
                    processed_data[field] = [processed_data[field]]

        # 确保数值字段类型正确
        if 'volumeTotal' in processed_data:
            try:
                if processed_data['volumeTotal'] is None or processed_data['volumeTotal'] == '':
                    processed_data['volumeTotal'] = 0
                else:
                    processed_data['volumeTotal'] = float(processed_data['volumeTotal'])
            except (ValueError, TypeError):
                processed_data['volumeTotal'] = 0

        # 计算sales_code（基于sku字段）
        sku_list = processed_data.get('sku', [])
        sku_json = json.dumps(sku_list, sort_keys=True, separators=(',', ':'))
        processed_data['sales_code'] = hashlib.md5(sku_json.encode('utf-8')).hexdigest()

        return processed_data

# ========= API调用 =========
if __name__ == "__main__":
    # 创建API客户端实例
    # 加载配置
    config = load_config_from_env()
    client = LingXingAPI(
        app_id=config['app_id'],
        app_secret=config['app_secret'])
    db_config = config['db_config']
    #设置时间
    start_time = "2025-11-22"
    end_time = "2025-12-23"
    # 使用分批处理方式
    # res1 = client.get_orders_by_time_range(db_config=db_config,start_time=start_time,end_time=end_time)
    # res2 = client.getstoreList(db_config=db_config)
    #非分页数据直接处理
    # res3 = client.getwarehouseList(db_config=db_config)
    # warehouseids = client.getwarehouseids(db_config=db_config)
    # for i in warehouseids:
    #     wid_str = ",".join([str(i) for i in warehouseids])
    # print(wid_str)
    # res4 = client.getinvetoryList(db_config=db_config, str=wid_str)
    res5 = client.get_sales_by_date_range(start_date=start_time,end_date=end_time,db_config = db_config)
    print(res5)



