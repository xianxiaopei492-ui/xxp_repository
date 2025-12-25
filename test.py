import time
import json
import hashlib
import base64
import requests
from Crypto.Cipher import AES

# ==========================
# 零星的接口密钥
# ==========================
APP_ID = "ak_89uM2PNqPSPFJ"              # 文档里的 app_key & AES 密钥
APP_SECRET = "COQo5uhVIR8eAPTN3Vy/ig=="  # AppSecret

BASE_URL = "https://openapi.lingxing.com"


# ========= AES 工具 =========
def pkcs5_pad(s: str) -> str:
    pad_len = 16 - len(s) % 16
    return s + chr(pad_len) * pad_len


def aes_encrypt(text: str, key: str) -> str:
    cipher = AES.new(key.encode("utf-8"), AES.MODE_ECB)
    padded = pkcs5_pad(text).encode("utf-8")
    encrypted = cipher.encrypt(padded)
    # 不做 urlencode，只做 base64；urlencode 交给 requests 处理
    return base64.b64encode(encrypted).decode("utf-8")


# ========= 生成 sign =========
def generate_sign(params: dict) -> str:
    # value 为空字符串的参数不参与签名（None 要参与）
    filtered = {k: v for k, v in params.items() if v != ""}

    # ASCII 排序
    items = sorted(filtered.items(), key=lambda x: x[0])

    # 拼接 key=value&...
    s = "&".join(f"{k}={v}" for k, v in items)

    # MD5 → 大写
    md5_str = hashlib.md5(s.encode("utf-8")).hexdigest().upper()

    # AES/ECB/PKCS5PADDING，加密 key = APP_ID
    return aes_encrypt(md5_str, APP_ID)


# ========= 获取 access_token（无 sign） =========
def get_access_token() -> str:
    url = f"{BASE_URL}/api/auth-server/oauth/access-token"
    data = {
        "appId": APP_ID,
        "appSecret": APP_SECRET
    }
    resp = requests.post(url, data=data)
    j = resp.json()
    if j.get("data") is None:
        # 打印错误方便排查
        raise RuntimeError(f"获取 token 失败: {j}")
    return j["data"]["access_token"]


# ========= 业务 POST 请求 =========
def api_post(api_path: str, biz_body: dict) -> dict:
    access_token = get_access_token()
    timestamp = str(int(time.time()))

    # 把 body 里的字段展开进签名参数：
    # - 普通值直接用
    # - dict / list 转成 JSON 字符串
    biz_for_sign = {}
    for k, v in biz_body.items():
        if isinstance(v, (dict, list)):
            biz_for_sign[k] = json.dumps(v, separators=(",", ":"), ensure_ascii=False)
        else:
            biz_for_sign[k] = v

    # 参与签名的所有参数 = 业务参数 + 3 个公共参数
    sign_params = {
        **biz_for_sign,
        "access_token": access_token,
        "app_key": APP_ID,
        "timestamp": timestamp,
    }

    sign = generate_sign(sign_params)

    # 实际请求的 query 公共参数
    query = {
        "access_token": access_token,
        "app_key": APP_ID,
        "timestamp": timestamp,
        "sign": sign,   # 不要自己 urlencode，requests 会自动处理
    }

    headers = {"Content-Type": "application/json"}

    url = BASE_URL + api_path
    resp = requests.post(url, params=query, json=biz_body, headers=headers)
    return resp.json()


# ========= API调用 =========
if __name__ == "__main__":
    wid_list = [15963, 16488, 16489, 16490, 16491, 16492, 16493, 16629, 16630, 16631, 16632, 17735, 18479, 18499, 18742, 18888, 19204]
    for i in wid_list:
        wid_str  = ",".join([str(i) for i in wid_list])
    print(wid_str)
    #查询销量统计列表v2
    api_path = "/erp/sc/routing/data/local_inventory/inventoryDetails"
    biz_body = {
      "wid":wid_str
    }

    result = api_post(api_path, biz_body)
    print(json.dumps(result["data"][0], indent=4, ensure_ascii=False))
