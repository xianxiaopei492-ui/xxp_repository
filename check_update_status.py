import subprocess
import sys
import os
from datetime import datetime, timedelta



def check_recent_logs(log_file_path, keyword_success='任务执行成功', keyword_error='任务执行失败', hours_to_check=24):
    """
    检查最近指定小时内的日志
    """
    try:
        # 检查日志文件是否存在
        if not os.path.exists(log_file_path):
            print(f"❌ 日志文件不存在: {log_file_path}")
            return False

        # 检查文件大小
        file_size = os.path.getsize(log_file_path)
        if file_size == 0:
            print(f"⚠️  日志文件为空: {log_file_path}")
            return False

        # 使用grep检查最近N小时内是否有关键词
        # 查找成功关键词
        result_success = subprocess.run([
            'grep', '-E', keyword_success, log_file_path
        ], capture_output=True, text=True)

        # 查找错误关键词
        result_error = subprocess.run([
            'grep', '-E', keyword_error, log_file_path
        ], capture_output=True, text=True)

        # 判断逻辑
        if result_success.returncode == 0:  # 找到了成功关键词
            print(f"✅ {log_file_path} 脚本近期运行成功")
            # 显示最近的成功记录
            success_lines = result_success.stdout.strip().split('\n')
            if success_lines:
                latest_success = success_lines[-1]  # 取最后一条记录
                print(f"   最近成功记录: {latest_success[:100]}...")  # 显示前100个字符
            return True
        elif result_error.returncode == 0:  # 找到了错误关键词
            print(f"❌ {log_file_path} 脚本近期运行失败")
            # 显示最近的错误记录
            error_lines = result_error.stdout.strip().split('\n')
            if error_lines:
                latest_error = error_lines[-1]  # 取最后一条记录
                print(f"   最近错误记录: {latest_error[:100]}...")  # 显示前100个字符
            return False
        else:
            print(f"⚠️  {log_file_path} 未找到脚本近期运行记录")
            return False

    except Exception as e:
        print(f"检查日志失败 {log_file_path}: {e}")
        return False


def check_log_timestamp(log_file_path, hours_threshold=24):
    """
    检查日志文件最后修改时间是否在指定小时内
    """
    try:
        if not os.path.exists(log_file_path):
            print(f"❌ 日志文件不存在: {log_file_path}")
            return False

        # 获取文件最后修改时间
        mtime = os.path.getmtime(log_file_path)
        last_modified = datetime.fromtimestamp(mtime)
        now = datetime.now()

        # 计算时间差
        time_diff = now - last_modified
        hours_diff = time_diff.total_seconds() / 3600

        if hours_diff <= hours_threshold:
            print(f"✅ {log_file_path} 最后修改于 {last_modified.strftime('%Y-%m-%d %H:%M:%S')} ({hours_diff:.1f} 小时前)")
            return True
        else:
            print(f"❌ {log_file_path} 最后修改时间过久: {last_modified.strftime('%Y-%m-%d %H:%M:%S')} ({hours_diff:.1f} 小时前)")
            return False

    except Exception as e:
        print(f"检查日志时间戳失败 {log_file_path}: {e}")
        return False


def check_cancel_order_log():
    """
    专门检查取消订单同步日志
    """
    log_path = '/var/log/daily_cancel_order_to_feishu.log'

    print("=" * 60)
    print("📋 检查取消订单同步日志")
    print("=" * 60)

    # 检查文件存在性和时间戳
    timestamp_ok = check_log_timestamp(log_path, hours_threshold=24)

    # 检查日志内容
    content_ok = check_recent_logs(
        log_path,
        keyword_success='取消订单数据同步完成',  # 根据实际日志调整关键词
        keyword_error='取消订单数据同步失败',
        hours_to_check=24
    )

    return timestamp_ok and content_ok


def check_daily_order_log():
    """
    检查每日订单更新日志
    """
    log_path = '/var/log/daily_order_update.log'

    print("=" * 60)
    print("📋 检查每日订单更新日志")
    print("=" * 60)

    # 检查文件存在性和时间戳
    timestamp_ok = check_log_timestamp(log_path, hours_threshold=24)

    # 检查日志内容
    content_ok = check_recent_logs(
        log_path,
        keyword_success='任务执行成功',  # 根据实际日志调整关键词
        keyword_error='任务执行失败',
        hours_to_check=24
    )

    return timestamp_ok and content_ok


def check_daily_sales_summary_to_feishu_log():
    """
    检查每日订单更新日志
    """
    log_path = '/var/log/daily_sales_summary_to_feishu.log'

    print("=" * 60)
    print("📋 检查每日订单更新日志")
    print("=" * 60)

    # 检查文件存在性和时间戳
    timestamp_ok = check_log_timestamp(log_path, hours_threshold=24)

    # 检查日志内容
    content_ok = check_recent_logs(
        log_path,
        keyword_success='任务执行成功',  # 根据实际日志调整关键词
        keyword_error='任务执行失败',
        hours_to_check=24
    )

    return timestamp_ok and content_ok

def check_daily_inventory_to_feishu_log():
    """
    检查每日订单更新日志
    """
    log_path = '/var/log/inventory_to_feishu.log'

    print("=" * 60)
    print("📋 检查每日订单更新日志")
    print("=" * 60)

    # 检查文件存在性和时间戳
    timestamp_ok = check_log_timestamp(log_path, hours_threshold=24)

    # 检查日志内容
    content_ok = check_recent_logs(
        log_path,
        keyword_success='任务执行成功',  # 根据实际日志调整关键词
        keyword_error='任务执行失败',
        hours_to_check=24
    )

    return timestamp_ok and content_ok



def main():
    """
    主检查函数
    """
    print("🚀 开始检查定时任务执行状态")
    print("=" * 60)

    # 检查两个日志文件
    cancel_order_ok = check_cancel_order_log()
    daily_order_ok = check_daily_order_log()
    inventory_ok = check_daily_inventory_to_feishu_log()
    sales_summary_ok = check_daily_sales_summary_to_feishu_log()
    print("=" * 60)
    print("📊 检查结果汇总:")
    print(f"   取消订单同步任务: {'✅ 正常' if cancel_order_ok else '❌ 异常'}")
    print(f"   每日订单更新任务: {'✅ 正常' if daily_order_ok else '❌ 异常'}")
    print(f"   每日销量统计同步任务: {'✅ 正常' if sales_summary_ok else '❌ 异常'}")
    print(f"   每日库存更新任务: {'✅ 正常' if inventory_ok else '❌ 异常'}")

    # 总体状态判断
    if cancel_order_ok and daily_order_ok and inventory_ok and sales_summary_ok :
        print("🎉 所有定时任务运行正常!")
        return 0
    else:
        print("⚠️  部分定时任务存在异常，请检查!")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)