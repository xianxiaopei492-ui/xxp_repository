import subprocess
import sys



def check_recent_logs(log_file_path, keyword_success='任务执行成功', keyword_error='任务执行失败', hours_to_check=24):
    """
    检查最近指定小时内的日志
    """
    try:
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
            print("✅ 脚本近期运行成功")
            # 这里可以进一步解析具体时间和次数
            return True
        elif result_error.returncode == 0:  # 找到了错误关键词
            print("❌ 脚本近期运行失败")
            # 可以发送通知邮件或其它告警
            return False
        else:
            print("⚠️  未找到脚本近期运行记录")
            return False

    except Exception as e:
        print(f"检查日志失败: {e}")
        return False


if __name__ == "__main__":
    log_path1 = '/tmp/xxp/sqljob/sql_job.log'  # 检查sql增量更新是否执行
    log_path2 = '/var/log/daily_order_update.log'
    # if check_recent_logs(log_path1):
    #     print("sql文件正常执行!")
    #     sys.exit(0)  # 检查正常，退出码0
    # else:
    #     print("sql文件")
    #     sys.exit(1)  # 检查异常，退出码1
    if check_recent_logs(log_path2):
        print("每日订单数据更新py文件正常执行!")
        sys.exit(0)  # 检查正常，退出码0
    else:
        print("py文件")
        sys.exit(1)  # 检查异常，退出码1