import base64
import os, sys
import time
import psutil
import logging
import ftplib

sys.path.append("/data/faird")
from sdk.dacp_client import DacpClient


# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ===== 基础配置 =====
SERVER_URL = "dacp://10.0.89.38:3101"
USERNAME = "user1@cnic.cn"
PASSWORD = "user1@cnic.cn"
TENANT = "conet"

FTP_HOST = "10.0.89.38"
FTP_USER = "ftpuser"
FTP_PASS = "ftpuser"

RUN_TIMES = 3  # 每项测试运行次数

process = psutil.Process(os.getpid())

# ===== 测试数据配置 =====
TEST_DATASETS = [
    {
        "name": "test_zip_01.zip", # 测试数据文件名称
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_zip_01.zip", # DACP URL
        "ftp_path": "remote/path/test_zip_01.zip", # FTP 服务器上的路径
        "description": "zip文件，大概160MB" # 数据文件描述
    },
    {
        "name": "test_zip_02.zip",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_zip_02.zip",
        "ftp_path": "remote/path/test_zip_02.zip",
        "description": "zip文件，大概160MB"
    },
    {
        "name": "test_zip_03.zip",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_zip_03.zip",
        "ftp_path": "remote/path/test_zip_03.zip",
        "description": "zip文件，大概160MB"
    },
    {
        "name": "test_zip_04.zip",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_zip_04.zip",
        "ftp_path": "remote/path/test_zip_04.zip",
        "description": "zip文件，大概160MB"
    },
    {
        "name": "test_zip_05.zip",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_zip_05.zip",
        "ftp_path": "remote/path/test_zip_05.zip",
        "description": "zip文件，大概160MB"
    },
    {
        "name": "test_image_01.jpg",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_image_01.jpg",
        "ftp_path": "remote/path/test_image_01.jpg",
        "description": "jpg文件，大概120MB"
    },
    {
        "name": "test_image_02.jpg",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_image_02.jpg",
        "ftp_path": "remote/path/test_image_02.jpg",
        "description": "jpg文件，大概120MB"
    },
    {
        "name": "test_image_03.jpg",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_image_03.jpg",
        "ftp_path": "remote/path/test_image_03.jpg",
        "description": "jpg文件，大概120MB"
    },
    {
        "name": "test_image_04.jpg",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_image_04.jpg",
        "ftp_path": "remote/path/test_image_04.jpg",
        "description": "jpg文件，大概120MB"
    },
    {
        "name": "test_image_05.jpg",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_image_05.jpg",
        "ftp_path": "remote/path/test_image_05.jpg",
        "description": "jpg文件，大概120MB"
    },
    {
        "name": "test_video_01.mov",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_video_01.mov",
        "ftp_path": "remote/path/test_video_01.mov",
        "description": "video文件，大概100MB"
    },
    {
        "name": "test_video_02.mov",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_video_02.mov",
        "ftp_path": "remote/path/test_video_02.mov",
        "description": "video文件，大概100MB"
    },
    {
        "name": "test_video_03.mov",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_video_03.mov",
        "ftp_path": "remote/path/test_video_03.mov",
        "description": "video文件，大概100MB"
    },
    {
        "name": "test_video_04.mov",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_video_04.mov",
        "ftp_path": "remote/path/test_video_04.mov",
        "description": "video文件，大概100MB"
    },
    {
        "name": "test_video_05.mov",
        "dacp_url": "dacp://10.0.89.38:3101/test/test_dataset/test_video_05.mov",
        "ftp_path": "remote/path/test_video_05.mov",
        "description": "video文件，大概100MB"
    }
]


# ===== 工具函数 =====
def get_mem_usage():
    return process.memory_info().rss / (1024 ** 2)


def get_file_size_mb(filepath):
    """获取文件大小(MB)"""
    if os.path.exists(filepath):
        return os.path.getsize(filepath) / (1024 ** 2)
    return 0


def faird_test(dataset):
    """faird测试"""
    logger.info(f"开始 Faird 测试: {dataset['description']}")
    mem_before = get_mem_usage()

    try:
        start_connect = time.time()
        conn = DacpClient.connect(SERVER_URL)
        connect_time = time.time() - start_connect

        start_open = time.time()

        filename = dataset['dacp_url']
        base64_str = conn.get_base64(filename)

        encode_start = time.time()
        decoded_data = base64.b64decode(base64_str)
        encode_time = time.time() - encode_start
        logger.info(f"filename: {filename}, Base64转换耗时: {encode_time:.3f}s")

        read_time = time.time() - start_open
        total_time = time.time() - start_connect

        mem_after = get_mem_usage()
        file_size_mb = get_file_size_mb(f"/tmp/{dataset['name']}")

        result = {
            "dataset_name": dataset['name'],
            "dataset_desc": dataset['description'],
            "type": "faird",
            "connect_time": connect_time,
            "read_time": read_time,
            "total_time": total_time,
            "file_size_mb": file_size_mb,
            "throughput_mbps": file_size_mb / total_time if total_time > 0 else 0,
            "memory_change_mb": mem_after - mem_before,
            "success": True
        }

        #conn.close()

    except Exception as e:
        logger.error(f"Faird测试失败: {e}")
        result = {
            "dataset_name": dataset['name'],
            "dataset_desc": dataset['description'],
            "type": "faird",
            "error": str(e),
            "success": False
        }

    logger.info(f"Faird 测试完成: {dataset['name']}")
    return result


def ftp_test(dataset):
    """FTP测试"""
    logger.info(f"开始 FTP 测试: {dataset['description']}")
    mem_before = get_mem_usage()
    local_temp_file = f"/tmp/{dataset['name']}"

    try:
        # 清理重复测试的临时文件
        if os.path.exists(local_temp_file):
            os.remove(local_temp_file)

        start_connect = time.time()
        ftp = ftplib.FTP(FTP_HOST)
        ftp.login(user=FTP_USER, passwd=FTP_PASS)
        connect_time = time.time() - start_connect

        start_download = time.time()
        logger.info(f"ftp正在测试下载 {dataset['name']}")
        with open(local_temp_file, 'wb') as f:
            ftp.retrbinary(f"RETR {dataset['ftp_path']}", f.write)
        download_time = time.time() - start_download

        with open(local_temp_file, 'rb') as f:
            file_data = f.read()  # 读取整个文件到内存

        total_time = time.time() - start_connect
        mem_after = get_mem_usage()
        file_size_mb = get_file_size_mb(local_temp_file)

        result = {
            "dataset_name": dataset['name'],
            "dataset_desc": dataset['description'],
            "type": "ftp",
            "connect_time": connect_time,
            "download_time": download_time,
            "total_time": total_time,
            "file_size_mb": file_size_mb,
            "throughput_mbps": file_size_mb / total_time if total_time > 0 else 0,
            "memory_change_mb": mem_after - mem_before,
            "success": True
        }

        ftp.quit()

    except Exception as e:
        logger.error(f"FTP测试失败: {e}")
        result = {
            "dataset_name": dataset['name'],
            "dataset_desc": dataset['description'],
            "type": "ftp",
            "error": str(e),
            "success": False
        }

    logger.info(f"FTP 测试完成: {dataset['name']}")
    return result


# ===== 批量测试函数 =====
def run_single_protocol_tests(test_func, protocol_name, datasets, repeat_times=RUN_TIMES):
    """运行单个协议的所有数据集测试"""
    all_results = []

    for dataset in datasets:
        logger.info(f"\n--- {protocol_name} 测试数据集: {dataset['description']} ---")
        dataset_results = []

        for i in range(repeat_times):
            logger.info(f"{protocol_name} {dataset['name']} 第 {i + 1}/{repeat_times} 次测试")
            result = test_func(dataset)
            if result.get('success', False):
                dataset_results.append(result)

        all_results.extend(dataset_results)

        # 输出当前数据集的统计
        if dataset_results:
            avg_connect = sum(r["connect_time"] for r in dataset_results) / len(dataset_results)
            avg_total = sum(r["total_time"] for r in dataset_results) / len(dataset_results)
            avg_throughput = sum(r["throughput_mbps"] for r in dataset_results) / len(dataset_results)
            memory_change_mb = sum(r["memory_change_mb"] for r in dataset_results) / len(dataset_results)
            logger.info(
                f"{dataset['name']} 平均连接时间: {avg_connect:.3f}s, 总时间: {avg_total:.3f}s, 吞吐量: {avg_throughput:.2f}MB/s, 占用内存变化: {memory_change_mb:.2f}MB")

    return all_results


def analyze_results(results, protocol_name):
    """分析测试结果 - 汇总所有文件的统计信息"""
    if not results:
        logger.warning(f"{protocol_name} 没有有效结果")
        return {}

    # 汇总所有文件的统计
    total_connect_time = sum(r["connect_time"] for r in results)
    total_download_time = sum(r.get("download_time", r.get("read_time", 0)) for r in results)
    total_time = sum(r["total_time"] for r in results)
    total_file_size = sum(r["file_size_mb"] for r in results)
    total_memory_change = sum(r["memory_change_mb"] for r in results)

    # 计算平均值
    count = len(results)
    avg_connect_time = total_connect_time / count
    avg_download_time = total_download_time / count
    avg_total_time = total_time / count
    avg_file_size = total_file_size / count
    avg_memory_change = total_memory_change / count

    # 计算总体带宽（所有文件总大小 / 所有文件总传输时间）
    overall_throughput = total_file_size / total_time if total_time > 0 else 0

    analysis = {
        "protocol": protocol_name,
        "total_tests": count,
        "total_file_size_mb": total_file_size,
        "total_connect_time": total_connect_time,
        "total_download_time": total_download_time,
        "total_time": total_time,
        "avg_connect_time": avg_connect_time,
        "avg_download_time": avg_download_time,
        "avg_total_time": avg_total_time,
        "avg_file_size_mb": avg_file_size,
        "avg_memory_change_mb": avg_memory_change,
        "overall_throughput_mbps": overall_throughput,
        "min_total_time": min(r["total_time"] for r in results),
        "max_total_time": max(r["total_time"] for r in results),
        "files_tested": list(set(r["dataset_name"] for r in results))
    }

    return analysis


# ===== 主程序 =====
if __name__ == "__main__":
    logger.info("====== FAIRD vs FTP 性能对比测试 ======")

    # 1. 生成测试数据
    # generate_test_csv_files()

    # 2. 开始测试
    ftp_results = run_single_protocol_tests(ftp_test, "FTP", TEST_DATASETS, RUN_TIMES)
    faird_results = run_single_protocol_tests(faird_test, "FAIRD", TEST_DATASETS, RUN_TIMES)


    # 3. 结果分析
    ftp_analysis = analyze_results(ftp_results, "FTP")
    faird_analysis = analyze_results(faird_results, "FAIRD")

    print(f"ftp_analysis: {ftp_analysis}")
    print(f"faird_analysis: {faird_analysis}")


    sys.exit(0)

