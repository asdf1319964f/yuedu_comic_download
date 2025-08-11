# --- START OF FILE app.py ---

import os
import time
import uuid
import threading
import subprocess
from flask import Flask, request, jsonify, send_from_directory, render_template
from concurrent.futures import ThreadPoolExecutor
import json
import shutil
import redis

# 确保 downloader 在 app 之前导入，以避免循环依赖
from downloader import process_task_file_with_progress, DownloadController

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "output")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(TEMPLATE_DIR, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER

# --- Redis 配置 ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_KEY_PREFIX = "comic_downloader:"

# --- Redis 客户端现在是全局可访问的 ---
try:
    redis_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    redis_client = redis.Redis(connection_pool=redis_pool)
    redis_client.ping() # 测试连接
    print("Redis 连接成功。")
except Exception as e:
    print(f"警告: Redis 连接失败: {e}。URL去重功能将不可用。")
    redis_client = None

executor = ThreadPoolExecutor(max_workers=4)
download_controllers = {}

def get_task_key(task_id):
    return f"{REDIS_KEY_PREFIX}task:{task_id}"

def get_all_tasks_key():
    return f"{REDIS_KEY_PREFIX}tasks"

def save_task_status(task_id, status_data):
    if not redis_client: return # 如果Redis不可用，则不保存
    try:
        task_key = get_task_key(task_id)
        redis_client.set(task_key, json.dumps(status_data, ensure_ascii=False))
        redis_client.sadd(get_all_tasks_key(), task_id)
    except Exception as e:
        print(f"保存任务状态到 Redis 时出错 {task_id}: {e}")

def load_task_status(task_id):
    if not redis_client: return None # 如果Redis不可用，则无法加载
    try:
        task_key = get_task_key(task_id)
        json_data = redis_client.get(task_key)
        if json_data:
            return json.loads(json_data)
    except Exception as e:
        print(f"从 Redis 加载任务状态时出错 {task_id}: {e}")
    return None

def load_all_task_ids():
    if not redis_client: return set()
    return redis_client.smembers(get_all_tasks_key()) or set()

def check_interrupted_tasks():
    if not redis_client:
        print("Redis 不可用，跳过中断任务检查。")
        return
    print("正在检查中断的任务...")
    task_ids = load_all_task_ids()
    interrupted_count = 0
    for task_id in task_ids:
        status_data = load_task_status(task_id)
        if status_data and status_data.get('status') == '执行中':
            status_data['status'] = '中断'
            status_data['logs'].append(f"警告: 服务器重启，任务在 {time.strftime('%Y-%m-%d %H:%M:%S')} 被中断。")
            save_task_status(task_id, status_data)
            interrupted_count += 1
    if interrupted_count > 0:
        print(f"发现并标记了 {interrupted_count} 个中断的任务。")
    print(f"从 Redis 加载了 {len(task_ids)} 个现有任务。")

def parse_proxy_list(proxy_list_text):
    proxies = []
    for line in proxy_list_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"): continue
        proxies.append(line)
    return proxies

def auto_cleanup(output_folder, max_age_days=7):
    now = time.time()
    for filename in os.listdir(output_folder):
        file_path = os.path.join(output_folder, filename)
        mtime = os.path.getmtime(file_path)
        if now - mtime > max_age_days * 86400:
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"自动清理: 删除过期文件 {file_path}")
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    print(f"自动清理: 删除过期文件夹 {file_path}")
            except Exception as e:
                print(f"自动清理失败: {file_path} {e}")

@app.route('/')
def index():
    return render_template("index.html")

@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files.get('taskfile')
    if not file: return jsonify({'error': '未选择任务文件'}), 400
    if not (file.filename.lower().endswith('.json') or file.filename.lower().endswith('.txt')): return jsonify({'error': '请上传 .json 或 .txt 任务文件'}), 400
    save_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(save_path)

    cookie = request.form.get('cookie', '')
    custom_referer = request.form.get('custom_referer', '').strip()
    proxy_list_text = request.form.get('proxy_list', '').strip()
    pack_after_download = request.form.get('pack_after_download', 'true') == 'true'
    delete_after_pack = request.form.get('delete_after_pack', 'false') == 'true'
    thread_count = int(request.form.get('thread_count', 4))
    aes_key = request.form.get('aes_key', None)
    aes_iv = request.form.get('aes_iv', None)

    headers = {"User-Agent": "Mozilla/5.0"}
    if cookie: headers["Cookie"] = cookie
    proxy_list = parse_proxy_list(proxy_list_text) if proxy_list_text else []

    task_id = str(uuid.uuid4())
    initial_status = {
        'task_type': 'download', 'status': '等待执行', 'progress_percent': 0,
        'progress_current': 0, 'progress_total': 0, 'logs': [], 'result': None,
        'config': {
            'filename': file.filename, 'pack_after_download': pack_after_download,
            'delete_after_pack': delete_after_pack, 'thread_count': thread_count
        }
    }
    save_task_status(task_id, initial_status)

    download_controller = DownloadController()
    download_controllers[task_id] = download_controller

    def download_progress_callback(current, total, msg):
        task_data = load_task_status(task_id)
        if not task_data: return
        task_data['progress_current'] = current
        task_data['progress_total'] = total
        task_data['progress_percent'] = (current / total * 100) if total > 0 else 0
        task_data['logs'].append(msg)
        if len(task_data['logs']) > 100:
            task_data['logs'] = task_data['logs'][-50:]
        save_task_status(task_id, task_data)

    def download_task_wrapper():
        task_data = load_task_status(task_id)
        if not task_data: return
        task_data['status'] = '执行中'
        save_task_status(task_id, task_data) 
        try:
            result = process_task_file_with_progress(
                save_path, app.config['OUTPUT_FOLDER'], headers, 
                download_progress_callback, proxy_list, pack_after_download, 
                delete_after_pack, aes_key, aes_iv, max_workers=thread_count,
                download_controller=download_controller, custom_referer=custom_referer,
                redis_client=redis_client
            )
            task_data = load_task_status(task_id)
            if task_data:
                task_data['status'] = '完成'
                task_data['result'] = result
                task_data['progress_percent'] = 100
                if task_data['progress_total'] > 0:
                    task_data['progress_current'] = task_data['progress_total']
                save_task_status(task_id, task_data)
        except Exception as e:
            task_data = load_task_status(task_id)
            if task_data:
                current_status = task_data.get('status', '')
                if current_status not in ['暂停中', '中断']:
                    task_data['status'] = '失败'
                task_data['logs'].append(f"❌ 任务执行失败: {e}")
                task_data['result'] = {'error': str(e)}
                save_task_status(task_id, task_data)
        finally:
            if task_id in download_controllers:
                del download_controllers[task_id]

    executor.submit(download_task_wrapper)
    return jsonify({'status': 'ok', 'task_id': task_id})

@app.route('/pause/<task_id>', methods=['POST'])
def pause_task(task_id):
    ctrl = download_controllers.get(task_id)
    if ctrl:
        ctrl.pause()
        task_data = load_task_status(task_id)
        if task_data:
            task_data['status'] = '暂停中'
            task_data['logs'].append("⏸️ 用户暂停了任务")
            save_task_status(task_id, task_data)
        return jsonify({'status': 'paused'})
    return jsonify({'error': '任务不存在或已结束'}), 404

@app.route('/resume/<task_id>', methods=['POST'])
def resume_task(task_id):
    ctrl = download_controllers.get(task_id)
    if ctrl:
        ctrl.resume()
        task_data = load_task_status(task_id)
        if task_data:
            task_data['status'] = '执行中'
            task_data['logs'].append("▶️ 用户恢复了任务")
            save_task_status(task_id, task_data)
        return jsonify({'status': 'resumed'})
    return jsonify({'error': '任务不存在或已结束'}), 404

@app.route('/status/<task_id>')
def get_status(task_id):
    status = load_task_status(task_id)
    if not status: return jsonify({'error': '任务不存在'}), 404
    if 'logs' in status and len(status['logs']) > 10:
        status['logs'] = status['logs'][-10:]
    return jsonify(status)

@app.route('/download/<path:filename>')
def download_file(filename):
    return send_from_directory(app.config['OUTPUT_FOLDER'], filename, as_attachment=True)

@app.route('/tasks')
def list_tasks():
    task_ids = load_all_task_ids()
    all_tasks = []
    for task_id in task_ids:
        t = load_task_status(task_id)
        if t:
            all_tasks.append({
                'task_id': task_id, 'status': t.get('status', '未知'),
                'filename': t.get('config', {}).get('filename', ''),
                'progress_percent': t.get('progress_percent', 0),
                'result': t.get('result', None)
            })
    all_tasks.sort(key=lambda x: x.get('filename', ''), reverse=True)
    return jsonify(all_tasks)

@app.route('/rclone_upload', methods=['POST'])
def rclone_upload():
    filename = request.form.get('filename')
    remote_path = request.form.get('remote_path')
    if not filename or not remote_path: return jsonify({'error': '参数缺失'}), 400
    local_path = os.path.join(app.config['OUTPUT_FOLDER'], filename)
    if not os.path.exists(local_path): return jsonify({'error': '文件不存在'}), 404

    task_id = str(uuid.uuid4())
    initial_status = {
        'task_type': 'rclone_upload', 'status': '等待执行', 'logs': [], 'result': None,
        'config': {'filename': filename, 'remote_path': remote_path}
    }
    save_task_status(task_id, initial_status)

    def rclone_upload_task():
        task_data = load_task_status(task_id)
        task_data['status'] = '执行中'
        save_task_status(task_id, task_data)
        try:
            cmd = ["rclone", "copy", local_path, remote_path, "--progress", "--stats=1s"]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for line in process.stdout:
                task_data = load_task_status(task_id)
                if not task_data: break
                task_data['logs'].append(line.strip())
                save_task_status(task_id, task_data)
            process.wait()
            task_data = load_task_status(task_id)
            if task_data:
                if process.returncode == 0:
                    task_data['status'] = '完成'
                    task_data['result'] = {'msg': '上传成功'}
                else:
                    task_data['status'] = '失败'
                    task_data['result'] = {'msg': '上传失败'}
                save_task_status(task_id, task_data)
        except Exception as e:
            task_data = load_task_status(task_id)
            if task_data:
                task_data['status'] = '失败'
                task_data['logs'].append(f"❌ 上传失败: {e}")
                task_data['result'] = {'error': str(e)}
                save_task_status(task_id, task_data)
    executor.submit(rclone_upload_task)
    return jsonify({'status': 'ok', 'task_id': task_id})

@app.route('/rclone_status/<task_id>')
def rclone_status(task_id):
    return get_status(task_id)

if __name__ == '__main__':
    check_interrupted_tasks()
    auto_cleanup(app.config['OUTPUT_FOLDER'])
    # For production, use a WSGI server like Gunicorn instead of app.run()
    # Example: gunicorn --workers 4 --bind 0.0.0.0:5000 app:app
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

# --- END OF FILE app.py ---
