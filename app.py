import os
import time
import uuid
import threading
import random
import subprocess
from flask import Flask, request, jsonify, send_from_directory, render_template
from concurrent.futures import ThreadPoolExecutor
import json
import base64
import shutil

from downloader import process_task_file_with_progress, DownloadController

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(BASE_DIR, 'templates')
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
OUTPUT_FOLDER = os.path.join(BASE_DIR, "output")
TASK_DATA_FOLDER = os.path.join(BASE_DIR, "task_data")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(TEMPLATE_DIR, exist_ok=True)
os.makedirs(TASK_DATA_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER
app.config['TASK_DATA_FOLDER'] = TASK_DATA_FOLDER

executor = ThreadPoolExecutor(max_workers=4)
task_statuses = {}
task_status_lock = threading.Lock()
download_controllers = {}  # 每个任务一个控制器

def get_task_status_file(task_id):
    return os.path.join(app.config['TASK_DATA_FOLDER'], f"{task_id}.json")

def save_task_status(task_id, status_data):
    file_path = get_task_status_file(task_id)
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(status_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error saving task status for {task_id}: {e}")

def load_task_status(task_id):
    file_path = get_task_status_file(task_id)
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading task status for {task_id}: {e}")
    return None

def load_all_task_statuses():
    global task_statuses
    for filename in os.listdir(app.config['TASK_DATA_FOLDER']):
        if filename.endswith(".json"):
            task_id = filename[:-5]
            status_data = load_task_status(task_id)
            if status_data:
                with task_status_lock:
                    if status_data.get('status') == '执行中':
                        status_data['status'] = '中断'
                        status_data['logs'].append(f"警告: 服务器重启，任务在 {time.strftime('%Y-%m-%d %H:%M:%S')} 被中断。")
                        save_task_status(task_id, status_data)
                    task_statuses[task_id] = status_data
    print(f"Loaded {len(task_statuses)} existing tasks.")

def parse_proxy_list(proxy_list_text):
    proxies = []
    for line in proxy_list_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        proxies.append(line)
    return proxies

def auto_cleanup(output_folder, task_data_folder, max_age_days=7):
    now = time.time()
    for folder in [output_folder, task_data_folder]:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path):
                mtime = os.path.getmtime(file_path)
                if now - mtime > max_age_days * 86400:
                    try:
                        os.remove(file_path)
                        print(f"自动清理: 删除过期文件 {file_path}")
                    except Exception as e:
                        print(f"自动清理失败: {file_path} {e}")
            elif os.path.isdir(file_path):
                mtime = os.path.getmtime(file_path)
                if now - mtime > max_age_days * 86400:
                    try:
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
    if not file:
        return jsonify({'error': '未选择任务文件'}), 400

    if not (file.filename.lower().endswith('.json') or file.filename.lower().endswith('.txt')):
        return jsonify({'error': '请上传 .json 或 .txt 任务文件'}), 400

    save_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(save_path)

    cookie = request.form.get('cookie', '')
    proxy_list_text = request.form.get('proxy_list', '').strip()
    pack_after_download = request.form.get('pack_after_download', 'false') == 'true'
    delete_after_pack = request.form.get('delete_after_pack', 'false') == 'true'
    thread_count = int(request.form.get('thread_count', 4))

    aes_key = request.form.get('aes_key', None)
    aes_iv = request.form.get('aes_iv', None)

    headers = {"User-Agent": "Mozilla/5.0"}
    if cookie:
        headers["Cookie"] = cookie

    proxy_list = []
    if proxy_list_text:
        proxy_list = parse_proxy_list(proxy_list_text)

    task_id = str(uuid.uuid4())
    initial_status = {
        'task_type': 'download', 
        'status': '等待执行',
        'progress_percent': 0,
        'progress_current': 0,
        'progress_total': 0,
        'logs': [],
        'result': None,
        'config': {
            'filename': file.filename,
            'pack_after_download': pack_after_download,
            'delete_after_pack': delete_after_pack,
            'thread_count': thread_count
        }
    }
    with task_status_lock:
        task_statuses[task_id] = initial_status
    save_task_status(task_id, initial_status)

    # 新增：为每个任务分配控制器
    download_controller = DownloadController()
    download_controllers[task_id] = download_controller

    def download_progress_callback(current, total, msg):
        with task_status_lock:
            task_data = task_statuses[task_id]
            task_data['progress_current'] = current
            task_data['progress_total'] = total
            task_data['progress_percent'] = (current / total * 100) if total > 0 else 0
            task_data['logs'].append(msg)
            save_task_status(task_id, task_data)

    def download_task_wrapper():
        with task_status_lock:
            task_data = task_statuses[task_id]
            task_data['status'] = '执行中'
            save_task_status(task_id, task_data) 
        
        try:
            result = process_task_file_with_progress(
                save_path, 
                app.config['OUTPUT_FOLDER'], 
                headers, 
                download_progress_callback,
                proxy_list,
                pack_after_download, 
                delete_after_pack,
                aes_key, aes_iv,
                max_workers=thread_count,
                download_controller=download_controller  # 传入控制器
            )
            with task_status_lock:
                task_data = task_statuses[task_id]
                task_data['status'] = '完成'
                task_data['result'] = result
                task_data['progress_percent'] = 100
                if task_data['progress_total'] > 0:
                    task_data['progress_current'] = task_data['progress_total']
                save_task_status(task_id, task_data)
        except Exception as e:
            with task_status_lock:
                task_data = task_statuses[task_id]
                task_data['status'] = '失败'
                task_data['logs'].append(f"❌ 任务执行失败: {e}")
                task_data['result'] = {'error': str(e)}
                task_data['progress_percent'] = 0
                save_task_status(task_id, task_data)

    executor.submit(download_task_wrapper)

    return jsonify({'status': 'ok', 'task_id': task_id})

@app.route('/pause/<task_id>', methods=['POST'])
def pause_task(task_id):
    ctrl = download_controllers.get(task_id)
    if ctrl:
        ctrl.pause()
        with task_status_lock:
            task_data = task_statuses.get(task_id)
            if task_data:
                task_data['status'] = '暂停中'
                task_data['logs'].append("⏸️ 用户暂停了任务")
                save_task_status(task_id, task_data)
        return jsonify({'status': 'paused'})
    return jsonify({'error': '任务不存在'}), 404

@app.route('/resume/<task_id>', methods=['POST'])
def resume_task(task_id):
    ctrl = download_controllers.get(task_id)
    if ctrl:
        ctrl.resume()
        with task_status_lock:
            task_data = task_statuses.get(task_id)
            if task_data:
                task_data['status'] = '执行中'
                task_data['logs'].append("▶️ 用户恢复了任务")
                save_task_status(task_id, task_data)
        return jsonify({'status': 'resumed'})
    return jsonify({'error': '任务不存在'}), 404

@app.route('/status/<task_id>')
def get_status(task_id):
    with task_status_lock:
        status = task_statuses.get(task_id)
    if not status:
        status = load_task_status(task_id)
    if not status:
        return jsonify({'error': '任务不存在'}), 404
    return jsonify(status)

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['OUTPUT_FOLDER'], filename, as_attachment=True)

@app.route('/tasks')
def list_tasks():
    with task_status_lock:
        all_tasks = list(task_statuses.items())
    all_tasks.sort(key=lambda x: x[1].get('config', {}).get('filename', ''), reverse=True)
    return jsonify([
        {
            'task_id': tid,
            'status': t['status'],
            'filename': t.get('config', {}).get('filename', ''),
            'progress_percent': t.get('progress_percent', 0),
            'result': t.get('result', None)
        }
        for tid, t in all_tasks
    ])

@app.route('/rclone_upload', methods=['POST'])
def rclone_upload():
    filename = request.form.get('filename')
    remote_path = request.form.get('remote_path')
    if not filename or not remote_path:
        return jsonify({'error': '参数缺失'}), 400
    local_path = os.path.join(app.config['OUTPUT_FOLDER'], filename)
    if not os.path.exists(local_path):
        return jsonify({'error': '文件不存在'}), 404

    task_id = str(uuid.uuid4())
    initial_status = {
        'task_type': 'rclone_upload',
        'status': '等待执行',
        'progress_percent': 0,
        'progress_current': 0,
        'progress_total': 0,
        'logs': [],
        'result': None,
        'config': {
            'filename': filename,
            'remote_path': remote_path
        }
    }
    with task_status_lock:
        task_statuses[task_id] = initial_status
    save_task_status(task_id, initial_status)

    def rclone_upload_task():
        with task_status_lock:
            task_data = task_statuses[task_id]
            task_data['status'] = '执行中'
            save_task_status(task_id, task_data)
        try:
            cmd = [
                "rclone", "copy", local_path, remote_path,
                "--progress", "--stats=1s"
            ]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for line in process.stdout:
                with task_status_lock:
                    task_data = task_statuses[task_id]
                    task_data['logs'].append(line.strip())
                    save_task_status(task_id, task_data)
            process.wait()
            with task_status_lock:
                task_data = task_statuses[task_id]
                if process.returncode == 0:
                    task_data['status'] = '完成'
                    task_data['result'] = {'msg': '上传成功'}
                else:
                    task_data['status'] = '失败'
                    task_data['result'] = {'msg': '上传失败'}
                save_task_status(task_id, task_data)
        except Exception as e:
            with task_status_lock:
                task_data = task_statuses[task_id]
                task_data['status'] = '失败'
                task_data['logs'].append(f"❌ 上传失败: {e}")
                task_data['result'] = {'error': str(e)}
                save_task_status(task_id, task_data)

    executor.submit(rclone_upload_task)
    return jsonify({'status': 'ok', 'task_id': task_id})

@app.route('/rclone_status/<task_id>')
def rclone_status(task_id):
    with task_status_lock:
        status = task_statuses.get(task_id)
    if not status:
        status = load_task_status(task_id)
    if not status:
        return jsonify({'error': '任务不存在'}), 404
    return jsonify(status)

if __name__ == '__main__':
    load_all_task_statuses()
    auto_cleanup(app.config['OUTPUT_FOLDER'], app.config['TASK_DATA_FOLDER'])
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
