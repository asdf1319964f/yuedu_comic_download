# --- START OF FILE downloader.py ---

import os
import re
import json
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from Crypto.Cipher import AES

def sanitize_filename(filename):
    """
    净化文件名，移除或替换掉不适合在文件名或URL中使用的字符。
    """
    if not filename:
        return "untitled"
    # 移除非法字符
    sanitized = re.sub(r'[\\/*?:"<>|]', "", filename)
    # 将多个空格替换为单个空格
    sanitized = re.sub(r'\s+', ' ', sanitized).strip()
    return sanitized

def aes_decrypt(data, key, iv):
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted = cipher.decrypt(data)
    # PKCS7 unpadding
    pad_len = decrypted[-1]
    if pad_len > 0 and pad_len <= 16:
        decrypted = decrypted[:-pad_len]
    return decrypted

def is_img_line(line):
    line = line.strip()
    if not line:
        return False
    if re.match(r'^https?://', line):
        return True
    if '<img' in line and 'src=' in line:
        return True
    return False

def parse_img_src(src):
    src = src.strip()
    m = re.search(r'<img\s+[^>]*src=[\'"]([^\'"]+)[\'"]', src, re.IGNORECASE)
    if m:
        src = m.group(1)
    if ',{' in src:
        url, headers_str = src.split(',{', 1)
        headers_str = '{' + headers_str.strip().rstrip('">').rstrip('\'').rstrip('"')
    else:
        url = src
        headers_str = None
    url = url.strip()
    if not url.lower().startswith("http"):
        m2 = re.search(r'(https?://[^\s\'">]+)', url)
        if m2:
            url = m2.group(1)
    referer = None
    origin = None
    if headers_str:
        try:
            headers_json = headers_str.replace("'", '"')
            headers_json = re.sub(r'([a-zA-Z0-9_]+):', r'"\1":', headers_json)
            data = json.loads(headers_json)
            headers = data.get("headers") or data.get("header") or data
            referer = headers.get("Referer") or headers.get("referer")
            origin = headers.get("Origin") or headers.get("origin")
        except Exception:
            try:
                import ast
                data = ast.literal_eval(headers_str)
                headers = data.get("headers") or data.get("header") or data
                referer = headers.get("Referer") or headers.get("referer")
                origin = headers.get("Origin") or headers.get("origin")
            except Exception:
                pass
    return url, referer, origin

def parse_txt_task_file(txt_path):
    with open(txt_path, "r", encoding="utf-8") as f:
        text = f.read()
    raw_title = os.path.splitext(os.path.basename(txt_path))[0]
    title = sanitize_filename(raw_title)
    author = "未知作者"
    m = re.search(r'作者[:：]\s*([^\n]+)', text)
    if m:
        author = m.group(1).strip()
    global_referer = None
    m = re.search(r'📌当前源站：[^⓪]*⓪(https?://[^\s]+)', text)
    if m:
        global_referer = m.group(1).strip()
    image_tasks = []
    chapter_title = "第1章"
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if is_img_line(line):
            url, referer, origin = parse_img_src(line)
            headers = {}
            if referer:
                headers['Referer'] = referer
            if origin:
                headers['Origin'] = origin
            image_tasks.append((url, headers, chapter_title))
        else:
            chapter_title = sanitize_filename(line)
    return title, author, global_referer, image_tasks

def parse_json_task_file(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    raw_title = data.get("title") or os.path.splitext(os.path.basename(json_path))[0]
    title = sanitize_filename(raw_title)
    author = data.get("author", "未知作者")
    global_referer = data.get("referer")
    image_tasks = []
    for chapter in data.get("chapters", []):
        raw_chapter_title = chapter.get("title", "第1章")
        chapter_title = sanitize_filename(raw_chapter_title)
        for img in chapter.get("images", []):
            url = img.get("url")
            headers = img.get("headers", {})
            image_tasks.append((url, headers, chapter_title))
    return title, author, global_referer, image_tasks

def download_image(url, save_path, headers, proxies=None, retry=3, aes_key=None, aes_iv=None):
    for i in range(retry):
        try:
            r = requests.get(url, headers=headers, proxies=proxies, timeout=20)
            if r.status_code == 200:
                data = r.content
                if aes_key and aes_iv:
                    key = bytes.fromhex(aes_key) if len(aes_key) in (32, 48, 64) else aes_key.encode('utf-8')
                    iv = bytes.fromhex(aes_iv) if len(aes_iv) == 32 else aes_iv.encode('utf-8')
                    data = aes_decrypt(data, key, iv)
                with open(save_path, "wb") as f:
                    f.write(data)
                return True
        except Exception as e:
            if i == retry - 1:
                raise
    return False

class DownloadController:
    def __init__(self):
        self._pause_event = threading.Event()
        self._pause_event.set()
        self._stop_event = threading.Event()
    def pause(self): self._pause_event.clear()
    def resume(self): self._pause_event.set()
    def stop(self): self._stop_event.set()
    def check(self):
        if self._stop_event.is_set(): raise Exception("任务被终止")
        self._pause_event.wait()

def process_task_file_with_progress(
    task_path, output_folder, headers, progress_callback, proxy_list=None,
    pack_after_download=True, delete_after_pack=False, aes_key=None, aes_iv=None,
    max_workers=4, download_controller=None, custom_referer=None,
    redis_client=None
):
    if task_path.lower().endswith(".json"):
        title, author, global_referer, image_tasks = parse_json_task_file(task_path)
    else:
        title, author, global_referer, image_tasks = parse_txt_task_file(task_path)
    book_dir = os.path.join(output_folder, title)
    os.makedirs(book_dir, exist_ok=True)
    total = len(image_tasks)
    progress_callback(0, total, f"开始下载，共{total}张图片")
    lock = threading.Lock()
    finished = [0]
    failed = []

    REDIS_DOWNLOADED_URL_KEY_PREFIX = "comic_downloader:url:"

    def is_url_already_downloaded(url):
        if not redis_client:
            return False
        return redis_client.exists(f"{REDIS_DOWNLOADED_URL_KEY_PREFIX}{url}")

    def mark_url_as_downloaded(url):
        if not redis_client:
            return
        redis_client.set(f"{REDIS_DOWNLOADED_URL_KEY_PREFIX}{url}", "1", ex=90*24*60*60)


    def download_one(idx, url, headers_img, chapter_title):
        if download_controller:
            download_controller.check()

        if is_url_already_downloaded(url):
            with lock:
                finished[0] += 1
                progress_callback(finished[0], total, f"Redis记录已下载，跳过URL: {url[:50]}...")
            return

        chapter_dir = os.path.join(book_dir, chapter_title)
        os.makedirs(chapter_dir, exist_ok=True)
        
        ext = os.path.splitext(url)[1].split("?")[0]
        if not ext or len(ext) > 6:
            ext = ".jpg"
        filename = f"{idx+1:04d}{ext}"
        save_path = os.path.join(chapter_dir, filename)
        
        h = dict(headers)
        h.update(headers_img)
        if 'Referer' not in h and 'referer' not in h:
            if global_referer:
                h['Referer'] = global_referer
            elif custom_referer:
                h['Referer'] = custom_referer

        proxies = None
        if proxy_list:
            import random
            proxy = random.choice(proxy_list)
            proxies = {"http": proxy, "https": proxy}
        try:
            download_image(url, save_path, h, proxies, aes_key=aes_key, aes_iv=aes_iv)
            
            mark_url_as_downloaded(url)

            with lock:
                finished[0] += 1
                progress_callback(finished[0], total, f"下载成功: {chapter_title}/{filename}")
        except Exception as e:
            with lock:
                finished[0] += 1
                failed.append((idx, url, headers_img, chapter_title))
                progress_callback(finished[0], total, f"下载失败: {chapter_title}/{filename} {e}")

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = []
        for idx, (url, headers_img, chapter_title) in enumerate(image_tasks):
            futures.append(pool.submit(download_one, idx, url, headers_img, chapter_title))
        for f in as_completed(futures):
            pass

    max_retry_round = 3
    retry_round = 1
    while failed and retry_round <= max_retry_round:
        retry_failed = list(failed)
        failed.clear()
        progress_callback(finished[0], total, f"第{retry_round}轮重试，剩余{len(retry_failed)}张")
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = []
            for idx, url, headers_img, chapter_title in retry_failed:
                futures.append(pool.submit(download_one, idx, url, headers_img, chapter_title))
            for f in as_completed(futures):
                pass
        retry_round += 1

    final_archive_name = f"{title}.cbz"
    if pack_after_download and os.path.exists(book_dir):
        import shutil
        archive_base_path = os.path.join(output_folder, title)
        if os.path.exists(archive_base_path + ".zip"):
            os.remove(archive_base_path + ".zip")
        created_zip_file = shutil.make_archive(archive_base_path, 'zip', book_dir)
        final_cbz_path = os.path.join(output_folder, final_archive_name)
        if os.path.exists(final_cbz_path):
            os.remove(final_cbz_path)
        os.rename(created_zip_file, final_cbz_path)
        if delete_after_pack:
            shutil.rmtree(book_dir)
            
    progress_callback(total, total, f"全部完成，失败{len(failed)}张")
    return [{"zip": final_archive_name, "failed": [x[1] for x in failed]}]

# --- END OF FILE downloader.py ---
