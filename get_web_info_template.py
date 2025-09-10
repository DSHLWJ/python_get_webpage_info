# -*- coding: utf-8 -*-
import json
import logging
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from types import SimpleNamespace

import cloudscraper
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from httpx._urlparse import urlparse

# 加载 .env 文件
load_dotenv(".env")  # 如果文件名是 .env，默认可以省略参数


request_base_url = os.getenv("request_base_url")
save_root = os.getenv("save_root")

# 重新肯定是更新成功的
save_update_mark = False

# 错误抓取肯定是抓取失败的
save_retry_mark = True

# 最大线程数
MAX_WORKERS=10

def delay_time():
    time.sleep(random.uniform(1, 3))

# 开启测试
test_mark_my = True

# 测试文件所在位置
test_save_file_path = f"all_movies.json"
# 测试的分类
test_keywords_arr = [{'keyName': '12345'}]
# 测试的演员对象
test_actor_info_object = {
    }

# 创建全局 scraper（保持 Session 复用）
scraper = cloudscraper.create_scraper()
# 配置日志
log_dir=os.getenv("log_dir")

os.makedirs(log_dir, exist_ok=True)

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_file = os.path.join(log_dir, f"download_{timestamp}.log")
logging.basicConfig(
    level=logging.INFO,  # 改成 INFO，这样 INFO/ERROR 都会输出
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)



Referer = ["https://.com/"]

values = ["Hello"]
keys = ['你好']





# 生成字典
video_tap_map = dict(zip(keys, values))

Referer_json_file = "Referer_web.json"
if not os.path.exists(Referer_json_file):
    with open(Referer_json_file, "w", encoding="utf-8") as f:
        json.dump([], f, ensure_ascii=False, indent=4)
    print(f"⚠️ 创建空文件: {Referer_json_file}")
else:
    # === 保存为 JSON 文件 ===
    with open(Referer_json_file, "r", encoding="utf-8") as f:
        a_href_list = json.load(f)
    Referer = a_href_list

# 常见浏览器 UA 列表
USER_AGENTS = [
    # Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0) Gecko/20100101 Firefox/142.0",
    # Edge
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
]

MAX_RETRIES = 3

global_cookies = ""
cookie_file = "cookies.json"
with open(cookie_file, "r", encoding="utf-8") as f:
    global_cookies = json.load(f)


def log_download(msg_info, success_status):
    if success_status:
        msg = f"{msg_info} | 状态: 成功"
        logging.info(msg)
    else:
        msg = f"{msg_info} | 状态: 失败"
        logging.error(msg)


if len(keys) != len(values):
    raise ValueError("keys 和 values 数组长度不一致")




# 解析请求头
def headers_txt_to_dict(file_path):
    headers = {}
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or ":" not in line:
                continue
            key, value = line.split(":", 1)  # 只分割一次
            headers[key.strip()] = value.strip()
    return headers


# 解析开头首页面演员信息（头像图片链接、名字、个人详情页链接）
def get_actor_info_form_html(soup):
    actors_arr = []
    for box in soup.find_all("div", class_="box actor-box"):
        img_tag = box.find("img", class_="avatar")
        strong_tag = box.find("strong")
        a_tag = box.find("a")
        a_href = a_tag["href"]
        img_src = img_tag["src"] if img_tag else None
        name = strong_tag.get_text(strip=True) if strong_tag else None
        actors_arr.append({
            "name": name,
            "img": img_src,
            "a_href": a_href,
            "actor_img_download": 3,
            "is_request_movies": 3,
            "all_movies_arr": []
        })
    return actors_arr


def get_actor_movies_info_list_page_form_html(soup):
    movies_info_arr = []
    for item in soup.select("div.movie-list div.item"):
        movies_name = ""
        title_div = item.find("div", class_="video-title")
        if title_div:
            movies_name = title_div.get_text(strip=True)
        a_tag = item.find("a", href=True)
        href = a_tag["href"] if a_tag else ""
        img_tag = item.find("img", src=True)
        img_src = img_tag["src"] if img_tag else ""
        score_tag = item.select_one("div.score span.value")
        score = score_tag.get_text(strip=True) if score_tag else ""
        meta_tag = item.find("div", class_="meta")
        meta = meta_tag.get_text(strip=True) if meta_tag else ""
        movies_info_arr.append({
            "movies_name": movies_name,
            "movies_href": href,
            "movies_img": img_src,
            "movies_score": score,
            "movies_meta": meta,
            "movies_is_install": False,
            "movies_is_request": False,
            "movies_magnet_arr": [],
            "movies_screenshot_url_arr": [],
            "movies_cls": [],
            "movies_img_is_request": False
        })
    return movies_info_arr


def get_actor_movies_magnet_info__form_html(soup):
    video_info = {}
    video_info["Tags"] = ""
    movies_magnet_arr = []

    movies_cls = []
    movies_screenshot_url_arr = []

    tag = soup.find("div", class_="video-detail", attrs={"data-controller": "movie-detail"})

    h2_tag = soup.find("h2", class_="title is-4")

    if tag:
        # 找到父节点 div
        div = soup.find("div", class_="column column-video-cover")

        if div:
            magnet_arr = []
            for item in soup.select("div#magnets-content div.item"):
                a_tag = item.find("div", class_="magnet-name").find("a", href=True)
                if a_tag:
                    magnet_link = a_tag["href"]
                    name_tag = a_tag.find("span", class_="name")
                    name = name_tag.get_text(strip=True) if name_tag else ""
                    #  1 不可用 2 可用 3 未知
                    magnet_arr.append({"magnet_link": magnet_link, "is_available": 3})

            movies_magnet_arr = magnet_arr

        movies_screenshot_url_arr = []
        for a_tag in soup.select("div.tile-images.preview-images a.tile-item"):
            img_tag = a_tag.find("img")
            if img_tag:
                img_src = img_tag.get("src", "")
                img_alt = img_tag.get("alt", "")
                data_caption = a_tag.get("data-caption", "")
                movies_screenshot_url_arr.append({
                    "src": img_src,
                    "alt": img_alt,
                    "caption": data_caption,
                    "is_dl_success": False
                })

        nav = soup.find('nav', class_='panel movie-panel-info')
        video_info = {}
        video_info["Tags"] = []
        if nav:
            # 遍历每个 panel-block
            for block in nav.find_all('div', class_='panel-block', recursive=False):
                strong_tag = block.find('strong')
                # 有标题的 block
                try:
                    if strong_tag:
                        key = strong_tag.get_text(strip=True).replace(":", "")
                        value_span = block.find('span', class_='value')

                        if value_span:
                            # 检查是否有 a 标签
                            links = value_span.find_all('a')
                            if links:
                                video_info[key] = ', '.join([a.get_text(strip=True) for a in links])
                            else:
                                # 取纯文本并清理
                                text = value_span.get_text(separator=',', strip=True)
                                video_info[key] = ', '.join([t.strip() for t in text.split(',') if t.strip()])
                        else:
                            # 没有 span.value 的直接文本
                            text = block.get_text(separator=',', strip=True)
                            text = text.replace(strong_tag.get_text(strip=True), '').strip()
                            video_info[key] = text
                    else:
                        # 没有 strong 的 block，可能是想看/看过人数
                        span_tag = block.find('span', class_='is-size-7')
                        if span_tag:
                            text = span_tag.get_text(separator=',', strip=True)
                            video_info['觀看統計'] = ', '.join([t.strip() for t in text.split(',') if t.strip()])
                except Exception as e:
                    print("错误" + str(e))

        # map映射，英译中

        # 将原始字典映射成英文字段
        try:
            if video_info["Tags"] is not None and len(video_info["Tags"]) > 0:
                text = video_info["Tags"]
                tags = [video_tap_map.get(tag.strip(), tag.strip()) for tag in text.split(",")]
                translated_text = ", ".join(tags)
                video_info["Tags"] = translated_text
        except Exception as e:
            print("错误" + str(e))

    # 找到 h2 标签

    else:
        if h2_tag:
            # 查找 strong 标签
            strong_tag = soup.find("strong")
            # 找到其中的 strong
            if strong_tag:  # strong 存在
                strong_text = strong_tag.get_text(strip=True)
                target = "開通VIP"
                if strong_text == target:
                    log_download("已成功抓取是VIP影片", success_status=True)
                    video_info["Tags"] = ["開通VIP"]
                    return movies_magnet_arr, movies_screenshot_url_arr, video_info

    movies_cls.append(video_info)

    return movies_magnet_arr, movies_screenshot_url_arr, movies_cls


def init_json_load_file(file_path):
    data_arr = []
    try:
        if not os.path.exists(file_path):
            with open(file_path, "w", encoding="utf-8") as ef:
                json.dump([], ef, ensure_ascii=False, indent=4)
        else:
            with open(file_path, "r+", encoding="utf-8") as ef:
                data_arr = json.load(ef)
    except:
        log_download(f"打开错误文件是{file_path}",success_status=False)
    return data_arr


def update_json_load_file(file_path, update_data):
    try:
        with open(file_path, "w", encoding="utf-8") as ef:
            json.dump(update_data, ef, ensure_ascii=False, indent=4)
        return True
    except Exception as e:
        log_download(f"保存文件出错{file_path} ||出错数据为: {str(update_data)}||出错原因是: {str(e)}", success_status=False)
        return False


def get_next_page_tag_form_html(soup):
    next_page_tag = soup.find("a", rel="next", class_="pagination-next")
    if next_page_tag and next_page_tag.get("href"):
        return next_page_tag["href"]
    else:
        return None


def fetch_page_with_cookies(url):
    """
    使用 cloudscraper + 浏览器登录后的 Cookie 获取页面内容
    :param url: 目标页面 URL
    :return: (status_code, html_text)
    """
    # 3. 设置请求头

    is_referer = False
    RETRY_COUNT = 0
    while True:
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Connection": "keep-alive",
                "Referer":random.choice(Referer) ,
                "Host": "",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6"
            }
            delay_time()
            # 4. 发起请求
            with scraper.get(url, cookies=global_cookies, headers=headers, stream=True, timeout=25) as resp:
                if resp.status_code == 200:
                    return resp.status_code, resp.text
                else:
                    log_download(f"请求失败 {url}  状态码:{str(resp.status_code)}" , success_status=False)
                    return resp.status_code, resp.text
        except Exception as e:
            RETRY_COUNT += 1
            if RETRY_COUNT > MAX_RETRIES:
                log_download("超过最大请求次数五次", success_status=False)
                # 构造一个假的 resp，带 status_code
                resp = SimpleNamespace(status_code=404, text=None, content=None)
                return resp.status_code, resp.text


def download_image(request_url, save_path):
    """
    使用 cloudscraper + Cookie 下载图片
    :param url: 图片 URL
    :param cookies: 登录 Cookie 字典
    :param headers: 请求头
    :param save_path: 图片保存路径
    :return: HTTP 状态码
    """

    # 3. 设置请求头
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
        "Referer": random.choice(Referer),
        "Sec-Fetch-Dest": "image",
        "Host": "",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6"
    }

    RETRY_COUNT = 0

    while True:
        try:
            delay_time()

            with scraper.get(request_url, cookies=global_cookies, headers=headers, stream=True, timeout=25) as resp:
                if resp.status_code == 200:
                    os.makedirs(os.path.dirname(save_path), exist_ok=True)
                    with open(save_path, "wb") as f:
                        for chunk in resp.iter_content(1024):
                            if chunk:
                                f.write(chunk)
                    log_download(f"图片下载成功地址是 {request_url}", success_status=True)
                    return resp.status_code
                else:
                    log_download(f"图片下载失败地址是{request_url}", success_status=False)
                    return resp.status_code

        except Exception as e:
            RETRY_COUNT += 1
            if RETRY_COUNT > MAX_RETRIES:
                log_download(f"图片下载失败，原因: {e} | 网址: {request_url}", success_status=False)
                # 返回一个假对象，避免 resp 未定义
                fake_resp = SimpleNamespace(status_code=500, text=None, content=None)
                return fake_resp.status_code
            else:
                log_download(f"下载出错，重试第 {RETRY_COUNT} 次: {request_url}", success_status=False)


def download_cotter(keyword, file_path_keyword_page_json):
    url_keyname = "/actors/" + keyword
    state_step = "step1"

    keyword_page_dict = init_json_load_file(file_path_keyword_page_json)

    keyword_page_dict_error = []

    request_base_keyword_page_end_url = ""

    request_base_keyword_page_url = ""

    if len(keyword_page_dict) > 0:
        keyword_page_dict_error = next(
            (obj for obj in keyword_page_dict if obj["url_keyName_request_success"] == False), [])
        if len(keyword_page_dict_error) == 0:
            state_step = "step3"
    if len(keyword_page_dict_error) > 0:
        state_step = "step4"
    try:
        while True:
            if state_step == "step1":

                keyword_page_dict_object = {
                    "url_keyname": [],
                    "keyword_actors_dict_arr": [],
                    "url_keyName_request_success": False
                }
                if request_base_keyword_page_end_url is not None and request_base_keyword_page_end_url.strip() != "":
                    request_base_keyword_page_url = request_base_keyword_page_end_url
                else:
                    request_base_keyword_page_url = request_base_url + url_keyname

                keyword_page_dict_object["url_keyname"] = request_base_keyword_page_url

                log_download(f"正在请求= {request_base_keyword_page_url}", success_status=True)
                status, html = fetch_page_with_cookies(request_base_keyword_page_url)
                if status != 200:
                    raise Exception(f"请求失败，状态码: {status}")
                soup = BeautifulSoup(html, 'html.parser')
                log_download(f"请求成功{request_base_keyword_page_url}" , success_status=True)
                keyword_actors_dict_arr = get_actor_info_form_html(soup)

                index = next((i for i, obj in enumerate(keyword_page_dict) if
                              obj["url_keyname"] == request_base_keyword_page_url), None)
                if index is not None:
                    keyword_page_dict[index]["keyword_actors_dict_arr"] = keyword_actors_dict_arr
                    keyword_page_dict[index]["url_keyName_request_success"] = True
                else:
                    keyword_page_dict_object["keyword_actors_dict_arr"] = keyword_actors_dict_arr
                    keyword_page_dict_object["url_keyName_request_success"] = True
                    keyword_page_dict.append(keyword_page_dict_object)

                update_json_load_file(file_path_keyword_page_json, keyword_page_dict)

                exists_next_page_tag = get_next_page_tag_form_html(soup)
                old_exists_next_page_tag = url_keyname
                if exists_next_page_tag and exists_next_page_tag != old_exists_next_page_tag:
                    url_keyname = exists_next_page_tag
                    request_base_keyword_page_end_url = ""
                    state_step = "step1"
                else:
                    log_download("爬取所有演员信息结束", success_status=True)
                    break
            elif state_step == "step3":
                if save_update_mark:
                    request_base_keyword_page_end_url = keyword_page_dict[len(keyword_page_dict) - 1]["url_keyname"]
                    keyword_page_dict.remove(keyword_page_dict[len(keyword_page_dict) - 1])
                    state_step = "step1"
                else:
                    log_download(f"当前分类{keyword}不做演员更新",success_status=True)
                    break
            elif state_step == "step4":
                if save_retry_mark:
                    try:
                        for keyword_page_dict_error_object in keyword_page_dict_error:
                            request_base_keyword_page_error_url = request_base_url + keyword_page_dict_error_object[
                                "keyword_page_href"]
                            status, html = fetch_page_with_cookies(request_base_keyword_page_error_url)
                            if status != 200:
                                raise Exception(f"请求失败，状态码: {status}")
                            soup = BeautifulSoup(html, 'html.parser')
                            keyword_actors_dict_arr_error_update = get_actor_info_form_html(soup)
                            if keyword_actors_dict_arr:
                                index = next((i for i, obj in enumerate(keyword_page_dict) if
                                              obj["url_keyname"] == request_base_keyword_page_error_url), None)
                                keyword_page_dict[index]["keyword_actors_dict_arr"] = keyword_actors_dict_arr_error_update
                                keyword_page_dict[index]["url_keyName_request_success"] = True
                        state_step == "step3"
                    except Exception as e:
                        log_download("重新请求的页面加载失败 " + str(e), success_status=False)
                        state_step == "step3"
                else:
                    state_step == "step3"
    except Exception as e:
        log_download("程序错误,请求终止 " + request_base_keyword_page_url+str(e), success_status=False)


def create_folder(save_folder_path):
    os.makedirs(save_folder_path, exist_ok=True)


def download_image_actor_picture(actor_info_object, save_actor_folder_root):
    actor_info_object_movies_arr = []
    download_url = ""
    try:
        if actor_info_object["actor_img_download"] == 2:
            log_download(f"已下载过{actor_info_object["name"]}的头像", success_status=True)
            return actor_info_object
        else:
            actor_info_object["actor_img_download"] = 3

        download_url = actor_info_object["img"]

        save_folder_path = os.path.join(save_actor_folder_root, actor_info_object["name"])
        create_folder(save_folder_path)

        path = urlparse(download_url).path  # 提取 URL 中的路径部分
        _, ext = os.path.splitext(path)

        save_actor_picture_file = os.path.join(save_folder_path, actor_info_object["name"] + ext)

        save_movies_all_actor_json_file = os.path.join(save_folder_path, actor_info_object["name"] + "_all_movies.json")

        actor_info_object_movies_arr = init_json_load_file(save_movies_all_actor_json_file)
        if len(actor_info_object_movies_arr) <= 0:
            actor_info_movies_object = actor_info_object
            actor_info_movies_object["all_movies_info"] = []
            actor_info_object_movies_arr.append(actor_info_movies_object)
        else:
            actor_info_object_movies_arr = [actor_info_object]

        update_json_load_file(save_movies_all_actor_json_file, actor_info_object_movies_arr)
        status = download_image(download_url, save_actor_picture_file)
        if status != 200:
            raise Exception(f"请求失败，状态码: {status}")

        actor_info_object["actor_img_download"] = 2
        update_json_load_file(save_movies_all_actor_json_file, actor_info_object_movies_arr)

        log_download("下载头像成功 " + actor_info_object["name"], success_status=True)
        return actor_info_object
    except Exception as e:
        log_download(f"下载演员头像失败{actor_info_object["name"]} 失败地址 {download_url} 失败原因 {str(e)}", success_status=False)
        update_json_load_file(save_movies_all_actor_json_file, actor_info_object_movies_arr)
        return actor_info_object


def download_movies_info(save_actor_moviese_file_json):
    request_base_movies_page_url = ""

    try:
        actor_movies_info = init_json_load_file(save_actor_moviese_file_json)
        actor_all_movies_info_url_arr = actor_movies_info[0]["all_movies_info"]
        actor_name = actor_movies_info[0]["name"]
        actor_all_movies_info_page_index = 0
    except Exception as a:
        log_download(f"文件缺少重新生成all movies文件{save_actor_moviese_file_json}",success_status=False)
        data = init_json_load_file(save_actor_moviese_file_json)
        filename = os.path.basename(save_actor_moviese_file_json)  # あかね杏珠_all_movies.json
        # 去掉扩展名
        name_part = os.path.splitext(filename)[0]  # あかね杏珠_all_movies
        # 去掉后缀 "_all_movies"
        actor_name = name_part.replace("_all_movies", "")
        result = next((obj for obj in data if obj["name"] == actor_name), None)
        result["all_movies_info"] = []
        result_arr = [result]
        update_json_load_file(save_actor_moviese_file_json, result_arr)
        return

    page_movies_arr = []

    # 状态机
    status_step = "step3"

    request_movies_page_actor_url = ""


    while True:
        try:
            while True:
                if status_step == "step2" or status_step == "step1" :
                    break
                # 错误页面重新抓取
                if actor_movies_info[0]["is_request_movies"] == 2:
                    if save_update_mark:
                        if len(actor_all_movies_info_url_arr) >0 :
                                log_download(f"不做演员{actor_name}的错误页面重新爬取,抓取下一页看看",success_status=True)
                                request_base_movies_page_url = actor_all_movies_info_url_arr[len(actor_all_movies_info_url_arr) - 1]["page_movies_url"]
                                status_step = "step1"
                        else:
                            request_base_movies_page_url = actor_movies_info[0]["a_href"]
                            if save_retry_mark:
                                request_base_movies_page_url = actor_all_movies_info_url_arr[len(actor_all_movies_info_url_arr) - 1]["page_movies_url"]
                                status_step == "step1"
                            else:
                                log_download(f"已经抓取过演员{actor_name}的主演电影,并且不去更新任何操作",success_status=True)
                                status_step == "step2"
                    else:
                        status_step = "step2"
                else:
                    if  save_retry_mark :
                        try:
                            for actor_all_movies_info_url_arr_object in actor_all_movies_info_url_arr:
                                if actor_all_movies_info_url_arr_object["is_request_page_success"]:
                                    log_download(f"这个页面已经抓取过{actor_movies_info[0]["name"]}的电影信息", success_status=True)
                                    continue
                                else:
                                    request_movies_page_actor_url = request_base_url +  actor_all_movies_info_url_arr_object[ "page_movies_url"]
                                    log_download( f"正在重新抓取{actor_movies_info[0]["name"]}的电影页面 {request_movies_page_actor_url}", success_status=True)

                                    status, html = fetch_page_with_cookies(request_movies_page_actor_url)
                                    if status != 200:
                                        raise Exception(f"请求失败，状态码: {status}")
                                    soup = BeautifulSoup(html, 'html.parser')

                                    from_html_actor_all_movies_info_arr = get_actor_movies_info_list_page_form_html(soup)

                                    old_actor_all_page_movies_arr = actor_all_movies_info_url_arr_object[ "page_movies_arr"]
                                    for from_html_actor_all_movies_info_arr_object in from_html_actor_all_movies_info_arr:

                                        if not any(item["movies_name"] == from_html_actor_all_movies_info_arr_object[ "movies_name"] for item in old_actor_all_page_movies_arr):

                                            old_actor_all_page_movies_arr.append( from_html_actor_all_movies_info_arr_object)
                                        else:
                                            log_download(f"已有这个电影信息{from_html_actor_all_movies_info_arr_object["movies_name"]}",success_status=True)

                                    actor_all_movies_info_url_arr_object["page_movies_arr"] = old_actor_all_page_movies_arr

                                    actor_all_movies_info_url_arr_object["is_request_page_success"] = True

                                    actor_movies_info[0]["all_movies_info"] = actor_all_movies_info_url_arr

                                    update_json_load_file(save_actor_moviese_file_json, actor_movies_info)
                            status_step = "step2"
                        except Exception as e:
                            log_download(f"重新抓取{actor_movies_info[0]["name"]}电影页是{request_movies_page_actor_url}错误原因是{str(e)}",success_status=False)
                            status_step = "step2"
                    else:
                        status_step = "step2"

            #  抓取电影页面
            if status_step == "step1":

                if request_base_movies_page_url.strip() == "":
                    request_base_movies_page_url = actor_movies_info[0]["a_href"]
                    request_movies_page_actor_url = request_base_url + request_base_movies_page_url

                request_movies_page_actor_url = request_base_url + request_base_movies_page_url
                log_download(f"正在抓取演员={actor_name}的电影页面{request_movies_page_actor_url}", success_status=True)
                movies_info_object = {
                    "page_movies_url": request_base_movies_page_url,
                    "page_movies_arr": page_movies_arr,
                    "is_request_page_success": False
                }
                index = next((i for i, obj in enumerate(actor_all_movies_info_url_arr) if obj["page_movies_url"] == request_base_movies_page_url), None)

                if index is None:
                    actor_all_movies_info_url_arr.append(movies_info_object)

                status, html = fetch_page_with_cookies(request_movies_page_actor_url)
                if status != 200:
                    raise Exception(f"请求失败，状态码: {status}")

                soup = BeautifulSoup(html, 'html.parser')

                from_html_actor_all_movies_info_arr = get_actor_movies_info_list_page_form_html(soup)

                if index is None:
                    actor_all_movies_info_url_arr[actor_all_movies_info_page_index]["page_movies_arr"] = from_html_actor_all_movies_info_arr
                    actor_all_movies_info_url_arr[actor_all_movies_info_page_index]["is_request_page_success"] = True
                else:
                    old_actor_all_page_movies_arr  = actor_all_movies_info_url_arr[index]["page_movies_arr"]
                    for from_html_actor_all_movies_info_arr_object in from_html_actor_all_movies_info_arr:
                        if not any(item["movies_name"] == from_html_actor_all_movies_info_arr_object["movies_name"] for item in old_actor_all_page_movies_arr):
                            old_actor_all_page_movies_arr.append(from_html_actor_all_movies_info_arr_object)
                        else:
                            log_download(f"已有这个电影信息{from_html_actor_all_movies_info_arr_object["movies_name"]}",success_status=True)

                    actor_all_movies_info_url_arr[index]["is_request_page_success"] = True
                    actor_all_movies_info_url_arr[index]["page_movies_arr"] = old_actor_all_page_movies_arr

                actor_movies_info[0]["all_movies_info"] = actor_all_movies_info_url_arr

                update_json_load_file(save_actor_moviese_file_json, actor_movies_info)

                new_next_page = get_next_page_tag_form_html(soup)

                if new_next_page:
                    request_base_movies_page_url = new_next_page
                    update_json_load_file(save_actor_moviese_file_json, actor_movies_info)
                    log_download(f"正在抓取下一页 {new_next_page} ", success_status=True)
                else:
                    log_download(f"爬取所有影片信息结束，下一步开始抓取磁力链信息 {request_movies_page_actor_url}", success_status=True)
                    actor_movies_info[0]["all_movies_info"] = actor_all_movies_info_url_arr
                    actor_movies_info[0]["is_request_movies"] = 2
                    update_json_load_file(save_actor_moviese_file_json, actor_movies_info)
                    break

            elif status_step == "step2":
                log_download(f"不做演员{actor_name}电影的更新操作", success_status=True)
                break
        except Exception as e:
            actor_movies_info[0]["all_movies_info"] = actor_all_movies_info_url_arr
            actor_movies_info[0]["is_request_movies"] = 2
            update_json_load_file(save_actor_moviese_file_json, actor_movies_info)
            log_download(f"爬取失败的电影页{request_movies_page_actor_url},原因{str(e)}", success_status=False)
            break


def download_movies_magnet_info(save_actor_moviese_file_json):


    log_download(f"开始抓取电影的磁力链信息，文件是{str(save_actor_moviese_file_json)}", success_status=True)
    actot_movies_info = init_json_load_file(save_actor_moviese_file_json)

    actor_all_movies_info_url_arr = []
    if len(actot_movies_info) > 0:
        actor_all_movies_info_url_arr = actot_movies_info[0]["all_movies_info"]

    if len(actor_all_movies_info_url_arr) != 0 and actor_all_movies_info_url_arr is not None:

        try:
            for actor_all_movies_info_url_arr_object in actor_all_movies_info_url_arr:
                page_movies_arr = actor_all_movies_info_url_arr_object["page_movies_arr"]
                is_exist_magnet_status = "step1"
                for page_movies_arr_object in page_movies_arr:
                    try:
                        if page_movies_arr_object["movies_is_request"]:
                            if save_update_mark:
                                log_download(f"所有影片重新更新磁力链信息，当前影片{page_movies_arr_object["movies_name"]}" ,success_status=True)
                                is_exist_magnet_status = "step2"
                            else:
                                log_download("当前影片已抓取过，不做任何重新请求" + page_movies_arr_object["movies_name"], success_status=True)
                                continue
                        else:
                            if save_retry_mark:
                                while True:
                                    try:
                                        if page_movies_arr_object["is_exist_magnet"]:
                                            log_download( f"已存在{page_movies_arr_object["movies_name"]}影片磁力链的信息", success_status=True)
                                            is_exist_magnet_status = "step1"
                                            break
                                        else:
                                            log_download(f"更新不存在{page_movies_arr_object["movies_name"]}影片磁力链的信息", success_status=True)
                                            is_exist_magnet_status = "step2"
                                            break
                                    except Exception as e:
                                        if len(page_movies_arr_object["movies_magnet_arr"]) > 0:
                                            page_movies_arr_object["is_exist_magnet"] = True
                                        else:
                                            page_movies_arr_object["is_exist_magnet"] = False
                                        log_download(f"旧文件不存在这个is_exist_magnet,重新生成", success_status=False)
                                        actot_movies_info[0]["all_movies_info"] = actor_all_movies_info_url_arr
                                        update_json_load_file(save_actor_moviese_file_json, actot_movies_info)

                        if is_exist_magnet_status == "step1":
                            continue

                        request_magneturl_info_url = request_base_url + page_movies_arr_object["movies_href"]

                        page_movies_arr_object["movies_is_request"] = False

                        status, html = fetch_page_with_cookies(request_magneturl_info_url)
                        if status != 200:
                            raise Exception(f"请求失败，状态码: {status}")
                        soup = BeautifulSoup(html, 'html.parser')

                        movies_magnet_arr, movies_screenshot_url_arr, movies_cls = get_actor_movies_magnet_info__form_html(soup)

                        if len(movies_magnet_arr) <=0:
                            page_movies_arr_object["is_exist_magnet"] = False
                        else:
                            page_movies_arr_object["is_exist_magnet"] = True

                        page_movies_arr_object["movies_is_request"] = True
                        page_movies_arr_object["movies_magnet_arr"] = movies_magnet_arr
                        page_movies_arr_object["movies_screenshot_url_arr"] = movies_screenshot_url_arr
                        page_movies_arr_object["movies_cls"] = movies_cls

                        log_download(f"电影磁力链接爬取成功网址是 {request_magneturl_info_url}" , success_status=True)

                        actot_movies_info[0]["all_movies_info"] = actor_all_movies_info_url_arr
                        update_json_load_file(save_actor_moviese_file_json, actot_movies_info)

                    except Exception as e:
                        log_download(f"电影磁力链接爬取失败,链接是 {request_magneturl_info_url}  失败原因是{str(e)}", success_status=False)
                        continue
                actor_all_movies_info_url_arr_object["page_movies_arr"] = page_movies_arr
            log_download("电影磁力链抓取结束", success_status=True)
        except Exception as e:
            update_json_load_file(save_actor_moviese_file_json, actot_movies_info)
            log_download(f"下载出错 {str(actor_all_movies_info_url_arr)} 失败原因是 {str(e)}",success_status=False)


def clean_filename(filename: str) -> str:
    """
    清理文件名：去掉控制字符和常见非法字符
    """
    # 1. 去掉控制字符（不可打印字符）
    filename = re.sub(r'[\x00-\x1f\x7f-\x9f]', "", filename)

    # 2. 去掉 Windows 文件名非法字符  \ / : * ? " < > |
    filename = re.sub(r'[\\/:*?"<>|]', "", filename)

    # 3. 去掉开头结尾的空格（避免 Windows 不允许）
    filename = filename.strip()

    return filename


def download_movies_magnet_info_picture(save_actor_moviese_file_json):
    actot_movies_info = init_json_load_file(save_actor_moviese_file_json)

    actor_all_movies_info_url_arr = []
    if len(actot_movies_info) > 0:
        actor_all_movies_info_url_arr = actot_movies_info[0]["all_movies_info"]

    if len(actor_all_movies_info_url_arr) != 0 and actor_all_movies_info_url_arr is not None:
        for actor_all_movies_info_url_arr_object in actor_all_movies_info_url_arr:
            page_movies_arr = actor_all_movies_info_url_arr_object["page_movies_arr"]
            for page_movies_arr_object in page_movies_arr:
                try:
                    if page_movies_arr_object["movies_img_is_request"] == True:
                        log_download("已经下载过此封面截图 " + page_movies_arr_object["movies_img"],
                                     success_status=True)
                        continue
                    download_url = page_movies_arr_object["movies_img"]

                    file_picture_name = download_url.split("/")[-1]

                    save_folder_path = os.path.dirname(save_actor_moviese_file_json)

                    filename = page_movies_arr_object["movies_name"]
                    # 清理后的文件名
                    filename = clean_filename(filename)

                    save_actor_picture_file = os.path.join(save_folder_path, filename + "_" + file_picture_name)
                    status = download_image(download_url, save_actor_picture_file)
                    if status != 200:
                        raise Exception(f"请求失败，状态码: {status}")
                    page_movies_arr_object["movies_img_is_request"] = True
                    log_download("电影封面截图下载成功 " + page_movies_arr_object["movies_img"], success_status=True)

                    actot_movies_info[0]["all_movies_info"] = actor_all_movies_info_url_arr
                    update_json_load_file(save_actor_moviese_file_json, actot_movies_info)
                except Exception as e:
                    log_download("下载失败原因是" + str(e), success_status=False)
                    continue


def download_actor_picture_threadpool(file_actor_info_json, save_file_json, save_actor_picture_folder_root):
    page_info_arr_dict = init_json_load_file(file_actor_info_json)
    actor_picture_arr = init_json_load_file(save_file_json)

    seen_names = set()
    result = []

    add_actor_picture_arr = []
    for page_info_object in page_info_arr_dict:
        add_actor_picture_arr += page_info_object["keyword_actors_dict_arr"]

    for actor_info in add_actor_picture_arr:
        if actor_info["name"] not in seen_names:
            result.append(actor_info)
            seen_names.add(actor_info["name"])

    for obj in add_actor_picture_arr:
        if not any(existing_obj["name"] == obj["name"] for existing_obj in actor_picture_arr):
            actor_picture_arr.append(obj)

    update_json_load_file(save_file_json, actor_picture_arr)

    result_arr = []

    if test_mark_my:
        download_image_actor_picture(test_actor_info_object,save_actor_picture_folder_root)
    else:
        # 执行图片下载
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(download_image_actor_picture, actor_info_object, save_actor_picture_folder_root) for
                       actor_info_object in actor_picture_arr]
            for future in as_completed(futures):
                result_arr.append(future.result())
        update_json_load_file(save_file_json, result_arr)


def download_movies_info_threadpool(keyword_name):
    base_folder = os.path.join(save_root, keyword_name)
    keyWordFileName = "all_movies"
    save_path_movies_arr = []

    for first_dir in os.listdir(base_folder):
        filse_path = os.path.join(base_folder, first_dir)
        if os.path.isdir(filse_path):
            for file_name in os.listdir(filse_path):
                if file_name.endswith('.json') and keyWordFileName in file_name:
                    save_movies_file_path = os.path.join(filse_path, file_name)
                    save_path_movies_arr.append(save_movies_file_path)
    global test_save_file_path
    if test_mark_my:
        download_movies_info(test_save_file_path)
    else:
        # 执行电影信息下载
        with ThreadPoolExecutor(max_workers=40) as executor:
            futures = [executor.submit(download_movies_info, file_path_movies) for file_path_movies in save_path_movies_arr]
            for future in as_completed(futures):
                future.result()


def download_movies_magnet_info_threadpool(keyword_name):
    base_folder = os.path.join(save_root, keyword_name)
    keyWordFileName = "all_movies"
    save_path_movies_arr = []

    for first_dir in os.listdir(base_folder):
        file_path = os.path.join(base_folder, first_dir)
        if os.path.isdir(file_path):
            for file_name in os.listdir(file_path):
                if file_name.endswith('.json') and keyWordFileName in file_name:
                    save_movies_file_path = os.path.join(file_path, file_name)
                    save_path_movies_arr.append(save_movies_file_path)

    global test_save_file_path
    if test_mark_my:
        download_movies_magnet_info(test_save_file_path)
    else:
        # 执行电影磁力链接信息下载
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(download_movies_magnet_info, file_path_movies) for file_path_movies in
                       save_path_movies_arr]
            for future in as_completed(futures):
                future.result()


def download_movies_magnet_info_picture_threadpool(keyword_name):

    base_folder = os.path.join(save_root, keyword_name)
    keyWordFileName = "all_movies"
    save_path_movies_arr = []

    for first_dir in os.listdir(base_folder):
        filse_path = os.path.join(base_folder, first_dir)
        if os.path.isdir(filse_path):
            for file_name in os.listdir(filse_path):
                if file_name.endswith('.json') and keyWordFileName in file_name:
                    save_movies_file_path = os.path.join(filse_path, file_name)
                    save_path_movies_arr.append(save_movies_file_path)
    global test_save_file_path
    if test_mark_my:
        download_movies_magnet_info_picture(test_save_file_path)
    else:
        # 执行电影磁力链接和图片下载
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(download_movies_magnet_info_picture, file_path_movies) for file_path_movies in  save_path_movies_arr]
            for future in as_completed(futures):
                future.result()


def get_web_info(keyword):

    file_path_keyword_page_path = os.path.join(save_root, keyword["keyName"])

    create_folder(file_path_keyword_page_path)

    file_path_keyword_page_json = os.path.join(file_path_keyword_page_path, keyword["keyName"] + ".json")

    save_actor_info_file_json = os.path.join(file_path_keyword_page_path, keyword["keyName"] + "_all_actor_info.json")

    # download_cotter(keyword["keyName"], file_path_keyword_page_json)

    # download_actor_picture_threadpool(file_path_keyword_page_json, save_actor_info_file_json, file_path_keyword_page_path)

    # download_movies_info_threadpool(keyword["keyName"])

    download_movies_magnet_info_threadpool(keyword["keyName"])

    # download_movies_magnet_info_picture_threadpool(keyword["keyName"])


def get_web_info_threadpool(keywords_arr):
    # 主线程池管理所有任务
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_web_info, keyword) for keyword in keywords_arr]
        for future in as_completed(futures):
            future.result()


def auto_login():
    cookie_file = "cookies.json"  # 保存 cookie 的文件
    with sync_playwright() as p:

        browser = p.firefox.launch(headless=False)  # headless=False 可以看到浏览器操作
        page = browser.new_page()

        url = ""
        # 打开登录页面
        page.goto(url, timeout=80000)

        if page.locator("text=Yes, I am.").is_visible():
            with page.expect_navigation():
                page.click("text=Yes, I am.")
            print("已点击 18+ 确认按钮")

        user_name = os.getenv("user_name")
        user_password = os.getenv("user_password")

        # 输入账号密码并登录
        page.fill('//input[@id="email"]', user_name)
        page.fill('//input[@id="password"]', user_password)

        while True:
            # 找到验证码元素
            captcha_element = page.locator('img[alt="Captcha code"]')

            # 截图当前页面的验证码元素
            captcha_element.screenshot(path="captcha.png")

            time.sleep(3)

            reader = easyocr.Reader(['en'])
            result = reader.readtext("captcha.png")
            captcha_text = ''.join([r[1] for r in result])
            # 输入账号密码并登录
            page.fill('//input[@id="email"]', user_name)
            page.fill('//input[@id="password"]', user_password)

            captcha_input = page.locator('input[name="_rucaptcha"]')
            captcha_input.fill(captcha_text)

            # 勾选 "Keep me logged in for 7 days"
            page.check("#remember")  # 通过 id 定位并勾选# 勾选 "Keep me logged in for 7 days"

            submit_button = page.locator('input[type="submit"]')
            submit_button.click()
            try:
                # 等待页面网络空闲，超时也不中断
                page.wait_for_load_state("networkidle", timeout=3000)  # 5秒
            except TimeoutError:
                print("等待页面加载超时，但程序继续执行")
            # 等待导航完成
            # 判断 URL 是否变化

            # 登录后的跳转的目标网址，可以是首页
            current_url = ""
            if page.url == current_url:
                print("页面已经成功登录:", page.url)
                break
            else:
                time.sleep(2)
                captcha_img = page.locator('img.rucaptcha-image')
                captcha_img.click()

        # 获取所有 cookie
        cookies_list = page.context.cookies()
        cookies = {c['name']: c['value'] for c in cookies_list}
        # -------------------------
        # 保存 Cookie 到文件
        # -------------------------
        with open(cookie_file, "w", encoding="utf-8") as f:
            json.dump(cookies, f, ensure_ascii=False, indent=4)
        browser.close()


# 增加用户个性化定制爬取对应网页或者文本
# 暂时不写
def use_log_get_web_object():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # 有界面模式
        page = browser.new_page()
        page.goto("")

        page.expose_function("sendToPython", lambda elem: print("点击元素:", elem))

        page.evaluate("""
            document.addEventListener("click", e => {
                e.preventDefault();      // 阻止默认动作（比如跳转）
                e.stopPropagation();     // 阻止事件冒泡
                window.sendToPython(e.target.outerHTML);
            }, true);
        """)

        page.wait_for_timeout(60000)  # 等待 60 秒让用户点击


if __name__ == "__main__":

    keywords_arr = [{'keyName': '12334'}]
    os.makedirs(save_root, exist_ok=True)

    if test_mark_my:
        # 根据分类下载各个演员详情
        get_web_info_threadpool(test_keywords_arr)
    else:
        # 根据分类下载各个演员详情
        get_web_info_threadpool(keywords_arr)
