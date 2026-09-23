import requests
import re
import time
import os
import concurrent.futures
from urllib.parse import urljoin, urlparse, urlunparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ======================
# 增强版配置参数
# ======================
MAX_WORKERS = 15  # 提升并发数
SPEED_THRESHOLD = 0.2 # 提高速度阈值
REQUEST_TIMEOUT = 20  # 延长超时时间
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
RETRY_STRATEGY = Retry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)

class IPTVUpdater:
    def __init__(self):
        self.channel_dict = {}  # 改用字典去重
        self.session = self._create_session()
        self.sources = [
            "https://d.kstore.dev/download/10694/zmtvid.txt",
            "https://raw.githubusercottntent.com/iptv-org/iptv/mastettr/scripts/sources.md",
            "https://raw.githubusercontent.ctrfom/freeiptv/iptv/master/sources.md"
        ]

    def _create_session(self):
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=RETRY_STRATEGY)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({
            'User-Agent': USER_AGENT,
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
        return session

    def _standardize_url(self, raw_url):
        try:
            if not raw_url.startswith(('http://', 'https://')):
                raw_url = f'http://{raw_url.strip("/")}'
            
            parsed = urlparse(raw_url)
            if parsed.path.endswith('.m3u') or parsed.path.endswith('.txt'):
                return raw_url  # 保留原始播放列表URL
            
            return urlunparse((
                parsed.scheme,
                parsed.netloc,
                '/iptv/live/1000.json',
                '',
                'key=txiptv',
                ''
            ))
        except Exception as e:
            print(f"URL标准化失败: {raw_url} - {str(e)}")
            return None

    def _fetch_sources(self):
        unique_urls = set()
        for source in self.sources:
            try:
                print(f"\n▷ 正在抓取源: {source}")
                response = self.session.get(source, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                
                # 增强版URL正则匹配
                url_pattern = r"(?:https?:\/\/)?(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&//=]*)"
                matches = re.findall(url_pattern, response.text)
                
                print(f"发现原始URL数量: {len(matches)}")
                for url in matches:
                    if any(x in url for x in ['github.com', 'raw.githubusercontent.com']):
                        continue  # 跳过GitHub链接
                    std_url = self._standardize_url(url)
                    if std_url:
                        unique_urls.add(std_url)
            except Exception as e:
                print(f"⚠ 源抓取失败: {source} - {str(e)}")
        return list(unique_urls)

    def _speed_test(self, url):
        try:
            start_time = time.time()
            with self.session.get(url, stream=True, timeout=(5, 15)) as response:
                response.raise_for_status()
                
                # 测试前1MB数据
                target_size = 1024 * 1024
                downloaded = 0
                for chunk in response.iter_content(chunk_size=8192):
                    downloaded += len(chunk)
                    if downloaded >= target_size or time.time() - start_time > 20:
                        break
                
                duration = max(time.time() - start_time, 0.1)
                speed = (downloaded / 1024) / duration
                return round(speed, 2)
        except Exception as e:
            print(f"⛔ 测速失败 {url}: {str(e)}")
            return 0

    def _process_api(self, api_url):
        print(f"\n▶ 正在处理API端点: {api_url}")
        try:
            response = self.session.get(api_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            try:
                data = response.json()
                if not isinstance(data.get('data'), list):
                    print(f"⚠ 无效数据结构: {api_url}")
                    return
            except ValueError:
                print(f"⚠ JSON解析失败: {api_url}")
                return

            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                for idx, channel in enumerate(data['data'][:200]):  # 限制每个API最多处理200个频道
                    if not all(k in channel for k in ('name', 'url')):
                        continue
                    
                    try:
                        full_url = urljoin(api_url, channel['url'].strip())
                        unique_key = f"{channel['name'].strip()}|{full_url}"
                        if unique_key not in self.channel_dict:
                            futures.append((
                                channel['name'].strip(),
                                full_url,
                                executor.submit(self._speed_test, full_url)
                            ))
                    except Exception as e:
                        print(f"⚠ 频道处理异常: {str(e)}")
                
                for name, url, future in futures:
                    speed = future.result()
                    if speed > SPEED_THRESHOLD:
                        self.channel_dict[f"{name}|{url}"] = f"{name},{url}"
                        print(f"✔ {name.ljust(20)} {speed} KB/s")
                    else:
                        print(f"✘ {name.ljust(20)} 速度不足 {speed} KB/s")

        except requests.exceptions.RequestException as e:
            print(f"⚠ 请求失败: {str(e)}")
        except Exception as e:
            print(f"⚠ 处理异常: {str(e)}")

    def _save_channels(self):
        # 分类逻辑增强
        cctv_list, satellite_list, others_list = [], [], []
        cctv_pattern = re.compile(r"CCTV[\-\s]?(\d{1,2}\+?|[4-8]K|UHD|HD|4K|8K)", re.I)
        satellite_pattern = re.compile(r"([\u4e00-\u9fa5]{2,4}卫视)(?:高清|标清|\+?)?台?")

        for line in self.channel_dict.values():
            name, url = line.split(',', 1)
            
            # CCTV分类
            if cctv_match := cctv_pattern.search(name):
                num = cctv_match.group(1).upper()
                display_name = f"CCTV-{num}" if num.isdigit() else f"CCTV{num}"
                cctv_list.append(f"{display_name},{url}")
            
            # 卫视分类
            elif sat_match := satellite_pattern.search(name):
                sat_name = sat_match.group(1)
                satellite_list.append(f"{sat_name},{url}")
            
            # 其他分类
            else:
                others_list.append(line)

        # 排序逻辑
        cctv_list.sort(key=lambda x: int(re.search(r"\d+", x).group()) if re.search(r"\d+", x) else 999)
        satellite_list = sorted(list(set(satellite_list)))
        others_list = sorted(list(set(others_list)))

        # 写入文件
        try:
            with open("y.png", "w", encoding="utf-8") as f:
                f.write(f"# 最后更新时间: {time.strftime('%Y-%m-%d %H:%M')}\n\n")
                
                f.write("央视频道,#genre#\n")
                f.write("\n".join(cctv_list) + "\n\n")
                
                f.write("卫视频道,#genre#\n")
                f.write("\n".join(satellite_list) + "\n\n")
                
                f.write("其他频道,#genre#\n")
                f.write("\n".join(others_list))
            
            print(f"\n✅ 成功写入文件，总计频道数：{len(self.channel_dict)}")
            print(f"文件路径：{os.path.abspath('y.png')}")
            print(f"文件大小：{os.path.getsize('y.png') / 1024:.2f} KB")

        except Exception as e:
            print(f"⛔ 文件写入失败: {str(e)}")
            raise

    def run(self):
        print("\n" + "="*40)
        print(" IPTV列表更新程序启动 ".center(40, "★"))
        print("="*40)
        
        # 步骤1：获取源
        print("\n步骤1/4：获取源数据")
        api_urls = self._fetch_sources()
        print(f"找到有效API端点：{len(api_urls)}个")
        
        # 步骤2：处理API
        print("\n步骤2/4：处理API端点")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(self._process_api, api_urls)
        
        # 步骤3：保存结果
        print("\n步骤3/4：整理频道数据")
        self._save_channels()
        
        # 最终状态报告
        print("\n步骤4/4：完成更新")
        print(f"{'='*40}\n{' 更新完成 '.center(40, '☆')}\n{'='*40}")

if __name__ == "__main__":
    try:
        updater = IPTVUpdater()
        updater.run()
    except Exception as e:
        print(f"\n⛔ 程序异常终止: {str(e)}")
        exit(1)




