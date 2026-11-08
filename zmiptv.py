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
                        self.channel_dict[f"{name}|{url}"] = (name, url, speed)
                        print(f"✔ {name.ljust(20)} {speed} KB/s")
                    else:
                        print(f"✘ {name.ljust(20)} 速度不足 {speed} KB/s")

        except requests.exceptions.RequestException as e:
            print(f"⚠ 请求失败: {str(e)}")
        except Exception as e:
            print(f"⚠ 处理异常: {str(e)}")

    def _save_channels(self):
        # 同名归一去重: 每台只留测速最快的 2 条线路
        by_name = {}
        for name, url, speed in self.channel_dict.values():
            cn = clean_name(name)
            if not cn:
                continue
            by_name.setdefault(cn, []).append((speed, url))
        grouped = {g: [] for g in GROUPS}
        total = 0
        for cn, lines in by_name.items():
            total += min(len(lines), 2)
        for cn, lines in by_name.items():
            lines.sort(key=lambda x: -x[0])
            for _sp, url in lines[:2]:
                grouped[classify(cn)].append(f"{cn},{url}")
        grouped['央视'].sort(key=lambda l: _cctv_key(l.split(',', 1)[0]))
        grouped['卫视'].sort(key=lambda l: _sat_key(l.split(',', 1)[0]))
        for g in GROUPS[2:]:
            grouped[g] = sorted(set(grouped[g]))

        try:
            with open("y.png", "w", encoding="utf-8") as f:
                f.write(f"# 最后更新时间: {time.strftime('%Y-%m-%d %H:%M')}\n\n")
                for g in GROUPS:
                    if not grouped[g]:
                        continue
                    f.write(f"{g},#genre#\n")
                    f.write("\n".join(grouped[g]) + "\n\n")
            print(f"\n✅ 成功写入文件，总计频道数：{total}")
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


# ======================
# 命名归一 + 分组
# ======================
_FW = str.maketrans('０１２３４５６７８９＋', '0123456789+')
_CN_CCTV = {'综合': 'CCTV-1', '财经': 'CCTV-2', '综艺': 'CCTV-3', '中文国际': 'CCTV-4',
            '体育赛事': 'CCTV-5+', '体育': 'CCTV-5', '电影': 'CCTV-6', '国防军事': 'CCTV-7',
            '电视剧': 'CCTV-8', '纪录': 'CCTV-9', '科教': 'CCTV-10', '戏曲': 'CCTV-11',
            '社会与法': 'CCTV-12', '新闻': 'CCTV-13', '少儿': 'CCTV-14', '音乐': 'CCTV-15'}
_SUFFIX_JUNK = re.compile(r'(高清|标清|超清|蓝光|杜比|4K|8K|UHD|HD|SD|频道|电视台|台)$')

def clean_name(raw):
    n = (raw or '').translate(_FW).strip()
    n = re.sub(r'[\[【\(（].*?[\]】\)）]', '', n)          # 去括号注
    n = re.sub(r'^\d+[\.、\-\s]*', '', n)                     # 去序号前缀
    if re.search(r'[\u4e00-\u9fa5]', n):
        n = re.sub(r'\s+', '', n)
    else:
        n = re.sub(r'\s+', ' ', n)
    if re.search(r'CCTV[\-\s]?(4K|8K)', n, re.I):
        return 'CCTV-' + re.search(r'CCTV[\-\s]?(4K|8K)', n, re.I).group(1).upper()
    if n.startswith('中央'):
        for k in sorted(_CN_CCTV, key=len, reverse=True):
            if k in n:
                return _CN_CCTV[k]
    m = re.search(r'(?:CCTV|中央(?:电视台)?)[\-\s]?(\d{1,2})(\+)?', n, re.I)
    if m:
        return 'CCTV-%s%s' % (m.group(1), m.group(2) or '')
    if 'CGTN' in n.upper():
        return 'CGTN'
    m = re.search(r'([\u4e00-\u9fa5]{2,4}卫视)', n)
    if m:
        n = m.group(1)
    else:
        for _ in range(3):                                        # 画质/通用后缀连剥
            n2 = _SUFFIX_JUNK.sub('', n)
            if n2 == n:
                break
            n = n2
    return n

_HMT = ('TVB', '翡翠', '明珠', '无线', '港台', '凤凰', '有线', 'HOY', 'Viu', 'Now',
        '民视', '中视', '华视', '台视', '公视', '东森', '三立', 'TVBS', '纬来', '中天',
        '年代', '非凡', '八大', 'ELTA', 'MOMO', '靖天', '澳视', '澳门')
_SPORT = ('体育', '足球', '高尔夫', '网球', '赛车', '户外', '钓鱼', '武术', '搏击', 'ESPN', '高球')
_KIDS = ('少儿', '动漫', '卡通', '动画', '宝贝', '金鹰卡通', '哈哈炫动', '嘉佳卡通')
_MOVIE = ('电影', '影院', '剧场', '电视剧', '影视', '动作', '喜剧', 'CHC', '爱奇艺', '优酷', '腾讯')
_NEWS = ('新闻', '资讯', '财经', '第一财经')
_PLACE = ('都市', '综合', '经济', '生活', '公共', '教育', '法制', '文旅', '农业', '农村',
          '北京', '上海', '天津', '重庆', '河北', '山西', '内蒙古', '辽宁', '吉林', '黑龙江',
          '江苏', '浙江', '安徽', '福建', '江西', '山东', '河南', '湖北', '湖南', '广东',
          '广西', '海南', '四川', '贵州', '云南', '西藏', '陕西', '甘肃', '青海', '宁夏',
          '新疆', '深圳', '广州', '杭州', '南京', '成都', '武汉', '西安', '青岛', '大连',
          '宁波', '厦门', '苏州', '长沙', '郑州', '沈阳', '哈尔滨', '长春', '石家庄', '太原',
          '呼和浩特', '合肥', '福州', '南昌', '济南', '昆明', '南宁', '海口', '贵阳', '拉萨',
          '兰州', '西宁', '银川', '乌鲁木齐', '无锡', '佛山', '东莞', '温州', '泉州', '徐州')
GROUPS = ['央视', '卫视', '港澳台', '体育', '少儿', '影视', '新闻', '地方', '国际', '其他']
_SAT_ORDER = ['湖南卫视', '浙江卫视', '江苏卫视', '东方卫视', '广东卫视', '北京卫视',
              '深圳卫视', '山东卫视', '安徽卫视', '四川卫视', '湖北卫视', '河南卫视',
              '天津卫视', '重庆卫视', '东南卫视', '江西卫视', '辽宁卫视', '黑龙江卫视',
              '河北卫视', '山西卫视', '陕西卫视', '吉林卫视', '广西卫视', '云南卫视',
              '贵州卫视', '内蒙古卫视', '宁夏卫视', '新疆卫视', '西藏卫视', '青海卫视',
              '甘肃卫视', '海南卫视', '兵团卫视']

def classify(name):
    u = name.upper()
    if name.startswith('CCTV') or name == 'CGTN':
        return '央视'
    if any(k in name or k in u for k in _HMT):
        return '港澳台'
    if '卫视' in name:
        return '卫视'
    if any(k in name or k in u for k in _SPORT):
        return '体育'
    if any(k in name for k in _KIDS):
        return '少儿'
    if any(k in name or k in u for k in _MOVIE):
        return '影视'
    if any(k in name for k in _NEWS):
        return '新闻'
    if any(k in name for k in _PLACE):
        return '地方'
    if re.search(r'[A-Za-z]', name) and not re.search(r'[\u4e00-\u9fa5]', name):
        return '国际'
    return '其他'

def _cctv_key(name):
    m = re.search(r'CCTV-(\d+)', name)
    if m:
        return (0, int(m.group(1)), 1 if '+' in name else 0)
    if '4K' in name or '8K' in name:
        return (1, 0, 0)
    return (2, 0, 0)

def _sat_key(name):
    return (_SAT_ORDER.index(name) if name in _SAT_ORDER else 99, name)


if __name__ == "__main__":
    try:
        updater = IPTVUpdater()
        updater.run()
    except Exception as e:
        print(f"\n⛔ 程序异常终止: {str(e)}")
        exit(1)




