import subprocess
import sys
files = ['同花顺.py','新浪财经.py','财联社.py','分析并识别的荐股.py']
try:
    for k in files[:3]:
        subprocess.run([sys.executable,k],check=True)
    subprocess.run([sys.executable,files[3]],check=True)
except:
    print('出现错误')