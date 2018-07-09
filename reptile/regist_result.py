#coding=utf-8
import re
import requests
import numpy
'''
住房意向登记结果获取
'''


def getHtml(url, user_agent, num_retries):
    headers = {'user-agent': user_agent}
    try:
        r = requests.get(url,headers=headers)
        html = None
        if r.status_code == 200:
            html = r.content
        else:
            print 'status_code : {0}'.format(r.status_code)
    except BaseException as ex:
        print 'getHtml error :', ex
        html = None
        if num_retries > 0:
            print 'getHtml fail and retry...'
            return getHtml(url,user_agent,num_retries-1)
        pass
    return html

if __name__ == "__main__":
    user_agent = r'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36'
    num_retries = 3
    # 页码数组
    num_pages = numpy.arange(1,63,1)
    html_result = ''
    gx = 0
    hygx = 0
    for page_index in num_pages:
        if page_index == 1:
            url = r'http://124.115.228.93/zfrgdjpt/jggs.aspx?qy=07&yxbh=0000000090'
        else:
            url = 'http://124.115.228.93/zfrgdjpt/jggs.aspx?qy=07&yxbh=0000000090&page=' + bytes(page_index)
        html_temp = getHtml(url, user_agent, num_retries)
        html_result = html_result + html_temp
    all_tr = re.findall(r'<tr>(.*?)</tr>',html_result,re.S|re.M)
    print '总记录数：',len(all_tr)
    for tr in all_tr:
        all_td = re.findall(r'<td>(.*?)</td>', tr, re.S|re.M)
        # 家庭类型
        if unicode(all_td[4],'utf-8') == unicode('刚需家庭','utf-8'):
            gx = gx + 1
            if unicode(all_td[6],'utf-8') == unicode('核验通过','utf-8'):
                hygx = hygx + 1
    print '刚需家庭：' + bytes(gx) + '个；'
    print '核验通过刚需家庭：' + bytes(hygx) + '个'


